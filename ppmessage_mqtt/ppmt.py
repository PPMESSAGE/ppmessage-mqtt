# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical, PPMESSAGE
# Ding Guijin, guijin.ding@yvertical.com
# Ning Ben, ben.ning@yvertical.com
#
# All rights are reserved.
#

import sys
import time
import struct
import logging
import functools
import threading
from Queue import Queue
from tornado.web import Application
from tornado.ioloop  import PollIOLoop
from tornado.tcpserver import TCPServer

from .ppmtdb import worker
from .ppmtdb import sender
from .ppmtdb import ppmtdb
from .ppmtdb import mqtt3_message
from .ppmtdb import yvmq_msg_state

#from .ppauth import authenticate
from .yourauth import your_authenticate as authenticate

if sys.version_info[0] < 3:
    MQTT3_PROTOCOL_NAME = "MQIsdp"
else:
    MQTT3_PROTOCOL_NAME = b"MQIsdp"

PROTOCOL_VERSION = 3

# db cmd code for yvdbase maintain
DBCMD_INVALID   = 0
DBCMD_DEL_I_MSG = 1
DBCMD_DEL_O_MSG = 2
DBCMD_ADD_I_MSG = 3
DBCMD_UPD_O_MSG = 4
DBCMD_FINISH    = 5

# Message types 
MQTT3_CNNECT = 0x10
MQTT3_CONNACK = 0x20
MQTT3_PUBLISH = 0x30
MQTT3_PUBACK = 0x40
MQTT3_PUBREC = 0x50
MQTT3_PUBREL = 0x60
MQTT3_PUBCOMP = 0x70
MQTT3_SUBSCRIBE = 0x80
MQTT3_SUBACK = 0x90
MQTT3_UNSUBSCRIBE = 0xA0
MQTT3_UNSUBACK = 0xB0
MQTT3_PINGREQ = 0xC0
MQTT3_PINGRESP = 0xD0
MQTT3_DISCONNCT = 0xE0

MQTT3_ANY = 0xF0
MQTT3_NEW = 0x00

# Log levels
MQ_LOG_INFO = 0x01
MQ_LOG_NOTICE = 0x02
MQ_LOG_WARNING = 0x04
MQ_LOG_ERR = 0x08
MQ_LOG_DEBUG = 0x10

# CONNACK codes
CONNACK_ACCEPTED = 0
CONNACK_REFUSED_PROTOCOL_VERSION = 1
CONNACK_REFUSED_IDENTIFIER_REJECTED = 2
CONNACK_REFUSED_SERVER_UNAVAILABLE = 3
CONNACK_REFUSED_BAD_USERNAME_PASSWORD = 4
CONNACK_REFUSED_NOT_AUTHORIZED = 5

# Connection state
mq_cs_new = 0
mq_cs_connected = 1
mq_cs_disconnecting = 2
mq_cs_connect_async = 3
mq_cs_reconnected = 4


# Error values
MQ_ERR_AGAIN = -1
MQ_ERR_SUCCESS = 0
MQ_ERR_NOMEM = 1
MQ_ERR_PROTOCOL = 2
MQ_ERR_INVAL = 3
MQ_ERR_NO_CONN = 4
MQ_ERR_CONN_REFUSED = 5
MQ_ERR_NOT_FOUND = 6
MQ_ERR_CONN_LOST = 7
MQ_ERR_TLS = 8
MQ_ERR_PAYLOAD_SIZE = 9
MQ_ERR_NOT_SUPPORTED = 10
MQ_ERR_AUTH = 11
MQ_ERR_ACL_DENIED = 12
MQ_ERR_UNKNOWN = 13
MQ_ERR_ERRNO = 14

gMQbase = ppmtdb()
gAuth = authenticate()

def read_callback(function):
    @functools.wraps(function)
    def wrapper(self,*args, **keyargs):
        received_bytes = function(self,*args, **keyargs)
        self._ppmtdb.work_queue.put( (self._ppmtdb.add_received_bytes, (received_bytes,)) ) # put add pub task into queue
    return wrapper

class mqtt3context(object):

    def __init__(self, stream, ppmtdb=gMQbase, auth=gAuth):
        self._ppmtdb = ppmtdb 
        self._stream = stream  
        self._auth = auth
        self._sock = None
        self._message_retry = 20
        self._last_retry_check = 0
        self._client_id = None
        self._will_topic = None
        self._will_message = None
        self._user_name = None
        self._password = None
        self._connect_state = mq_cs_new

#       self._total_received_bytes = 0
#       self._total_sent_bytes = 0

        self._pub_acl = None
        self._sub_acl = None

#       self._semaphore = threading.BoundedSemaphore(1)
#       self.res_queue = Queue() # result queue for worker thread
        self._wait_pub_queue = Queue() # wait to pubish with qos 1 or qos 2 message
        self._curr_pub_msg = None # current publish message what waiting to be completed

        ''' for incoming packet from client '''
        self._msgtype = 0
        self._dup = False
        self._out_mid = 0
        self._last_mid = 0
        self._qos = 0
        self._retain = False
        self._username = ""
        self._password = ""
        self._buff = B""
        self._fixed_header_byte1 = 0
        self._len_bytes_count = 0
        self._error_code = 0

        self._keep_alive = 60
        self._timeout_handle = PollIOLoop.instance().call_later(1.5 * self._keep_alive, self.keep_alive_timeout)
        self._stream.set_close_callback(self.close_callback)

        # for remaining len
        self._multiplier = 1
        self._remaining_len = 0
        self._cnn_flag = 0

        
        self.handler = { 
            MQTT3_CNNECT: lambda:self.mqtt3_connect_handler(),
            #                    MQTT3_CONNACK:   lambda: self.mqtt3_connack_handler(),
            MQTT3_PUBLISH:   lambda: self.mqtt3_publish_handler(),
            MQTT3_PUBACK:    lambda: self.mqtt3_puback_handler(),
            MQTT3_PUBREC:    lambda: self.mqtt3_pubrec_handler(),
            MQTT3_PUBREL:    lambda: self.mqtt3_pubrel_handler(),
            MQTT3_PUBCOMP:   lambda: self.mqtt3_pubcomp_handler(),
            MQTT3_SUBSCRIBE: lambda: self.mqtt3_subscribe_handler(),
            #                    MQTT3_SUBACK:    lambda: self.suback_handler(),
            MQTT3_UNSUBSCRIBE: lambda: self.mqtt3_unsubscribe_handler(),
            #                    MQTT3_UNSUBACK:  lambda: self.unsuback_handler(),
            MQTT3_PINGREQ:   lambda: self.mqtt3_pingreq_handler(),
            #                    MQTT3_PINGRESP:  lambda: self.pingresp_handler(),
            MQTT3_DISCONNCT: lambda: self.mqtt3_disconnect_handler()
            }

        self.mqtt3_recv_packet()  


    def is_mqtt3_message(self, msg_type):

        ret = False;

        if msg_type == MQTT3_CNNECT:
            if self._connect_state == mq_cs_new:
                ret = True
        elif self._connect_state == mq_cs_connected:
               ret = True

        if not ret:
            self.close()
            
        return ret

    @read_callback
    def mqtt3_recv_msgtype_callback(self, data):

        byte1, = struct.unpack('B', data)
        msg_type = byte1 & 0xF0

        self._fixed_header_byte1 = byte1

        if self.is_mqtt3_message(msg_type):
            self._msgtype = msg_type
            self._dup = (byte1 & 0x08) > 0
            self._qos = (byte1 & 0x06) >> 1
            self._retain = (byte1 & 0x01) > 1
            self.mqtt3_recv_remaining_len()
        else:
            self._error_code = 1

        return len(data)

    @read_callback
    def mqtt3_recv_buff_callback(self, data):

        self._buff =  data
        self.mqtt3_msg_handler(self._msgtype)

        if not self._stream.closed():
            self.mqtt3_recv_packet()
        
        return len(data)

    def mqtt3_recv_buff(self):
        self._stream.read_bytes(self._remaining_len, self.mqtt3_recv_buff_callback)


    # get remaining len (1 ~ 4 bytes)
    @read_callback
    def mqtt3_recv_remaining_len_callback(self, data):
        
        tmpb, = struct.unpack('B', data)
        self._len_bytes_count += 1
        self._remaining_len += (tmpb & 0x7F) * self._multiplier
        self._multiplier *= 128

        if (tmpb & 0x80):
            if self._len_bytes_count<5: # must be less or eq. 4 bytes
                self.mqtt3_recv_remaining_len()
            else:
                self._error_code = 2
                self.mqtt3_recv_packet()
        else:
            self.mqtt3_recv_buff()

        return len(data)


    def mqtt3_recv_remaining_len(self):
        self._stream.read_bytes(1, self.mqtt3_recv_remaining_len_callback)
        

    def mqtt3_recv_packet(self):
        self._remaining_len = 0
        self._multiplier = 1
        self._len_bytes_count = 0
        self._stream.read_bytes(1, self.mqtt3_recv_msgtype_callback)

    def mqtt3_msg_handler(self, msg_type):
        fun = self.handler.get(msg_type)
        if not fun:
            logging.error("no mqtt handler for msg_type: %d" % msg_type)
            return
        
        self._ppmtdb.work_queue.put( (self._ppmtdb.add_received_message, (1,)) )
        fun()
        self.reset_timeout()
        return
    
    def mqtt3_puback_handler(self):
        if self._remaining_len != len(self._buff) and self._remaining_len != 2:
            self.close()
            return 1
        mid,  = struct.unpack("!H", self._buff)
        if self._curr_pub_msg and self._curr_pub_msg.mid == mid and self._curr_pub_msg.state == yvmq_msg_state.wait_puback:
                self._ppmtdb.work_queue.put( (self._ppmtdb.unpub, (self._curr_pub_msg.topic, self._curr_pub_msg.qos)) ) # unpub when be send ok
                topic = self._curr_pub_msg.topic
                qos = self._curr_pub_msg.qos
                device_id = self._client_id
                logging.info(" ack unpub --> topic: %s  qos: %d  device_id: %s " % ( topic, qos, device_id))
                self._curr_pub_msg = None
                self._ppmtdb.work_queue.put( (self._ppmtdb.dec_in_flight_o, ()) )
                self.wait_pub_queue_handler_callback()
        return

    def mqtt3_pubrec_handler(self):
        if self._remaining_len != len(self._buff) and self._remaining_len != 2:
            self.close()
            return 1

        mid,  = struct.unpack("!H", self._buff)
        logging.info("mqtt3_pubrec_handler..................mid: %d" % mid)

#       if self._curr_pub_msg:
#           if self._curr_pub_msg.mid == mid and self._curr_pub_msg.state == yvmq_msg_state.wait_pubrec:
#               PollIOLoop.instance().remove_timeout(self._curr_pub_msg.timeout)
#               self._curr_pub_msg = None
#               self._ppmtdb.work_queue.put( (self._ppmtdb.dec_in_flight_o, ()) )

        self.mqtt3_send_pubrel(mid)


    def mqtt3_pubrel_handler(self):

        logging.info("mqtt3_pubrel_handler..................")
        
        if self._remaining_len != len(self._buff) and self._remaining_len != 2:
            self.close()
            return 1

        mid,  = struct.unpack("!H", self._buff)
        self.mqtt3_send_pubcomp(mid)

        if self._dup and self._qos>0 and self._last_mid==mid:
            return

        self._last_mid = mid

#       self._ppmtdb.work_queue.put( (self, WQ_DEL_I_MSG, mid) )
        self._ppmtdb.work_queue.put( (self._ppmtdb.dec_in_flight_i, ()) )


    def mqtt3_pubcomp_handler(self):
        if self._remaining_len != len(self._buff) and self._remaining_len != 2:
            self.close()
            return 1

        mid,  = struct.unpack("!H", self._buff)
        if self._curr_pub_msg and self._curr_pub_msg.mid == mid and self._curr_pub_msg.state == yvmq_msg_state.wait_pubcomp:
                self._ppmtdb.work_queue.put( (self._ppmtdb.unpub, (self._curr_pub_msg.topic, self._curr_pub_msg.qos)) ) # unpub when be send ok
                self._curr_pub_msg = None
                self._ppmtdb.work_queue.put( (self._ppmtdb.dec_in_flight_o, ()) )
                self.wait_pub_queue_handler_callback()
        return

    def mqtt3_connect_handler(self):
        if (self._remaining_len != len(self._buff)):
            return 1

        remain_len = self._remaining_len - 12
        fmt = "!H6sBBH" + str(remain_len) + "s"
        proto_len, proto_name, ver, cnn_flag, keep_alive, payload = struct.unpack(fmt, self._buff)

        self._cnn_flag = cnn_flag

        if proto_name != "MQIsdp" or proto_len != 6 or ver != 3:
            self._error_code = MQ_ERR_PROTOCOL
#           self.close()
            self.mqtt3_send_connack( CONNACK_REFUSED_PROTOCOL_VERSION )
            return MQ_ERR_PROTOCOL
        
        remain_len = len(payload) - 2
        fmt = "!H" + str(remain_len) + "s"
        id_len, payload = struct.unpack(fmt, payload)

        remain_len = len(payload) - id_len

        if remain_len:
            fmt = "!" + str(id_len) + "s" + str(remain_len) + "s"
            client_id, payload =  struct.unpack(fmt, payload)
        else:
            fmt = "!" + str(id_len) + "s"
            client_id, =  struct.unpack(fmt, payload)

        self._client_id = client_id

        # for will flag
        remain_len = len(payload) - 2
        if cnn_flag & 0x04 and remain_len>0:

            # get will topic
            fmt = "!H" + str(remain_len) + "s"
            will_topic_len, payload = struct.unpack(fmt, payload)

            remain_len = len(payload) - will_topic_len

            if remain_len:
                fmt = "!" + str(will_topic_len) + "s" + str(remain_len) + "s"
                will_topic, payload =  struct.unpack(fmt, payload)
                remain_len = len(payload)
            else:
                fmt = "!" + str(will_topic_len) + "s"
                will_topic, =  struct.unpack(fmt, payload)

            self._will_topic = will_topic

            # get will message
            remain_len -= 2
            
            if remain_len:
                fmt = "!H" + str(remain_len) + "s"
                will_message_len, payload = struct.unpack(fmt, payload)
                remain_len = len(payload) - will_message_len

                if remain_len:
                    fmt = "!" + str(will_message_len) + "s" + str(remain_len) + "s"
                    will_message, payload =  struct.unpack(fmt, payload)
                    remain_len = len(payload)
                else:
                    fmt = "!" + str(will_message_len) + "s"
                    will_message, =  struct.unpack(fmt, payload)

                self._will_message = will_message


        # get user name
        remain_len = len(payload) - 2
        if cnn_flag & 0x80 and remain_len>0:
            # get will topic
            fmt = "!H" + str(remain_len) + "s"
            user_name_len, payload = struct.unpack(fmt, payload)

            remain_len = len(payload) - user_name_len

            user_name = None

            if remain_len:
                fmt = "!" + str(user_name_len) + "s" + str(remain_len) + "s"
                user_name, payload =  struct.unpack(fmt, payload)
                remain_len = len(payload)
            else:
                fmt = "!" + str(user_name_len) + "s"
                user_name, =  struct.unpack(fmt, payload)

            self._user_name = user_name

        # get user password
        remain_len -= 2
        if cnn_flag & 0x80 and cnn_flag &0x60 and remain_len>0:
            # get will topic
            fmt = "!H" + str(remain_len) + "s"
            password_len, payload = struct.unpack(fmt, payload)

            remain_len = len(payload) - password_len

            password = None

            if remain_len:
                fmt = "!" + str(password_len) + "s" + str(remain_len) + "s"
                pasword, payload =  struct.unpack(fmt, payload)
            else:
                fmt = "!" + str(password_len) + "s"
                password, =  struct.unpack(fmt, payload)

            self._password = password

        ack_code =  self._auth.auth(self._client_id, self._user_name, self._password)
        self.mqtt3_send_connack(ack_code)
        if keep_alive:
            self._keep_alive = keep_alive
        logging.info(" connect --> device_id: %s " % (  self._client_id))
        return

    def mqtt3_send_connack(self, connack_code=CONNACK_ACCEPTED):

        packet = bytearray()
        packet.extend(struct.pack("!BBBB", MQTT3_CONNACK, 2, 0, connack_code))

        self.write(packet)
        if connack_code == CONNACK_ACCEPTED:
            self._connect_state = mq_cs_connected
            if self._cnn_flag & 0x02: # clean flag is set (1)
#                logging.info ("%s  clean flag set (1) " % self._client_id)
                clean = True
            else: # clean flag is not set (0)
#                logging.info ("%s  clean flag set (0) " % self._client_id)
                clean = False

#           if self._client_id in self._ppmtdb.cnns:
#                self._ppmtdb.cnns[self._client_id].context._stream.close()

            self._ppmtdb.work_queue.put((self._ppmtdb.add_cnn , (self, clean)) ) # put a add_cnn task into queue

            self._pub_acl = self._auth.pub_acl_list(self._client_id)
            self._sub_acl = self._auth.sub_acl_list(self._client_id)

        else:
            self.close()

        return
    
    def mqtt3_publish_handler(self):

        qos = ( self._fixed_header_byte1 & 0x06 ) >> 1
        retain = self._fixed_header_byte1 & 0x01
        dup = self._fixed_header_byte1 & 0x08
        
        if qos > 2 or self._remaining_len != len(self._buff):
            self.close()
        
        # get topic len & topic
        remain_len = self._remaining_len - 2
        fmt = "!H" + str(remain_len) + "s"
        topic_len, payload = struct.unpack(fmt, self._buff)

        remain_len = len(payload) - topic_len

        if qos:
            remain_len -= 2
            message = b""
            if remain_len:
                fmt = "!" + str(topic_len) + "sH" + str(remain_len) + "s"
                topic, mid, message =  struct.unpack(fmt, payload)
            else:
                fmt = "!" + str(topic_len) + "sH"
                topic, mid =  struct.unpack(fmt, payload)
        else:
            fmt = "!" + str(topic_len) + "s" + str(remain_len) + "s"
            topic, message =  struct.unpack(fmt, payload)
            mid = 0

        if not self.verify_pubtopic_ok(topic):
            self.close()
            return

        if qos == 1:
            self.mqtt3_send_puback(mid)
        elif qos == 2:
            self.mqtt3_send_pubrec(mid)

        if dup and qos==2 and self._last_mid==mid: # if qos == 1 then republish to subscribers (At least once delivery)
            return

        self._last_mid = mid

        if qos==2:
            self._ppmtdb.work_queue.put( (self._ppmtdb.inc_in_flight_i, ()) )

        if message:

            #print  " received pub --> topic: %s  device_id: %s " % (  topic, self._client_id)
            logging.info(" received pub --> topic: %s  device_id: %s " % (  topic, self._client_id))

            if retain: 
                self._ppmtdb.work_queue.put( (self._ppmtdb.add_pub, (topic, qos, message)) ) # put add pub task into queue
            self._ppmtdb.work_queue.put( (self._ppmtdb.pub4pub_process, (topic, qos, message)) ) # put a pub task into queue

        elif retain:
            self._ppmtdb.work_queue.put( (self._ppmtdb.unpub, (topic, qos)) ) # put unpub task into queue


    def mqtt3_send_puback(self, mid):
        packet = bytearray()
        packet.extend( struct.pack("!BBH", MQTT3_PUBACK, 2, mid) )
        self.write(packet)
#       self._ppmtdb.work_queue.put( (self._ppmtdb.dec_in_flight_i, ()) )

    def mqtt3_send_pubrec(self, mid):
        packet = bytearray()
        packet.extend( struct.pack("!BBH", MQTT3_PUBREC | 0x02, 2, mid) )
        self.write(packet)
#       self._ppmtdb.work_queue.put( (self, WQ_ADD_I_MSG, (mid, yvmq_msg_state.wait_pubrel, packet)) )
#       self._ppmtdb.work_queue.put( (self._ppmtdb.add_i_message, (self._client_id, mid, yvmq_msg_state.wait_pubrel, packet)) )

    def mqtt3_send_pubrel(self, mid, update_state=True):
#       logging.info ("mqtt3_send_pubrel ---> mid:%d" % mid)
        packet = bytearray()
        packet.extend( struct.pack("!BBH", MQTT3_PUBREL | 0x02, 2, mid) )
        self._curr_pub_msg.state = yvmq_msg_state.wait_pubcomp
        self._curr_pub_msg.packet = packet
        self.write(packet)
        return

    def mqtt3_send_pubcomp(self, mid):
        packet = bytearray()
        packet.extend( struct.pack("!BBH", MQTT3_PUBCOMP, 2, mid) )
        self.write(packet)

    def mqtt3_disconnect_handler(self):
        self._connect_state = mq_cs_disconnecting
        self.close()
        return
    
    def mqtt3_pingreq_handler(self):
        logging.info(" pingreq --> device_id: %s " % (  self._client_id))
        self.mqtt3_send_pingresp()
        return
    
    def mqtt3_send_pingresp(self):
        logging.info(" pingresp --> device_id: %s " % (  self._client_id))
        packet = bytearray()
        packet.extend( struct.pack("!BB", MQTT3_PINGRESP, 0) )
        self.write(packet)
        return

    def mqtt3_subscribe_handler(self):
        qos = ( self._fixed_header_byte1 & 0x06 ) >> 1 
        dup = self._fixed_header_byte1 & 0x08
        if qos != 1 or self._remaining_len != len(self._buff):
            self.close()
            return

        # get mid
        remain_len = self._remaining_len - 2
        fmt = "!H" + str(remain_len) + "s"

        mid, payload = struct.unpack(fmt, self._buff)

        sub_list = []
        remain_len = len(payload) - 2

        while remain_len:
            fmt = "!H" + str(remain_len) + "s"
            sub_topic_len, payload = struct.unpack(fmt, payload)
            remain_len = len(payload) - sub_topic_len - 1
            if remain_len:
                fmt = "!" + str(sub_topic_len) + "sB" + str(remain_len) + "s"
                sub_topic, sub_qos, payload = struct.unpack(fmt, payload)
                remain_len = len(payload) - 2
            else:
                fmt = "!" + str(sub_topic_len) + "sB"
                sub_topic, sub_qos = struct.unpack(fmt, payload)
            
            sub_list.append((sub_topic, sub_qos))

            #print  " received sub --> topic: %s  device_id: %s " % (  sub_topic, self._client_id)
            logging.info(" received sub --> topic: %s  device_id: %s " % (  sub_topic, self._client_id))
            
            if sub_qos >= 0x03 or not self.verify_subtopic_ok(sub_topic): 
                self.close()
                return
              
        self.mqtt3_send_suback(mid, sub_list)
        if dup and qos>0 and self._last_mid==mid:
            return

        self._last_mid = mid
        self._ppmtdb.work_queue.put( (self._ppmtdb.add_sublist, (self._client_id, sub_list)) ) # put a add sub task into queue
        self._ppmtdb.work_queue.put( (self._ppmtdb.pub4sub_list, (self, sub_list)) ) # put a pub4sublist task into queue
        return

    def verify_subtopic_ok(self, topic):
        if topic.count('#')>1: 
            return False
        if '#' in topic and topic[-1] != '#':
            return False
        token_list = filter(None, topic.split('/'))
        for token in token_list:
            if token.count('+') > 1:
                return False
        #return True
        return self._acl_check(topic, self._sub_acl)

    def verify_pubtopic_ok(self, topic):
        if '#' in topic or '+' in topic: 
            return False
        if len(topic)>0:
            return self._acl_check(topic, self._pub_acl)
        return True

    def mqtt3_send_suback(self, mid, sub_list):
        n  = len(sub_list) # n bytes for n qos
        remain_len = n + 2 # added 2 bytes for mid in variable header
        packet = bytearray()
        packet.extend(struct.pack("!B", MQTT3_SUBACK))
        self._pack_remain_len(packet, remain_len)
        packet.extend(struct.pack("!H", mid))
        for (sub_topic, qos) in sub_list:
            packet.extend(struct.pack("!B", qos))                          
        self.write(packet)
        return

    def _pack_remain_len(self, packet, len):
        if len > 268435455:
            return
        while True:
            byte = len % 128
            len =  len // 128
            # If there are more digits to encode, set the top bit of this digit
            if len > 0:
                byte = byte | 0x80
            packet.extend(struct.pack("!B", byte))
            if len == 0:
                return
        return

    def mqtt3_unsubscribe_handler(self):
        logging.info("mqtt3_unsubscribe_handler..................")

        qos = ( self._fixed_header_byte1 & 0x06 ) >> 1 
        dup = self._fixed_header_byte1 & 0x08

        if qos != 1 or self._remaining_len != len(self._buff):
            self.close()
            return

        # get mid
        remain_len = self._remaining_len - 2
        fmt = "!H" + str(remain_len) + "s"

        mid, payload = struct.unpack(fmt, self._buff)

        unsub_list = []
        remain_len = len(payload) - 2

        while remain_len:
            fmt = "!H" + str(remain_len) + "s"
            unsub_topic_len, payload = struct.unpack(fmt, payload)

            remain_len = len(payload) - unsub_topic_len
            if remain_len:
                fmt = "!" + str(unsub_topic_len) + "s" + str(remain_len) + "s"
                unsub_topic, payload = struct.unpack(fmt, payload)
                remain_len = len(payload) - 2
            else:
                fmt = "!" + str(unsub_topic_len) + "s"
                unsub_topic, = struct.unpack(fmt, payload)
            
            unsub_list.append(unsub_topic)

        self.mqtt3_send_unsuback(mid)

        if dup and qos>0 and self._last_mid==mid:
            return

        self._last_mid = mid
        self.unsubs_from_ppmtdb(self._client_id, unsub_list)
        return
    
    def unsubs_from_ppmtdb(self, client_id, unsub_list):
        self._ppmtdb.work_queue.put( (self._ppmtdb.unsublist, (self._client_id, unsub_list)) ) # put a add sub task into queue
        return

    def mqtt3_send_unsuback(self, mid):
        packet = bytearray()
        packet.extend( struct.pack("!BBH", MQTT3_UNSUBACK, 2, mid) )
        self.write(packet)
        return
    
#    def read(self, to_read_bytes, read_callback):
#       self._stream.read_bytes(to_read_bytes, read_callback)

    def write(self, packet):
        self._ppmtdb.send_queue.put( (self,packet) ) # put a pub4sublist task into queue

    def close_for_reconnect(self, new_context, clean):
#        logging.info ("close_for_reconnect ============> clean: %s" % clean)
        self._connect_state = mq_cs_reconnected
        self.close()

        """
        if not clean and self._curr_pub_msg:
            new_context._out_mid = self._out_mid
            self._curr_pub_msg.context = new_context
            new_context._curr_pub_msg = self._curr_pub_msg
            if new_context._curr_pub_msg:
                #new_context._curr_pub_msg.context = new_context
                new_context._curr_pub_msg.reset() # reset to publish original state
                new_context.wait_pub_completed_timeout()

        """
    def close(self):

        if self._connect_state == mq_cs_disconnecting:
            if self._cnn_flag & 0x02: # clean flag is set (1)
                self._ppmtdb.work_queue.put( (self._ppmtdb.remove_cnn, (self,)) ) # put a disconnect task into queue

        elif self._connect_state == mq_cs_connected: # to handle will topic
            if self._cnn_flag & 0x04: # will flag is set(1)
                qos = (self._cnn_flag & 0x18) >> 3
                topic = self._will_topic
                message = self._will_message
#               print "qos:%d topic: %s   message: %s" % (qos, topic, message)

                if self._cnn_flag & 0x20: # will retain is set (1)
                    self._ppmtdb.work_queue.put( (self._ppmtdb.add_pub, (topic, qos, message)) ) # put add pub task into queue
                self._ppmtdb.work_queue.put( (self._ppmtdb.pub4pub_process, (topic, qos, message)) ) # put a pub task into queue

        self._stream.close()
        return

    def close_callback(self):
        PollIOLoop.instance().remove_timeout(self._timeout_handle)
        if self._connect_state == mq_cs_connected:
            logging.info(" close_callback --> device_id: %s " % (  self._client_id))
            self._ppmtdb.work_queue.put( (self._ppmtdb.set_inactive_cnn, (self,)) ) # put a set inactive task into queue
        return
    
    def keep_alive_timeout(self):        
        if not self._keep_alive: # dont set timeout if keep_alive is zero that be set by client
            return
        logging.info(" keep_alive_timeout --> device_id: %s " % (  self._client_id))
        self.close()
        return
            
    def reset_timeout(self):
        if not self._keep_alive: # dont set timeout if keep_alive is zero that be set by client
            return
        if self._timeout_handle != None:
            PollIOLoop.instance().remove_timeout(self._timeout_handle)
            self._timeout_handler = None
        self._timeout_handle = PollIOLoop.instance().call_later(1.5*self._keep_alive, self.keep_alive_timeout)
        logging.info(" reset_timeout --> device_id: %s" % (self._client_id))
        return

    def _acl_check(self, topic, acl):
        topic_token_list = filter(None, topic.split('/'))
        len0 = len(topic_token_list)
        for acl_topic in acl:
            acl_token_list = filter(None, acl_topic.split('/'))
            len1 = len(acl_token_list)
            if len0 == len1:
                for i in xrange(len0):
                    if topic_token_list[i] != acl_token_list[i] and acl_token_list[i] != "+" and acl_token_list[i] != "#":
                        break
                else: return True
            elif len0 == len1-1 and acl_token_list[len1-1] == "#":
                for i in xrange(len0):
                    if topic_token_list[i] != acl_token_list[i] and acl_token_list[i] != "+":
                        break
                else: return True
            elif len0 > len1:
                for i in xrange(len1):
                    if topic_token_list[i] != acl_token_list[i] and acl_token_list[i] != "+" and acl_token_list[i] != "#":
                        break
                else: return True        
        return False

    def wait_pub_queue_handler_callback(self):
        if self._curr_pub_msg:
            return
        if self._wait_pub_queue.empty() == True:
            return
        mid, wait_state, packet, topic, qos = self._wait_pub_queue.get(False)
        self._curr_pub_msg = mqtt3_message(self, mid, wait_state, packet, topic, qos)
        self._ppmtdb.work_queue.put( (self._ppmtdb.inc_in_flight_o, ()) ) # put an inc in_flight_o into queue
        self.write(packet)
        return
        
    def wait_pub_completed_timeout(self):
        if self._curr_pub_msg != None:
            return
        
        logging.info ("wait_pub_completed_timeout: resend_times ( %d) " % self._curr_pub_msg.resend_times)
        if self._curr_pub_msg.resend_times == 5:
            self.close()
        else:
            self._curr_pub_msg.resend()
            self._curr_pub_msg.timeout = PollIOLoop.instance().call_later(7, self.wait_pub_completed_timeout)
        return

    
class mqtt3server(TCPServer):    
    def handle_stream(self, stream, address):
        logging.info("yvmqtt3 New connection : (%s) " % str(address))
        mqtt3context(stream)

class MQTTSrv(Application):
    def __init__(self, *args, **kwargs):
        super(MQTTSrv, self).__init__(*args, autoreload=True)
        return
    
    def ppmt_main(self):
        #print "Mqtt3 Broker start ......"
        
        logging.info("Mqtt3 Broker start ......")
        work_thread = worker(gMQbase.work_queue)
        send_thread = sender(gMQbase.send_queue)
        
        work_thread.start()
        send_thread.start()
        
        server = mqtt3server()    
        server.listen(1883)
        
        PollIOLoop.instance().start()   
        
        send_thread.join()
        work_thread.join()
        
        gMQbase.work_queue.join()
        gMQbase.send_queue.join()
        return
        
def ppmt_main():
    _m = MQTTSrv()
    _m.ppmt_main()

def ppmt_set_authenticate(_class):
    global gAuth
    gAuth = _class()
    return
