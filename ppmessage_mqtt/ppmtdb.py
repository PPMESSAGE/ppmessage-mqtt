# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical, PPMESSAGE
# Ding Guijin, guijin.ding@yvertical.com
# Ning Ben, ben.ning@yvertical.com
#
# All rights are reserved.
#

import threading
import logging
import struct
import time

from Queue import Queue
from tornado.ioloop import PollIOLoop    

# Worker command
MQTT3_CNNECT = 0x10
MQTT3_CONNACK = 0x20
MQTT3_PUBLISH = 0x30
MQTT3_PUBACK = 0x40
MQTT3_PUBREC = 0x50
MQTT3_PUBREL = 0x60


# db cmd code for db maintain
DBCMD_INVALID   = 0
DBCMD_DEL_I_MSG = 1
DBCMD_DEL_O_MSG = 2
DBCMD_ADD_I_MSG = 3
DBCMD_UPD_O_MSG = 4
DBCMD_FINISH    = 5

class sys_stat:
    def __init__(self):
        self._received_bytes_per_sec = 0
        self._sent_bytes_per_sec = 0

        self._received_bytes = 0
        self._sent_bytes = 0
        self._total_received_bytes = 0
        self._total_sent_bytes = 0

        self._active_clients = 0
        self._inactive_clients = 0
        self._total_clients = 0

        self._received_messages_per_sec = 0
        self._sent_messages_per_sec = 0

        self._received_messages = 0
        self._sent_messages = 0
        self._total_received_messages = 0
        self._total_sent_messages = 0

        self._received_publish_messages = 0
        self._sent_publish_messages = 0
        self._subscriptions_count = 0

        self._retain_messages = 0
        self._in_flight_o = 0
        self._in_flight_i = 0
        self._timestamp = time.time()
        self._uptime = 0
        self._version = "1.00"

    def recalculate(self):
        curr_time = time.time()
        self._uptime = curr_time - self._timestamp
        self._timestamp = curr_time
        self._received_bytes_per_sec = long(self._received_bytes / self._uptime)
        self._sent_bytes_per_sec = long(self._sent_bytes / self._uptime)
        self._received_messages_per_sec = long(self._received_messages / self._uptime)
        self._sent_messages_per_sec =  long(self._sent_messages / self._uptime)

        self._total_received_bytes += self._received_bytes
        self._total_sent_bytes += self._sent_bytes
        self._total_received_messages += self._received_messages
        self._total_sent_messages += self._sent_messages

        self._received_bytes = 0
        self._sent_bytes = 0
        self._received_messages = 0
        self._sent_messages = 0


class yvmq_msg_state:
    invalid = 0
    wait_puback = 1
    wait_pubrec = 2
    wait_pubrel = 3
    wait_pubcomp = 4

class mqtt3_message:
    def __init__(self, context, mid, state, packet, topic, qos):
        self.context = context
        self.mid = mid
        self.state0 = state
        self.state = state
        self.packet0 = packet
        self.packet = packet
        self.topic = topic
        self.qos = qos
        self.timeout = None
        self.resend_times = 0

    def reset(self):
        self.state = self.state0
        self.packet = self.packet0
        self.resend_times = 0

    def resend(self):
        self.packet[0] |= 0x08 # dup be set (1) 

        self.context.write(self.packet)
        self.resend_times += 1
        return
        
class mqtt3cnn:
    def __init__(self, context):
        self.client_id = context._client_id
        self.cnn_flag = context._cnn_flag
        self.context = context
        self.subs = {} # { subtopic: qos }
        self.wait_pub_queue = Queue()
        self.active = True

    def clear(self):
        self.subs.clear()
        # to empty the queue
        while True:
            if self.wait_pub_queue.empty() == True:
                break
            self.wait_pub_queue.get(False)
        return
    
    def resend_message(self):
        PollIOLoop.instance().add_callback(self.context.wait_pub_queue_handler_callback)
        return

class mqtt3pub:
    def __init__(self, token,  qos=0, level=0, message=None, parent=None):
        self.token = token
        self.qos = qos
        self.message= message
        self.level = level
        self.children = {} # {next_level_token: mqtt3pub}
        self.parent = parent

class mqtt3sub:
    def __init__(self, token, qos=0, level=0, client_id=None, parent=None):
        self.token = token
        self.level = level
        self.clients = {}   # {client_id: qos}
        self.children = {}  # {next_level_token: mqtt3sub}
        self.parent = parent

        self.add_client(client_id, qos)

        if token is '#' and parent:
            parent.add_client(client_id, qos)

    def add_client(self, client_id, qos):
        if client_id:
            self.clients[client_id] = qos

    def remove_client(self, client_id):
        if self.clients.get(client_id):
            del self.clients[client_id]

    def clean_client(self):
        self.clients = {}

class ppmtdb(object):

    def __init__(self):

        self.pub_root = mqtt3pub('/')
        self.sub_root = mqtt3sub('/')
        self.cnns = {} # { client_id: mqtt3cnn }
        self.level = 0
        self.work_queue = Queue()
        self.send_queue = Queue()
        self.sys_info = sys_stat()
        self.update_sysinfo_timeout = PollIOLoop.instance().add_timeout(self.sys_info._timestamp + 15, self.update_sysinfo_callback)

    def add_pub(self,fulltopic, qos, message):


        parent = self.pub_root
        pub = self.pub_root.children

        token_list = filter(None, fulltopic.split('/'))
        i = 0
        n = len(token_list)

        for token in token_list:
            i += 1
            if not pub.get(token):

                self.sys_info._retain_messages += 1

                if i == n:
                    pub[token] = mqtt3pub(token, qos, i, message, parent)
                else:
                    pub[token] = mqtt3pub(token, qos, i, None, parent)
                    parent = pub[token]
                    pub = pub[token].children
            else:
                if i == n:
                    pub[token].qos = qos
                    pub[token].message = message
                else:
                    parent = pub[token]
                    pub = pub[token].children

    def unpub(self,fulltopic, qos):

        parent = self.pub_root
        pub = self.pub_root.children
        curr_pub = self.pub_root

        token_list = filter(None, fulltopic.split('/'))
        i = 0
        n = len(token_list)
        matched = False

        for token in token_list:
            i += 1
            if token in pub:
                if i == n:
                    matched = True
                    self.sys_info._retain_messages -= 1
                    pub[token].message = None
                    curr_pub = pub[token]
                else:
                    parent = pub[token]
                    pub = pub[token].children
            else:
                break

        if matched:
            while curr_pub.level and not len(curr_pub.children) and not curr_pub.message:
                token = curr_pub.token
                parent = curr_pub.parent
                del parent.children[token]
                curr_pub = parent


    def add_sub(self,fulltopic, qos, client_id=None):

        parent = self.sub_root
        sub = self.sub_root.children

        token_list = filter(None, fulltopic.split('/'))

        i = 0
        n = len(token_list)

        for token in token_list:
            i += 1
            if not sub.get(token):
                if i == n:
                    sub[token] = mqtt3sub(token, qos, i, client_id, parent)
                else:
                    sub[token] = mqtt3sub(token, qos, i, None, parent)
                    parent = sub[token]
                    sub = sub[token].children
            else:
                if i == n:
                    sub[token].add_client(client_id, qos)
                    if token is '#' and parent.level:
                        parent.add_client(client_id, qos)
                else:
                    parent = sub[token]
                    sub = sub[token].children

        if self.cnns.has_key(client_id):
            self.cnns[client_id].subs[fulltopic] = qos
            self.sys_info._subscriptions_count += 1

    def unsub(self, topic, client_id):
        parent = self.sub_root
        curr_sub = self.sub_root
        matched = False
        sub = self.sub_root.children
        token_list = filter(None, topic.split('/'))

        i = 0
        n = len(token_list)

        for token in token_list:
            i += 1
            if token in sub:
                if i == n:
                    matched = True
                    curr_sub = sub[token]
                    sub[token].remove_client(client_id)
                    if token is '#' and parent.level:
                        parent.remove_client(client_id)
                else:
                    parent = sub[token]
                    sub = sub[token].children
            else:
                break
        
        if matched:
            while curr_sub.level and not len(curr_sub.children):
                token = curr_sub.token
                parent = curr_sub.parent
                del parent.children[token]
                curr_sub = parent

        if client_id in self.cnns and topic in self.cnns[client_id].subs:
            del self.cnns[client_id].subs[topic]
            self.sys_info._subscriptions_count -= 1

    def add_cnn(self, context, clean=True):

        if not context: return

        if clean:
            if context._client_id in self.cnns:
                old_context = self.cnns[context._client_id].context
                if context is not old_context:
                    PollIOLoop.instance().add_callback(old_context.close_for_reconnect, context, clean)
                    
                if not self.cnns[context._client_id].active:
                    self.sys_info._active_clients += 1
                    self.sys_info._inactive_clients -= 1
                    self.cnns[context._client_id].active = True

                self.cnns[context._client_id].clear()
                self.cnns[context._client_id].context = context
                self.cnns[context._client_id].cnn_flag = context._cnn_flag
            else:
                self.sys_info._total_clients += 1
                self.sys_info._active_clients += 1
                self.cnns[context._client_id] = mqtt3cnn(context)

            context._wait_pub_queue = self.cnns[context._client_id].wait_pub_queue
        else:
            if context._client_id in self.cnns:
                old_context = self.cnns[context._client_id].context
                if context is not old_context:
                    PollIOLoop.instance().add_callback(old_context.close_for_reconnect, context, clean)

                self.cnns[context._client_id].context = context
                self.cnns[context._client_id].cnn_flag = context._cnn_flag
                if not self.cnns[context._client_id].active:
                    self.cnns[context._client_id].active = True
                    self.sys_info._active_clients += 1
                    self.sys_info._inactive_clients -= 1
                context._wait_pub_queue = self.cnns[context._client_id].wait_pub_queue
            else:
                self.cnns[context._client_id] = mqtt3cnn(context)
                self.sys_info._active_clients += 1
                self.sys_info._total_clients += 1
                context._wait_pub_queue = self.cnns[context._client_id].wait_pub_queue

    def _clean_sub(self, context):
        if not context: return
        if context._client_id in self.cnns:
            topic_list = self.cnns[context._client_id].subs.keys()
            for topic in topic_list:
                self.unsub(topic, context._client_id)

    def remove_cnn(self, context):
        if not context: return
        self._clean_sub(context)
        if context._client_id in self.cnns:
            if self.cnns[context._client_id].active:
                self.sys_info._active_clients -= 1
            else:
                self.sys_info._inactive_clients -= 1

            self.sys_info._total_clients -= 1

            self.cnns[context._client_id].clear()
            del self.cnns[context._client_id]

        
    def inc_in_flight_i(self):
        self.sys_info._in_flight_i += 1

    def dec_in_flight_i(self):
        self.sys_info._in_flight_i -= 1

    def inc_in_flight_o(self):
        self.sys_info._in_flight_o += 1

    def dec_in_flight_o(self):
        self.sys_info._in_flight_o -= 1
    
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
                break
        return

    # publish message (to check already exits publish and send)  message to this subscriber
    def pub4sub_list(self, context, sub_list):

        pub_dict = self.pub_root.children
        for (topic, qos) in sub_list:
            pub_token_list = []
            sub_token_list = filter(None, topic.split('/'))[::-1] # reverse the list for recursive call
            self.pub4sub(context, sub_token_list, qos, pub_token_list, pub_dict, False)
#           if ret == DBCMD_FINISH:
#               break

    def pub4sub(self, context, sub_token_list, qos, pub_token_list, pub_dict, multi_level):
#       cmd = DBCMD_INVALID
        if len(sub_token_list):
            sub_token = sub_token_list.pop() # pop sub_list
            multi_level = False
            if sub_token is '+':
                for pub_token in sorted(pub_dict):
                    pub_token_list.append(pub_token) # pubtopic push
                    next_pub_dict = pub_dict[pub_token].children
                    if len(sub_token_list):
                        if sub_token_list[-1] is '#' and pub_dict[pub_token].message:
                            pub_qos = min(qos, pub_dict[pub_token].qos)
                            msg = pub_dict[pub_token].message
                            topic = "/".join(pub_token_list)
                            self.mqtt3_send_publish(context, topic, pub_qos, msg)
                        if len(next_pub_dict):
                            self.pub4sub(context, sub_token_list, qos, pub_token_list, next_pub_dict, multi_level)

                    elif pub_dict[pub_token].message:
                        pub_qos = min(qos, pub_dict[pub_token].qos)
                        msg = pub_dict[pub_token].message
                        topic = "/".join(pub_token_list)
                        self.mqtt3_send_publish(context, topic, pub_qos, msg)
                        
                    pub_token_list.pop() # pub topic pop
#                   if cmd == DBCMD_FINISH:
#                       break


            elif sub_token is '#':
                multi_level = True
                for pub_token in sorted(pub_dict):
                    pub_token_list.append(pub_token) # push
                    if pub_dict[pub_token].message:
                        pub_qos = min(qos, pub_dict[pub_token].qos)
                        msg = pub_dict[pub_token].message
                        topic = "/".join(pub_token_list)
                        self.mqtt3_send_publish(context,topic, pub_qos, msg)
                    next_pub_dict = pub_dict[pub_token].children
                    if len(next_pub_dict):
                        self.pub4sub(context, sub_token_list, qos, pub_token_list, next_pub_dict, multi_level)
                    pub_token_list.pop() # pop
#                   if cmd == DBCMD_FINISH:
#                       break
                    
            elif sub_token in pub_dict:
                pub_token_list.append(sub_token) # push

                if len(sub_token_list):

                    if sub_token_list[-1] is '#' and pub_dict[sub_token].message:
                        pub_qos = min(qos, pub_dict[sub_token].qos)
                        msg = pub_dict[sub_token].message
                        topic = "/".join(pub_token_list)
                        self.mqtt3_send_publish(context, topic, pub_qos, msg)

                    next_pub_dict = pub_dict[sub_token].children
                    if len(next_pub_dict):
                        self.pub4sub(context, sub_token_list, qos, pub_token_list, next_pub_dict, multi_level)
                else:
                    if pub_dict[sub_token].message:
                        pub_qos = min(qos, pub_dict[sub_token].qos)
                        msg = pub_dict[sub_token].message
                        topic = "/".join(pub_token_list)
                        self.mqtt3_send_publish(context, topic, pub_qos, msg)

                pub_token_list.pop() # pop
                
            sub_token_list.append(sub_token) # push sub_list

        elif multi_level:
            for pub_token in sorted(pub_dict):
                pub_token_list.append(pub_token) # push

                if pub_dict[pub_token].message:
                    pub_qos = min(qos, pub_dict[pub_token].qos)
                    msg = pub_dict[pub_token].message
                    topic = "/".join(pub_token_list)
                    self.mqtt3_send_publish(context, topic, pub_qos, msg)

                next_pub_dict = pub_dict[pub_token].children
                if len(next_pub_dict):
                    self.pub4sub(context, sub_token_list, qos, pub_token_list, next_pub_dict, multi_level)

                pub_token_list.pop() # pop
#               if cmd == DBCMD_FINISH:
#                   break
#       return cmd

    # publish message (to check already exits subscribe and send) for this publish
    def pub4pub(self, topic, qos, message, pub_token_list, sub_dict):
        
        pub_token = pub_token_list.pop()

        if sub_dict.has_key(pub_token):
            if len(pub_token_list):
                next_sub_dict = sub_dict[pub_token].children
                self.pub4pub(topic, qos, message, pub_token_list, next_sub_dict)
            else:
                for client_id in sub_dict[pub_token].clients:
                    if self.cnns[client_id].active:
                        pub_qos = min( qos, sub_dict[pub_token].clients[client_id] )
                        context = self.cnns[client_id].context
                        self.mqtt3_send_publish(context,topic, pub_qos, message) # send publish message to the client (with client_id) 
                    else:
                        pub_qos = min( qos, sub_dict[pub_token].clients[client_id] )
                        context = self.cnns[client_id].context

        if sub_dict.has_key('+'):
            if len(pub_token_list):
                next_sub_dict = sub_dict['+'].children
                self.pub4pub(topic, qos, message, pub_token_list, next_sub_dict)
            else:
                for client_id in sub_dict['+'].clients:
                    if self.cnns[client_id].active:
                        pub_qos = min( qos, sub_dict['+'].clients[client_id] )
                        context = self.cnns[client_id].context
                        self.mqtt3_send_publish(context, topic, pub_qos, message) # send publish message to the client (with client_id) 
                    else:
                        pub_qos = min( qos, sub_dict['+'].clients[client_id] )
                        context = self.cnns[client_id].context


        if sub_dict.has_key('#'):
            for client_id in sub_dict['#'].clients:
                if self.cnns[client_id].active:
                    pub_qos = min( qos, sub_dict['#'].clients[client_id] )
                    context = self.cnns[client_id].context
                    self.mqtt3_send_publish(context,topic, pub_qos, message) # send publish message to the client (with client_id) 
                else:
                    pub_qos = min( qos, sub_dict['#'].clients[client_id] )
                    context = self.cnns[client_id].context
                    logging.info("inactive pubtopic: %s device_id: %s " % (topic, context._client_id))

        pub_token = pub_token_list.append(pub_token)
        return

    def mqtt3_send_publish(self, context, topic, qos, message):

        logging.info("sended pub --> topic: %s   device_id: %s " % (topic, context._client_id))

        packet = bytearray()
        packet.extend(struct.pack("!B", MQTT3_PUBLISH | (qos << 1) ))

        topic_len = len(topic)
        msg_len = 0 if message == None else len(message)
        remain_len = 2 + topic_len + msg_len

        if qos: 
            remain_len += 2 # for mid

        self._pack_remain_len(packet, remain_len)

        fmt = "!H" + str(topic_len) + "s"

        # topic_len + topic
        packet.extend(struct.pack(fmt, topic_len, topic))

        if qos: # for mid
            context._out_mid += 1
            if 65536==context._out_mid:
                context._out_mid = 1

            packet.extend(struct.pack("!H", context._out_mid))
        
        if msg_len: # for payload
            fmt =  "!" + str(msg_len) + "s"
            packet.extend(struct.pack(fmt, message))

        mid = context._out_mid

        wait_state = yvmq_msg_state.invalid
 
        if qos == 1:
            wait_state = yvmq_msg_state.wait_puback
        elif qos == 2:
            wait_state = yvmq_msg_state.wait_pubrec
        
        if wait_state:
#           self.inc_in_flight_o()
#           context._wait_pub_queue.put( (mid, wait_state, packet) )

            context._wait_pub_queue.put( (mid, wait_state, packet, topic, qos) ) # modify on June 14, 2014 for PPMESSAGE just send onece. Ben Ning

#           PollIOLoop.instance().add_callback(context.wait_pub_queue_handler_callback, MQTT3_PUBLISH)
            PollIOLoop.instance().add_callback(context.wait_pub_queue_handler_callback)
        else:
            self.send_queue.put( (context, packet) ) # put a send packet task into send_queue

        self.sys_info._sent_publish_messages += 1

#       return cmd


    def pub4pub_process(self, topic, qos, message):
#       topic, qos, message = pub_tuple
        self.sys_info._received_publish_messages += 1

        pub_token_list = filter(None, topic.split('/'))[::-1] # reverse the list for recursive call
        sub_dict = self.sub_root.children

        self.pub4pub(topic, qos, message, pub_token_list, sub_dict)
        
    def add_sublist(self, client_id, sub_list):
        for (sub_topic, qos) in sub_list:
            self.add_sub(sub_topic, qos, client_id)

    def unsublist(self, client_id, unsub_list):
        for topic in unsub_list:
            self.unsub(topic, client_id)

    def add_sent_bytes(self, sent_bytes):
        self.sys_info._sent_bytes += sent_bytes
        self.sys_info._sent_messages += 1
#       self.update_sys_info_topic()

    def add_received_bytes(self, received_bytes):
        self.sys_info._received_bytes += received_bytes
#       self.update_sys_info_topic()

    def add_received_message(self, received_messages):
        self.sys_info._received_messages += received_messages

    def set_inactive_cnn(self, context):
        if context._client_id in self.cnns:
            if self.cnns[context._client_id].active:
                self.cnns[context._client_id].active = False
                self.sys_info._active_clients -= 1
                self.sys_info._inactive_clients += 1
                logging.info("set_inactive_cnn: device_id: %s " % (context._client_id))
        return
            
    def pub_sysinfo(self, topic, qos, message):
        pub_token_list = filter(None, topic.split('/'))[::-1] # reverse the list for recursive call
        sub_dict = self.sub_root.children
        self.pub4pub(topic, qos, message, pub_token_list, sub_dict)

    def update_sys_info_topic(self):
        qos = 0
        self.pub_sysinfo( "$SYS/stat/clients/active", qos, str(self.sys_info._active_clients) )
        self.pub_sysinfo( "$SYS/stat/clients/inactive", qos, str(self.sys_info._inactive_clients) )
        self.pub_sysinfo( "$SYS/stat/clients/total", qos, str(self.sys_info._total_clients) )

        self.pub_sysinfo( "$SYS/stat/bytes/received", qos, str(self.sys_info._total_received_bytes) )
        self.pub_sysinfo( "$SYS/stat/bytes/per_second/received", qos, str(self.sys_info._received_bytes_per_sec) )
        self.pub_sysinfo( "$SYS/stat/bytes/sent", qos, str(self.sys_info._total_sent_bytes) )
        self.pub_sysinfo( "$SYS/stat/bytes/per_second/sent", qos, str(self.sys_info._sent_bytes_per_sec) )

        self.pub_sysinfo( "$SYS/stat/message/received", qos, str(self.sys_info._total_received_messages) )
        self.pub_sysinfo( "$SYS/stat/message/per_second/received", qos, str(self.sys_info._received_messages_per_sec) )
        self.pub_sysinfo( "$SYS/stat/message/sent", qos, str(self.sys_info._total_sent_messages) )
        self.pub_sysinfo( "$SYS/stat/message/per_second/sent", qos, str(self.sys_info._sent_messages_per_sec) )
        self.pub_sysinfo( "$SYS/stat/message/retain", qos, str(self.sys_info._retain_messages) )
        self.pub_sysinfo( "$SYS/stat/message/in_flight_in", qos, str(self.sys_info._in_flight_i) )
        self.pub_sysinfo( "$SYS/stat/message/in_flight_out", qos, str(self.sys_info._in_flight_o) )

        self.pub_sysinfo( "$SYS/stat/publish/messages/received", qos, str(self.sys_info._received_publish_messages) )
        self.pub_sysinfo( "$SYS/stat/publish/messages/sent", qos, str(self.sys_info._sent_publish_messages) )

        self.pub_sysinfo( "$SYS/stat/subscriptions/count", qos, str(self.sys_info._subscriptions_count) )
        self.pub_sysinfo( "$SYS/stat/timestamp", qos, str(self.sys_info._timestamp) )


    def update_sysinfo_callback(self):
#       self.recount_cnns()
        self.sys_info.recalculate()

        self.work_queue.put( (self.update_sys_info_topic, ()) )
        self.update_sysinfo_timeout = PollIOLoop.instance().add_timeout(self.sys_info._timestamp + 15, self.update_sysinfo_callback)

   

class worker(threading.Thread): 
    def __init__(self, work_queue):
        super(worker, self).__init__()
        self.setDaemon(True)
        self.queue = work_queue

    def run(self):
        while True:
            try:
                callback_fun, args = self.queue.get()
                callback_fun(*args)
            finally:
                self.queue.task_done()


class sender(threading.Thread): 
    def __init__(self, send_queue):
        super(sender, self).__init__()
        self.setDaemon(True)
        self.queue = send_queue

    def run(self):
        while True:
            try:
                context, packet = self.queue.get()
                buff = str(packet)
                PollIOLoop.instance().add_callback(self.ioloop_callback, context, buff)
            finally:
                self.queue.task_done()

    # The callback will be call in main thread 
    def ioloop_callback(self, context, buff):
        if not context._stream.closed():
            sent_bytes = len(buff)
            context._stream.write(buff)
            context._ppmtdb.work_queue.put( (context._ppmtdb.add_sent_bytes, (sent_bytes,)) ) # put add pub task into queue
#       else:
#           context.res_queue.put( (DBCMD_FINISH, None) )


        
