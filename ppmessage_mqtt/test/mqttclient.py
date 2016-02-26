# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical. PPMESSAGE.
# Guijin Ding, guijin.ding@yvertical.com
#

from paho.mqtt import client as mqttc
from Queue import Queue

import json
import logging
import datetime

MQTT_PORT = 1883
MQTT_HOST = "127.0.0.1"

class MQTTClient():

    def __init__(self):
        self._mqttc = mqttc.Client("admins")
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        self._mqttc.on_disconnect = self.mqtt_on_disconnect
        self._mqttc_connection_status = "NULL"
        self._active_time = datetime.datetime.now()
        self._push_queue = Queue()
        self._published = set()
        return

    def _conn_str(self, _code):
        _d = {
            0: "Connection successful",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorised",
        }
        _str = _d.get(_code)
        if _str == None:
            _str = "unknown error"
        return _str
    
    def mqtt_on_connect(self, mqttc, userdata, flags, rc):
        logging.info("mqtt_on_connect rc: " + self._conn_str(rc))
        if rc == 0:
            self._mqttc_connection_status = "CONNECTED"
        return

    def mqtt_on_message(self, mqttc, userdata, msg):
        logging.info(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
        return

    def mqtt_on_publish(self, mqttc, userdata, mid):
        logging.info("published mid: " + str(mid))
        if mid in self._published:
            self._published.remove(mid)
        self._active_time = datetime.datetime.now()
        return

    def mqtt_on_subscribe(self, mqttc, userdata, mid, granted_qos):
        logging.info("Subscribed: "+str(mid)+" "+str(granted_qos))
        return

    def mqtt_on_log(self, mqttc, userdata, level, string):
        logging.info("on-log:" + string)
        return
        
    def mqtt_on_disconnect(self, mqttc, userdata, rc):
        logging.info("mqtt-on-disconnect:" + str(rc))
        self._mqttc_connection_status = "NULL"
        return
    
    def username_pw_set(self, user, password):
        self._mqttc.username_pw_set(user, password)
        
    def connect(self):
        if self._mqttc_connection_status == "CONNECTED":
            return

        self._mqttc_connection_status = "CONNECTING"
        self._published = set()

        # server will auto disconnect after 120"
        # client should disconnect before server
        # for mqttclient can not get the server disconnect event??
        _r = self._mqttc.connect(MQTT_HOST, MQTT_PORT, 120)
        return
        
    def publish_one(self, _token, _body):
        #topic, payload, qos=1, retain=True):
        qos = 1
        retain = True
        _body = json.dumps(_body)
        self._push_queue.put([_token, _body, qos, retain])
        return True

    def disconnect(self):
        if self._mqttc_connection_status == "NULL":
            return
        self._mqttc_connection_status = "NULL"
        self._mqttc.disconnect()
        return
    
    def outdate(self, _delta):
        if self._mqttc_connection_status == "NULL":
            return
        _now = datetime.datetime.now()
        if _now - self._active_time > _delta:
            self.disconnect()
        return
    
    def loop(self):
        if self._mqttc_connection_status == "NULL":
            self.connect()
            return

        if self._mqttc_connection_status != "CONNECTED":
            logging.info("looping for: " + self._mqttc_connection_status)
            self._mqttc.loop()
            return

        if self._push_queue.empty() == True:
            if len(self._published) > 0:
                self._mqttc.loop()
                return

        _push = self._push_queue.get(False)
        if _push == None:
            if len(self._published) > 0:
                self._mqttc.loop()
            return
        self._push_queue.task_done()

        result, mid = self._mqttc.publish(*_push)
        if result == mqttc.MQTT_ERR_SUCCESS:
            self._published.add(mid)
        elif result == mqttc.MQTT_ERR_NO_CONN:
            self._push_queue.put(_push)
            self.connect()
        else:
            self._push_queue.put(_push)
            logging.info("WHAT HAPPEND?? %d" % result)
        
        self._mqttc.loop()
        return

    def start_send(self):
        while True:
            self.loop()
            
            if self._push_queue.empty() == True and len(self._published) == 0:
                logging.info("nothing to push")
                break

            if self._mqttc_connection_status == "NULL":
                logging.info("mqttclient connection error")
                break
        return
