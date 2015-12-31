# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical. PPMESSAGE.
# Ding Guijin, guijin.ding@yvertical.com
#

from mqttclient import MQTTClient

import uuid
import unittest
import logging

class TestMQTTClient(unittest.TestCase):

    def test_publishOne(self):
        #connect
        #publishone
        #disconnect
        #connect
        #publishone
        #...
        #publishone
        #disconnect
        topic = "test_topic"
        message = "test_message"
        mqtt_client = MQTTClient()
        
        mqtt_client.connect()
        mqtt_client.publish_one(topic, message)
        mqtt_client.publish_one(topic, message)
        #raw_input("Wait for restart push server and return.")
        mqtt_client.publish_one(topic, message)
        mqtt_client.publish_one(topic, message)
        mqtt_client.publish_one(topic, message)
        mqtt_client.start_send()
        mqtt_client.disconnect()

        
                
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
    unittest.main()
