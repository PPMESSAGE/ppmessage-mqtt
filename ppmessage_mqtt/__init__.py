# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical, PPMessage
# Ding Guijin, guijin.ding@ppmessage.com
# Ning Ben, ben.ning@yvertical.com
#
# All rights reserved
#

from .ppmt import ppmt_main
from .ppmt import ppmt_set_authenticate
from .ppauth import authenticate

def mqtt_server():
    ppmt_main()
    return

def mqtt_authenticate(_class):
    ppmt_set_authenticate(_class)
    return

__version__ = "1.0.9"
