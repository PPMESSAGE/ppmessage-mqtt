# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical, PPMESSAGE
# Ding Guijin, guijin.ding@yvertical.com
# Ning Ben, ben.ning@yvertical.com
#
# All rights reserved
#

from setuptools import setup, find_packages
from distutils.core import setup, Extension
import os

setup(
    name = "ppmessage-mqtt",
    version = "1.0.11",
    author = "ppmessage.com",
    author_email = 'dingguijin@gmail.com',
    license = "http://www.apache.org/licenses/LICENSE-2.0",
    install_requires = ["tornado>=4.3"],
    packages = ["ppmessage_mqtt"],
    url = "http://www.ppmessage.com",
    keywords = "mqtt server ppmessage",
    description="A Python mqtt server, originally developed at PPMessage.",
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
    ],

)
