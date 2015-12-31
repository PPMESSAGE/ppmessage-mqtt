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
    name = "ppmessage",
    version = "1.0.0",
    description = "mqtt broker lib of PPMESSAGE",
    author = "ppmessage.com",
    license = "http://www.apache.org/licenses/LICENSE-2.0",
    install_requires = ["tornado>=4.3"],
    packages = ["ppmessage"],
    author_email = 'dingguijin@gmail.com',
    keywords = 'mqtt broker ppmessage',
    url = "https://www.ppmessage.com",
)
