# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical, PPMESSAGE
# Ding Guijin, guijin.ding@yvertical.com
# Ning Ben, ben.ning@yvertical.com
#
# All rights are reserved.
#

from .ppauth import authenticate

class your_authenticate(authenticate):
    def verify_client_id(self, client_id):
        return True
    def verify_user_password(self, user_id, password):
        return True

    
