# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2016 YVertical, PPMessage.
# Ding Guijin, guijin.ding@yvertical.com
# Ning Ben, ben.ning@yvertical.com
#
# All rights are reserved.
#


# CONNACK codes

CONNACK_ACCEPTED = 0
CONNACK_REFUSED_PROTOCOL_VERSION = 1
CONNACK_REFUSED_IDENTIFIER_REJECTED = 2
CONNACK_REFUSED_SERVER_UNAVAILABLE = 3
CONNACK_REFUSED_BAD_USERNAME_PASSWORD = 4
CONNACK_REFUSED_NOT_AUTHORIZED = 5

class authenticate:
    def auth(self, client_id, user_id=None, password=None):

        if not self.verify_client_id(client_id):
            return CONNACK_REFUSED_IDENTIFIER_REJECTED

        if not self.verify_user_password(user_id, password):
            return CONNACK_REFUSED_BAD_USERNAME_PASSWORD

        return CONNACK_ACCEPTED

    def verify_client_id(self, client_id):
        # to do verify the client id. return True is verify ok, else False
        return True

    def verify_user_password(self, user_id, password):
        # to do verify the user id and password. return True is verify ok, else False
        return True

    def pub_acl_list(self, client_id):
        # to implement your get acl list code at here
#       acl = [client_id+"/+"]
        acl = ["+/#"]
        return acl

    def sub_acl_list(self, client_id):
        # to implement your get acl list code at here
        acl = ["$SYS/stat/#", "+/#"]
        return acl
