# ppmessage-mqtt
Python mqtt server at PPMESSSAGE (http://www.ppmessage.com)

# install
pip install ppmessage-mqtt

# test

## start server
```
from ppmessage_mqtt import mqtt_server
from tornado.options import parse_command_line()

if __name__ == "__main__":
    # initial tornado options (logging used)
    parse_command_line()
    mqtt_server()
```

## client to publish message with paho-mqtt
cd test
pip install paho-mqtt

cd test
python test/main.py


## overwrite antheticate
```
from ppmessage_mqtt import authenticate
from ppmessage_mqtt import mqtt_authenticate

class your_authenticate(authenticate):
    def verify_client_id(self, client_id):
        return True
    def verify_user_password(self, user_id, password):
        return True

if __name__ == "__main__":
    # initial tornado options (logging used)
    parse_command_line()
    mqtt_authenticate(your_authenticate)
    mqtt_server()
```    
