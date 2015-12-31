# ppmessage-mqtt
It is a python mqtt server of PPMESSSAGE (https://www.ppmessage.com)

```
from ppmessage import mqtt_server
import tornado.options

if __name__ == "__main__":
    tornado.options.parse_command_line()
    mqtt_server()
```
