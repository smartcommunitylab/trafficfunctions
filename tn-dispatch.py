import os
import io
import json
import paho.mqtt.client as mqtt
import calendar
import time
from datetime import datetime
from datetime import timedelta

#config
MQTT_BROKER='172.17.0.1'
MQTT_TOPIC='traffic-tn/ranges'
MQTT_CLIENT='dispatch'

TYPES = ['Narx','Spot','RDT']

def handler(context, event):
    #params - expect json
    message = {}

    if(event.content_type == 'application/json'):
        message = event.body

    else:
        jsstring = event.body.decode('utf-8').strip()

        if not jsstring:
                return context.Response(body='Error. Empty json',
                                headers={},
                                content_type='text/plain',
                                status_code=400)

        message = json.loads(jsstring)

    context.logger.info(message)

    date_start =  datetime.fromisoformat(message['start'])
    date_end =  datetime.fromisoformat(message['end'])

    client = mqtt.Client(MQTT_CLIENT+"_"+event.id) #create new instance
    client.connect(MQTT_BROKER) #connect to broker
    client.loop_start() #init loop


    interval_start = date_start
    interval_end = date_start.replace(hour=23,minute=59,second=59)
    stop = (interval_end > date_end)

    if(not stop):
        for t in TYPES:
            #send first interval then iterate
            interval = {
                "start": interval_start.isoformat(),
                "end": interval_end.isoformat(),
                "type": t
            }
            js = json.dumps(interval)
            context.logger.debug(js)
            client.publish(MQTT_TOPIC,js)

        #loop over days
        while(not stop):
            interval_start=interval_end + timedelta(seconds=1)
            interval_end = interval_start.replace(hour=23,minute=59,second=59)
            stop = (interval_end > date_end)

            if (not stop):
                for t in TYPES:
                    interval = {
                        "start": interval_start.isoformat(),
                        "end": interval_end.isoformat(),
                        "type": t                
                    }
                    js = json.dumps(interval)
                    client.publish(MQTT_TOPIC,js)
                    context.logger.debug(js)
            

    context.logger.info('done.')
    client.loop_stop()
    time.sleep(1)
    client.disconnect()

    return context.Response(body='Done',
                            headers={},
                            content_type='text/plain',
                            status_code=200)