import json
import paho.mqtt.client as mqtt
import os
import time
import uuid 
import re


#config
MQTT_BROKER=os.environ.get('MQTT_BROKER')
MQTT_TOPIC_PREFIX=os.environ.get('MQTT_TOPIC_PREFIX')
MQTT_CLIENT='dispatch'

def eventMapper(eventName):
    if eventName.startswith('s3:ObjectCreated'):
        return 'created'
    elif eventName.startswith('s3:ObjectRemoved'):
        return 'removed'
    elif eventName.startswith('s3:ObjectAccessed'):
        return 'accessed'
    else:
        return ''

def init_context(context):
    global client
    context.logger.info('init mqtt')
    client = mqtt.Client(MQTT_CLIENT+"-"+uuid.uuid4().hex[0:8]) #create new instance
    client.connect(MQTT_BROKER) #connect to broker
    client.loop_start()
    context.logger.info('mqtt loop started')


def handler(context, event):   
    #msg - expect json
    if(event.content_type == 'application/json'):
        msg = event.body
    else:
        jsstring = event.body.decode('utf-8').strip()

        if not jsstring:
            return context.Response(body='Error. Empty json',
                            headers={},
                            content_type='text/plain',
                            status_code=400)

        msg = json.loads(jsstring)

    context.logger.debug(msg)


    # list of events
    if 'Records' in msg:        
        for r in msg['Records']:
            #gather info to route
            eventName = r['eventName']
            bucket = r['s3']['bucket']['name']
            key = r['s3']['object']['key']
            #build dest topic
            topic = MQTT_TOPIC_PREFIX+bucket+'/'+eventMapper(eventName)
            #publish original msg to new destination
            client.publish(topic,payload=json.dumps(r), qos=2, retain=False)
            context.logger.debug('Routed {} {} {} to {}'.format(eventName, bucket, key, topic))

    #done
    context.logger.debug('Done.')

    return context.Response(body='Done.',
                            headers={},
                            content_type='text/plain',
                            status_code=200)
    
