import pandas as pd
import boto3
import io
import json
import os
import requests
import base64
import time  
import pymongo

from botocore.client import Config
from datetime import datetime
from datetime import timedelta    
from pymongo import MongoClient 


#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
S3_BUCKET=os.environ.get('S3_BUCKET')

MONGODB_HOST=os.environ.get('MONGODB_HOST')
MONGODB_PORT=os.environ.get('MONGODB_PORT')
MONGODB_USERNAME=os.environ.get('MONGODB_USERNAME')
MONGODB_PASS=os.environ.get('MONGODB_PASS')
MONGODB_DB=os.environ.get('MONGODB_DB')
MONGODB_AUTHDB=os.environ.get('MONGODB_AUTHDB')

def _connect_mongo(host, port, username, password, db, authdb):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s?authSource=%s' % (username, password, host, port, db, authdb)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]

def read_mongo(db, collection, query={}, no_id=True, no_class=True):
    """ Read from Mongo and Store into DataFrame """

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    if no_class:
        del df['_class']        

    return df


def read_events(db, query={}):
    cursor = db['wsnEvent'].find(query).sort([("timestamp", pymongo.ASCENDING)])
    events=[]
    for i in cursor:
        try:
            #unpack payload
            i.update(i['payload'])
            events.append(i)
        except Exception as e:
            #print(e)
    
    df = pd.DataFrame(events)
    #drop _id
    del df['_id']
    #drop payload
    del df['payload']
    del df['_class']
    return df



def handler(context, event):
    try:
        # parse date from params
        date = datetime.today().astimezone(pytz.UTC)

        datestring = event.body.decode('utf-8').strip()
        if datestring:
            date = datetime.strptime(datestring, '%Y-%m-%d')


        # derive delta for timestamp as 1day (from 00 to 23:59:59)
        date_start = date.replace(minute=00,second=00, microsecond=00) - timedelta(hours=1)
        date_end = date_start + timedelta(seconds=86399)

        filename='climb-{}-'+ date_start.strftime('%Y-%m-%dT%H:%M:%S')+"_"+date_end.strftime('%Y-%m-%dT%H:%M:%S')
        path = date_start.strftime('%Y%m')+"/"

        #init client
        s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')


        # init client
        db = _connect_mongo(host=MONGODB_HOST, port=MONGODB_PORT, username=MONGODB_USERNAME, password=MONGODB_PASS, db=MONGODB_DB, authdb=MONGODB_AUTHD)


        #check if files already exist
        filenamePassenger = filename.format('passengers')
        existsPassenger = False
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=path+filenamePassenger+'.parquet')
            existsPassenger = True
        except botocore.exceptions.ClientError:
            # Not found
            existsPassenger = False
            pass
        
        filenameVolunteer = filename.format('volunteers')
        existsVolunteer = False
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=path+filenameVolunteer+'.parquet')
            existsVolunteer = True
        except botocore.exceptions.ClientError:
            # Not found
            existsVolunteer = False
            pass


        if existsPassenger or existsVolunteer:
            context.logger.info('files for '+filename+' already exists in bucket, skip.')
            return context.Response(body='File already exists',
                    headers={},
                    content_type='text/plain',
                    status_code=200)    


        # read shared data
        context.logger.info('read shared data...')
        df_route = read_mongo(db, 'route')
        df_institute = read_mongo(db, 'institute')
        df_school = read_mongo(db, 'school')

        # process passengers
        context.logger.info('process passengers...')

        # read source data
        df_child = read_mongo(db, 'child')

        #read events data
        context.logger.info('read events...')

        query_events_101 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 101}}
        df_events_101 = read_events(db, query_events_101)
        df_events_101.set_index('passengerId', inplace=True)

        query_events_102 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 102}}
        df_events_102 = read_events(db, query_events_102)
        df_events_102.set_index('passengerId', inplace=True)

        query_events_103 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 103}}
        df_events_103 = read_events(db, query_events_103)
        df_events_103.set_index('passengerId', inplace=True)

        query_events_104 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 104}}
        df_events_104 = read_events(db, query_events_104)
        df_events_104.set_index('passengerId', inplace=True)

        query_events_501 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 501}}
        df_events_501 = read_events(db, query_events_501)
        df_events_501.set_index('passengerId', inplace=True)   

        #iterate and compose
        context.logger.info('compose events...')
        events = []
        for idx, row in df_events_102.iterrows():
            passengerId = idx

            #check if arrived == has event 104
            toDestination = passengerId in df_events_104.index

            #autoCheckin guess == has event 101
            autoCheckin = passengerId in df_events_101.index

            #fetch battery data = has event 501 with battery status
            batteryLevel = -1
            batteryVoltage = -1
            if passengerId in df_events_501.index:
                #get only last matching row
                battery = df_events_501.loc[[passengerId]].tail(1).iloc[0]
                batteryLevel = battery['batteryLevel']
                batteryVoltage = battery['batteryVoltage']

            #merge
            e = row
            e['passengerId'] = idx
            e['toDestination'] = toDestination
            e['autoCheckin'] = autoCheckin
            e['batteryLevel'] = batteryLevel
            e['batteryVoltage'] = batteryVoltage

            #append
            events.append(e)

        #parse as dataFrame        
        df_events_passengers = pd.DataFrame(events)    
        df_events_passengers.drop(axis=1, columns=['creationDate','lastUpdate','wsnNodeId','eventType'], inplace=True)

        #merge with related info
        df_events_passengers = pd.merge(df_events_passengers, df_child[['objectId','name','surname','classRoom','instituteId','schoolId']].add_suffix('_child'), how='left', suffixes=['_event','_child'], left_on='passengerId', right_on='objectId_child') 
        df_events_passengers = pd.merge(df_events_passengers, df_route[['objectId','name','instituteId','schoolId']].add_suffix('_route'), how='left', suffixes=['','_route'], left_on='routeId', right_on='objectId_route')
        df_events_passengers = pd.merge(df_events_passengers, df_institute[['objectId','name']].add_suffix('_institute'), how='left', suffixes=['','_institute'], left_on='instituteId_route', right_on='objectId_institute')
        df_events_passengers = pd.merge(df_events_passengers, df_school[['objectId','name']].add_suffix('_school'), how='left', suffixes=['','_school'], left_on='schoolId_route', right_on='objectId_school')
        df_events_passengers['date'] = df_events_passengers['timestamp'].dt.date


        context.logger.info('passenger events count: '+str(len(df_events_passengers)))

        # write to io buffer
        context.logger.info('write parquet to buffer')

        parquetio_passengers = io.BytesIO()
        df_events_passengers.to_parquet(parquetio_passengers, engine='pyarrow')
        # seek to start otherwise upload will be 0
        parquetio_passengers.seek(0)

        context.logger.info('upload to s3 as '+filenamePassenger+'.parquet')
        s3.upload_fileobj(parquetio_passengers, S3_BUCKET, path+filenamePassenger+'.parquet')


        # process volunteers
        context.logger.info('process volunteers...')

        # read source data
        df_volunteer = read_mongo(db, 'volunteer')

        #read events data
        context.logger.info('read events...')

        query_events_301 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 301}}
        df_events_301 = read_events(db, query_events_301)
        df_events_301['isDriver'] = True
        df_events_301.drop(axis=1, columns=['creationDate','lastUpdate','wsnNodeId','eventType'], inplace=True, errors='ignore')
        df_events_301.set_index('volunteerId', inplace=True)        

        query_events_301 = {'timestamp': {'$gte': date_start, '$lte': date_end},'eventType': {'$eq': 301}}
        df_events_301 = read_events(db, query_events_301)
        df_events_302['isDriver'] = False
        df_events_302.drop(axis=1, columns=['creationDate','lastUpdate','wsnNodeId','eventType'], inplace=True, errors='ignore')
        df_events_302.set_index('volunteerId', inplace=True)

        #iterate and compose
        context.logger.info('compose events...')

        df_events_volunteer = pd.concat([df_events_301, df_events_302])
        df_events_volunteer.reset_index(['volunteerId'], inplace=True)

        #merge with related info
        df_events_volunteer = pd.merge(df_events_volunteer, df_volunteer[['objectId','name','instituteId','schoolId']].add_suffix('_volunteer'), how='left', suffixes=['','_volunteer'], left_on='volunteerId', right_on='objectId_volunteer')
        df_events_volunteer = pd.merge(df_events_volunteer, df_route[['objectId','name','instituteId','schoolId']].add_suffix('_route'), how='left', suffixes=['','_route'], left_on='routeId', right_on='objectId_route')
        df_events_volunteer = pd.merge(df_events_volunteer, df_institute[['objectId','name']].add_suffix('_institute'), how='left', suffixes=['','_institute'], left_on='instituteId_route', right_on='objectId_institute')
        df_events_volunteer = pd.merge(df_events_volunteer, df_school[['objectId','name']].add_suffix('_school'), how='left', suffixes=['','_school'], left_on='schoolId_route', right_on='objectId_school')
        df_events_volunteer['date'] = df_events_volunteer['timestamp'].dt.date

        context.logger.info('volunteer events count: '+str(len(df_events_volunteer)))

        # write to io buffer
        context.logger.info('write parquet to buffer')

        parquetio_volunteer = io.BytesIO()
        df_events_volunteer.to_parquet(parquetio_volunteer, engine='pyarrow')
        # seek to start otherwise upload will be 0
        parquetio_volunteer.seek(0)

        context.logger.info('upload to s3 as '+filenameVolunteer+'.parquet')
        s3.upload_fileobj(parquetio_volunteer, S3_BUCKET, path+filenameVolunteer+'.parquet')

    except Exception as e:
        context.logger.error('Error: '+str(e))        
        return context.Response(body='Error '+str(e),
                        headers={},
                        content_type='text/plain',
                        status_code=500)   