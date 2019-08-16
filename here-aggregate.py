import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import base64
import time
import re
import pytz
  
from pandas.io.json import json_normalize
from botocore.client import Config
from datetime import datetime
from datetime import timedelta    

from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry, Counter, REGISTRY, generate_latest,CONTENT_TYPE_LATEST

#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
S3_BUCKET=os.environ.get('S3_BUCKET')
INTERVAL=os.environ.get('INTERVAL')

BBOX='45.667805,10.446625;46.547528,11.965485'
# the following MUST be set via OS ENV for multiprocess
# os.environ["prometheus_multiproc_dir"] = "/tmp"


def init_context(context):
    global COUNTER_FRAMES
    global COUNTER_DF
    global COUNTER_OUT
    context.logger.info('init')
    COUNTER_FRAMES = Counter('frames', 'Number of file frames read')
    COUNTER_DF = Counter('df', 'Number of data frames read')
    COUNTER_OUT = Counter('out', 'Number of data frames outputted')


def metrics(context, event):
    context.logger.info('called metrics')
    #use multiprocess metrics otherwise data collected from different processors is not included
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    output = generate_latest(registry).decode('UTF-8')

    return context.Response(body=output,
        headers={},
        content_type=CONTENT_TYPE_LATEST,
        status_code=200)       


def get_matching_s3_objects(s3,bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(s3, bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(s3, bucket, prefix, suffix):
        yield obj["Key"]

        
def map_time_minutes(dt , interval):
    ts = time.mktime(dt.timetuple())
    newTime = ts - ((dt.minute % interval) * 60) - dt.second
    return datetime.fromtimestamp(int(newTime))


timeregex = re.compile(r'^((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$')

def parse_time(time_str):
    """
    Parse a time string e.g. (2h13m) into a timedelta object.

    :param time_str: A string identifying a duration.  (eg. 2h13m)
    :return datetime.timedelta: A datetime.timedelta object
    """
    parts = timeregex.match(time_str)
    assert parts is not None, "Could not parse any time information from '{}'.  Examples of valid strings: '8h', '2d8h5m20s', '2m4s'".format(time_str)
    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)    


def handler(context, event):
    try:
        # check if metrics called
        if event.trigger.kind == 'http' and event.method == 'GET' and event.path == '/metrics':
            return metrics(context, event)
        else:
            return process(context, event)

        
    except Exception as e:
        context.logger.error('Error: '+str(e))        
        return context.Response(body='Error '+str(e),
                        headers={},
                        content_type='text/plain',
                        status_code=500)   

def process(context, event):            
    #params
    date = datetime.today().astimezone(pytz.UTC)

    datestring = event.body.decode('utf-8').strip()
    if datestring:
        date = datetime.strptime(datestring, '%Y-%m-%dT%H:%M:%S')
        

    #init client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')


    # derive start - end from date as previous interval 
    interval_delta = parse_time(INTERVAL)
    # get interval as minutes for mapping function
    interval_minutes = (interval_delta.seconds//60)%60

    date_end = map_time_minutes(date, interval_minutes)
    date_start = date_end - interval_delta

    #subtract 1s from end to avoid overlap
    date_end = date_end - timedelta(seconds=1)

    #encode bbox as base64 
    bbox64 = str(base64.b64encode(BBOX.encode("utf-8")), "utf-8")
    filename='aggregate-bbox-'+bbox64+"-"+ date_start.strftime('%Y-%m-%dT%H:%M:%S')+"_"+date_end.strftime('%Y-%m-%dT%H:%M:%S')
    path = date_start.strftime('%Y%m%d')+"/"+date_start.strftime('%H')+"/"

    #check if files already exist
    exists = False
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=path+filename+'.parquet')
        exists = True
    except botocore.exceptions.ClientError:
        # Not found
        exists = False
        pass


    if exists:
        context.logger.info('files for '+filename+' already exists in bucket, skip.')
        return context.Response(body='File already exists',
                headers={},
                content_type='text/plain',
                status_code=200)       


    context.logger.info("interval "+INTERVAL+" for date "+str(date) + " as "+str(date_start)+" => "+str(date_end))      
    context.logger.info("list frames for date "+str(date_start) + " from path "+path)      
    # fetch only data from selected bbox
    prefix = path+"traffic-bbox-"+bbox64+"-"

    try: 
        frames = []
        for obj in get_matching_s3_objects(s3, bucket=S3_BUCKET, prefix=prefix, suffix='-filtered.parquet'):
            key = obj["Key"]
            m = re.search('traffic-bbox-(.+?)-(.+?)-filtered.parquet', key)
            if m:
                kbbox = m.group(1)
                ktime = m.group(2)
                kdate=datetime.strptime(ktime, '%Y-%m-%dT%H:%M:%S.%f%z')
                if(map_time_minutes(kdate, interval_minutes) == date_start):
                    #context.logger.debug("read from "+key)        
                    kobj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                    kdataio = io.BytesIO(kobj['Body'].read())
                    kdf = pd.read_parquet(kdataio, engine='pyarrow')
                    #add confidence factor to old data if missing
                    if 'confidence_factor' not in kdf:
                        kdf['confidence_factor'] = 0.0

                    frames.append(kdf)


        context.logger.info('frames count: '+str(len(frames)))
        COUNTER_FRAMES.inc(len(frames))

        #concat
        df = pd.concat(frames)
        context.logger.info('res count: '+str(len(df)))
        COUNTER_DF.inc(len(df))

        #parse data from timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')

        context.logger.info('build groups and calculate means...')

        #process to calculate mean over interval
        grouped = df.groupby(['tmc_id'])
        bins = []
        for id, group in grouped:
            rxf = group.groupby(pd.Grouper(key='timestamp',freq=INTERVAL))['tmc_id','speed','jam_factor','free_flow_speed','confidence_factor'].mean().reset_index(level=['timestamp'])    
            bins.append(rxf)

        outdf = pd.concat(bins)
        #drop NaN because grouper inserts results for each interval, even when we have no real data
        outdf = outdf.dropna()

        #sort and format
        outdf.sort_values(['timestamp','tmc_id'], inplace=True)
        outdf['tmc_id']=outdf['tmc_id'].astype(int)
        outdf.reset_index(drop=True)

        context.logger.info('out count: '+str(len(outdf)))

        # merge with data
        df.drop(columns=['timestamp','speed','jam_factor','free_flow_speed','confidence_factor'], inplace=True)
        # need to drop duplicates on df since it holds multiple measures
        df.drop_duplicates(keep='first', inplace=True)

        mergeddf = pd.merge(outdf, df, how='left', on='tmc_id')    


        context.logger.info('merged count: '+str(len(mergeddf)))
        COUNTER_OUT.inc(len(mergeddf))

        # write to io buffer
        context.logger.info('write parquet to buffer')

        parquetio = io.BytesIO()
        mergeddf.to_parquet(parquetio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        parquetio.seek(0)

        context.logger.info('upload to s3 as '+filename+'.parquet')
        s3.upload_fileobj(parquetio, S3_BUCKET, path+filename+'.parquet')

        # # write csv
        # context.logger.info('write csv to buffer')

        # csvio = io.StringIO()
        # outdf.to_csv(csvio, header=True, index=False)
        # # seek to start otherwise upload will be 0
        # csvio.seek(0)

        # # wrap as byte with reader
        # wrapio = BytesIOWrapper(csvio)

        # context.logger.info('upload to s3 as '+filename+'.csv')
        # s3.upload_fileobj(wrapio, S3_BUCKET, path+filename+'.csv')

        # cleanup
        del mergeddf
        del outdf
        del parquetio
        del bins
        del df

        return context.Response(body='Done',
                    headers={},
                    content_type='text/plain',
                    status_code=200)
    
    except Exception as e:
        context.logger.error('Error: '+str(e))        
        return context.Response(body='Error '+str(e),
                        headers={},
                        content_type='text/plain',
                        status_code=500)        