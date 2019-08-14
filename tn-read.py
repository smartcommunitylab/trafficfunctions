import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import time
import re
import pytz
  
from requests.auth import HTTPBasicAuth
from botocore.client import Config
from datetime import datetime
from datetime import timedelta    

#config
# INTERVAL='1h'

API_USERNAME = os.environ.get('API_USERNAME')
API_PASSWORD = os.environ.get('API_PASSWORD')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')
INTERVAL = os.environ.get('INTERVAL')

#static setup 
ENDPOINT_TRAFFIC="https://tn.smartcommunitylab.it/trento.mobilitydatawrapper/traffic/{0}/{1}/{2}/{3}"
ENDPOINT_POSITIONS="https://tn.smartcommunitylab.it/trento.mobilitydatawrapper/positions/{0}"
AGGREGATE="By5m"
AGGREGATE_TIME=5*60*1000
TYPE=['Narx','RDT','Spot']

# class BytesIOWrapper(io.BufferedReader):
#     """Wrap a buffered bytes stream over TextIOBase string stream."""

#     def __init__(self, text_io_buffer, encoding=None, errors=None, **kwargs):
#         super(BytesIOWrapper, self).__init__(text_io_buffer, **kwargs)
#         self.encoding = encoding or text_io_buffer.encoding or 'utf-8'
#         self.errors = errors or text_io_buffer.errors or 'strict'

#     def _encoding_call(self, method_name, *args, **kwargs):
#         raw_method = getattr(self.raw, method_name)
#         val = raw_method(*args, **kwargs)
#         return val.encode(self.encoding, errors=self.errors)

#     def read(self, size=-1):
#         return self._encoding_call('read', size)

#     def read1(self, size=-1):
#         return self._encoding_call('read1', size)

#     def peek(self, size=-1):
#         return self._encoding_call('peek', size)


def read_df_from_url(url, context):
    context.logger.info("read from "+url)
    response = requests.get(url, auth=HTTPBasicAuth(API_USERNAME, API_PASSWORD))
    context.logger.info("response code "+str(response.status_code))
    if(response.status_code == 200):
        return pd.read_json(io.BytesIO(response.content), orient='records')
    else:
        return pd.DataFrame()



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
    #params - expect json
    message = {}
    datatypes = TYPE
    
    if(event.content_type == 'application/json'):
        message = event.body
        date_start =  datetime.fromisoformat(message['start'])
        date_end =  datetime.fromisoformat(message['end'])
        if 'type' in message:
            datatypes = [message['type']]

    else:
        datestring = event.body.decode('utf-8').strip()
        if datestring:
            date = datetime.strptime(datestring, '%Y-%m-%dT%H:%M:%S')
        else:
            date = datetime.today().astimezone(pytz.UTC)

        #derive interval
        interval_delta = parse_time(INTERVAL)
        # get interval as minutes for mapping function
        interval_minutes = (interval_delta.seconds//60)%60
    
        date_end = date.replace(minute=00,second=00, microsecond=00)
        date_start = date_end - interval_delta
    
        #subtract 1s from end to avoid overlap
        date_end = date_end - timedelta(seconds=1)
            

    context.logger.info('fetch data from API for interval '+str(date_start)+' => '+str(date_end))

    # calculate date interval as ms for API call
    time_start = int(datetime.timestamp(date_start)*1000)
    time_end = int(datetime.timestamp(date_end)*1000)
    day = date_start.strftime('%Y%m%d')

    #init s3 client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')    


    for datatype in datatypes:
        context.logger.info('fetch data for type '+datatype)
        filename='{}/{}/traffic-{}_{}-{}-{}'.format(datatype,day,datatype,AGGREGATE,
            date_start.strftime('%Y%m%dT%H%M%S'),date_end.strftime('%Y%m%dT%H%M%S'))

        #check if files already exist
        exists = False
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=filename+'.parquet')
            exists = True
        except botocore.exceptions.ClientError:
            # Not found
            exists = False
            pass


        if not exists:
            #load positions
            context.logger.info('read positions...')
            df_positions = read_df_from_url(ENDPOINT_POSITIONS.format(datatype), context)
            context.logger.info('num positions '+str(len(df_positions)))
            if(len(df_positions) > 0):
                df_positions[['latitude','longitude']] = pd.DataFrame(df_positions.coordinates.tolist(), columns=['latitude', 'longitude'])
                df_positions['place'] = df_positions['place'].str.strip()
            #load traffic
            context.logger.info('read traffic for '+datatype)
            df_traffic = read_df_from_url(ENDPOINT_TRAFFIC.format(datatype,AGGREGATE,time_start,time_end), context)
            context.logger.info('num traffic '+str(len(df_traffic)))

            if(len(df_traffic) > 0):
                #remove datatype from places with regex: anything between []
                df_traffic['place'].replace(regex=True, inplace=True, to_replace="\[(.*)\] ", value="")
                df_traffic['place'] = df_traffic['place'].str.strip()

                #merge with positions
                df = pd.merge(df_traffic, df_positions, on='place')

                #sort by timestamp/place
                df.sort_values(['time','place'], inplace=True)

                #calculate interval from timestamp used for aggregation
                df.rename(columns={'time':'time_end'}, inplace=True)
                df['time_start'] = df['time_end']-AGGREGATE_TIME

                #rename and drop columns
                df = df[['time_start','time_end','place','station','value','latitude','longitude']]

                # write to io buffer
                context.logger.info('write parquet to buffer')
                parquetio = io.BytesIO()
                df.to_parquet(parquetio, engine='pyarrow')
                # seek to start otherwise upload will be 0
                parquetio.seek(0)

                context.logger.info('upload to s3 as '+filename+'.parquet')
                s3.upload_fileobj(parquetio, S3_BUCKET, filename+'.parquet')


                # # write csv
                # context.logger.info('write csv to buffer')
                # csvio = io.StringIO()
                # df.to_csv(csvio, header=True, index=False)
                # # seek to start otherwise upload will be 0
                # csvio.seek(0)

                # # wrap as byte with reader
                # wrapio = BytesIOWrapper(csvio)

                # context.logger.info('upload to s3 as '+filename+'.csv')
                # s3.upload_fileobj(wrapio, S3_BUCKET, filename+'.csv')

                # # send message with filename

                # cleanup
                del df
                del parquetio
                # del wrapio
                # del csvio
            
            del df_positions
            del df_traffic

    #end types loop

    context.logger.info('done.')

    return context.Response(body='Done. Interval '+str(date_start)+' to '+str(date_end)+' for types '+str(datatypes),
                            headers={},
                            content_type='text/plain',
                            status_code=200)



