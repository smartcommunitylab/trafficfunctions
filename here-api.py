import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import base64
  
from pandas.io.json import json_normalize
from botocore.client import Config
from datetime import datetime

#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
S3_BUCKET=os.environ.get('S3_BUCKET')
S3_CONFIG_BUCKET=os.environ.get('S3_CONFIG_BUCKET')

HERE_APP_ID=os.environ.get('HERE_APP_ID')
HERE_APP_CODE=os.environ.get('HERE_APP_CODE')
HERE_ENDPOINT="https://traffic.api.here.com/traffic/6.2/flow.json"

BPOINTS_PARQUET='trentino-boundpoints.parquet'
BBOX='45.667805,10.446625;46.547528,11.965485'


class BytesIOWrapper(io.BufferedReader):
    """Wrap a buffered bytes stream over TextIOBase string stream."""

    def __init__(self, text_io_buffer, encoding=None, errors=None, **kwargs):
        super(BytesIOWrapper, self).__init__(text_io_buffer, **kwargs)
        self.encoding = encoding or text_io_buffer.encoding or 'utf-8'
        self.errors = errors or text_io_buffer.errors or 'strict'

    def _encoding_call(self, method_name, *args, **kwargs):
        raw_method = getattr(self.raw, method_name)
        val = raw_method(*args, **kwargs)
        return val.encode(self.encoding, errors=self.errors)

    def read(self, size=-1):
        return self._encoding_call('read', size)

    def read1(self, size=-1):
        return self._encoding_call('read1', size)

    def peek(self, size=-1):
        return self._encoding_call('peek', size)


def read_json_from_url(url,params):
    response = requests.get(url,params=params)
    if(response.status_code == 200):
        return response.json()
    else:
        raise Exception('Response error, status code {}'.format(response.status_code)) 
        
        
def grouped_weighted_average(self, values, weights, *groupby_args, **groupby_kwargs):
    """
    :param values: column(s) to take the average of
    :param weights_col: column to weight on
    :param group_args: args to pass into groupby (e.g. the level you want to group on)
    :param group_kwargs: kwargs to pass into groupby
    :return: pandas.Series or pandas.DataFrame
    """

    if isinstance(values, str):
        values = [values]

    ss = []
    for value_col in values:
        df = self.copy()
        prod_name = 'prod_{v}_{w}'.format(v=value_col, w=weights)
        weights_name = 'weights_{w}'.format(w=weights)

        df[prod_name] = df[value_col] * df[weights]
        df[weights_name] = df[weights].where(~df[prod_name].isnull())
        df = df.groupby(*groupby_args, **groupby_kwargs).sum()
        s = df[prod_name] / df[weights_name]
        s.name = value_col
        ss.append(s)
    df = pd.concat(ss, axis=1) if len(ss) > 1 else ss[0]
    return df     


#extend pd
pd.DataFrame.grouped_weighted_average = grouped_weighted_average

def handler(context, event):
    #init client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

    params={
        'bbox':BBOX,
        'app_id':HERE_APP_ID,
        'app_code':HERE_APP_CODE
        }

    #encode bbox as base64 
    bbox64 = str(base64.b64encode(BBOX.encode("utf-8")), "utf-8")
    filename='traffic-bbox-'+bbox64

    context.logger.info("call HERE api for data for "+filename)

    try:        
        res = read_json_from_url(HERE_ENDPOINT,params)
        timestamp = str(res['CREATED_TIMESTAMP'])
        date=datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
        context.logger.info('received data for '+str(timestamp))

        list = []
        #read data as json
        rws = res['RWS']
        for r in rws:
            for s in r['RW']:
                for q in s['FIS']:
                    for v in q['FI']:
                        le = v['TMC']['LE']
                        pc = v['TMC']['PC']
                        for z in v['CF']:
                            entry = {
                            "free_flow_speed": z['FF'],
                            "jam_factor": z['JF'],
                            "length": v['TMC']['LE'],
                            "speed": z['SP'],
                            "tmc_id":  v['TMC']['PC'],
                            "timestamp": str(timestamp),
                            "confidence_factor": z['CN']
                            }
                            list.append(entry)
                        #end z
                    #end v
                #end q
            #end s
        #end r

        path = date.strftime('%Y%m%d')+"/"+date.strftime('%H')+"/"
        filename = filename + "-"+timestamp        

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

        context.logger.info('read results in dataframe')
        df = pd.DataFrame(list, columns=["free_flow_speed", "jam_factor", "length", "speed", "tmc_id", "timestamp", "confidence_factor"])

        count = len(df)
        context.logger.info('read count: '+str(count))

        context.logger.info('process segments...')

        #process to pack all TMC segments into one
        #calculate weighted mean for measures wrt length
        gwdf = df.grouped_weighted_average(['speed','jam_factor','free_flow_speed'],'length',['timestamp','tmc_id'])

        #sum length of segments 
        ldf = df.groupby(['timestamp','tmc_id'])['length'].sum()

        #get min confidence factor for tmc_id 
        cdf = df.groupby(['timestamp','tmc_id'])['confidence_factor'].min()

        #merge and reset index to move timestamp, tmc_id as columns
        resdf = gwdf.merge(ldf, left_index=True, right_index=True).merge(cdf, left_index=True, right_index=True)
        resdf.reset_index(level=['timestamp','tmc_id'], inplace=True)

        #parse timestamp as date
        resdf['timestamp'] = pd.to_datetime(resdf['timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')

        context.logger.info('res count: '+str(len(resdf)))

        # write to io buffer
        context.logger.info('write parquet to buffer')

        parquetio = io.BytesIO()
        resdf.to_parquet(parquetio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        parquetio.seek(0)

        context.logger.info('upload to s3 as '+filename+'.parquet')

        s3.upload_fileobj(parquetio, S3_BUCKET, path+filename+'.parquet')

        #filter 
        context.logger.info('load bounding points mapping from s3 '+BPOINTS_PARQUET)

        obj = s3.get_object(Bucket=S3_CONFIG_BUCKET, Key=BPOINTS_PARQUET)
        dataio = io.BytesIO(obj['Body'].read())
        boundpoints = pd.read_parquet(dataio, engine='pyarrow', columns=['CID','LCD','LON','LAT'])

        context.logger.info('merge data with bound points for region...')
        mergeddf = pd.merge(resdf, boundpoints, how='inner', left_on='tmc_id', right_on='LCD')

        context.logger.info('res count: '+str(len(mergeddf)))

        filename = filename + "-filtered"
        
        # write to io buffer
        context.logger.info('write parquet to buffer')

        filterio = io.BytesIO()
        mergeddf.to_parquet(filterio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        filterio.seek(0)

        context.logger.info('upload to s3 as '+filename+'.parquet')

        s3.upload_fileobj(filterio, S3_BUCKET, path+filename+'.parquet')


        # # write csv
        # context.logger.info('write csv to buffer')

        # csvio = io.StringIO()
        # mergeddf.to_csv(csvio, header=True, index=False)
        # # seek to start otherwise upload will be 0
        # csvio.seek(0)

        # # wrap as byte with reader
        # wrapio = BytesIOWrapper(csvio)

        # context.logger.info('upload to s3 as '+filename+'.csv')

        # s3.upload_fileobj(wrapio, S3_BUCKET, path+filename+'.csv')

        #cleanup
        del mergeddf
        del resdf
        del boundpoints
        del ldf
        del gwdf
        del df
        del list

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


