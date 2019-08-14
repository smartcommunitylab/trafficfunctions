import pandas as pd
import sqlalchemy
import boto3
import botocore
import io
import json
import os
import gc
  
from botocore.client import Config

#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
DB_HOST=os.environ.get('DB_HOST')
DB_PORT=os.environ.get('DB_PORT')
DB_NAME=os.environ.get('DB_NAME')
DB_USERNAME=os.environ.get('DB_USERNAME')
DB_PASSWORD=os.environ.get('DB_PASSWORD')
DB_SCHEMA=os.environ.get('DB_SCHEMA')
DB_TABLE_PREFIX=os.environ.get('DB_TABLE_PREFIX')
DB_CHUNKSIZE=5000


def handler(context, event):
    #params - expect json
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

    context.logger.info(msg)

    bucket = msg['bucket']
    key =  msg['key']


    if not key.endswith('.parquet'):
        return context.Response(body='Error. Not a parquet file',
            headers={},
            content_type='text/plain',
            status_code=400)


    #init client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')


    context.logger.info('download from s3 bucket '+bucket+' key '+key)

    obj = s3.get_object(Bucket=bucket, Key=key)
    dataio = io.BytesIO(obj['Body'].read())

    context.logger.info('read parquet into pandas dataframe')

    df = pd.read_parquet(dataio, engine='pyarrow')

    count = len(df)
    context.logger.info('read count: '+str(count))

    # use sqlalchemy because it supports multi/insert with pagination
    engine = sqlalchemy.create_engine('postgresql://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+DB_PORT+'/'+DB_NAME)

    # derive full table name from prefix + bucket + file prefix
    key_prefix = (key.rsplit('/',1)[1]).split('-')[0]
    table = DB_TABLE_PREFIX+'_'+bucket.replace('-','_')+'_'+key_prefix

    context.logger.debug('write dataframe into table '+table)

    df.to_sql(table, engine, schema=DB_SCHEMA, index=False, if_exists='append', method='multi', chunksize=DB_CHUNKSIZE)
  
    context.logger.info('Done.')

    # try to cleanup memory / which doesn't actually works in python..
    del df
    del dataio
    gc.collect()
    df = pd.DataFrame()
    del df

    return context.Response(body='Done. File '+key+' done',
                            headers={},
                            content_type='text/plain',
                            status_code=200)