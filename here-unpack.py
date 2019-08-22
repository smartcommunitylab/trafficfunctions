import pandas as pd
import boto3
import botocore
import io
import json
import os
import base64
import psycopg2
import sqlalchemy

from botocore.client import Config
from datetime import datetime

#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
S3_BUCKET=os.environ.get('S3_BUCKET')
S3_CONFIG_BUCKET=os.environ.get('S3_CONFIG_BUCKET')
DB_HOST=os.environ.get('DB_HOST')
DB_NAME=os.environ.get('DB_NAME')
DB_USERNAME=os.environ.get('DB_USERNAME')
DB_PASSWORD=os.environ.get('DB_PASSWORD')


BPOINTS_PARQUET='trentino-boundpoints.parquet'
BBOX='46.4249,9.8756;45.7789,11.9657'

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

    offset = int(msg['offset'])
    limit = int(msg['limit'])

    #encode bbox as base64 
    bbox64 = str(base64.b64encode(BBOX.encode("utf-8")), "utf-8")
    filename_prefix='traffic-bbox-'+bbox64

    #init client
    s3 = boto3.client('s3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1')

    
    con = psycopg2.connect(user = DB_USERNAME,
        password = DB_PASSWORD,
        host = DB_HOST,
        port = "5432",
        database = DB_NAME)


    #read boundpoints
    obj = s3.get_object(Bucket=S3_CONFIG_BUCKET, Key=BPOINTS_PARQUET)
    dataio = io.BytesIO(obj['Body'].read())
    boundpoints = pd.read_parquet(dataio, engine='pyarrow', columns=['CID','LCD','LON','LAT'])


    # execute query
    query = 'select data,created_date from public."trafficData" LIMIT %s OFFSET %s'
    cur = con.cursor()
    cur.execute(query, (limit, offset))

    #loop over rows containing single responses as json
    for row in cur:
        try:
            list = []
            #read data as json
            timestamp = str(row[0]['CREATED_TIMESTAMP'])
            date=datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
            rws = row[0]['RWS']
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

            #process data
            path = date.strftime('%Y%m%d')+"/"+date.strftime('%H')+"/"
            filename = filename_prefix + "-"+timestamp        
    
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
            else:
                #process
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
    
                #cleanup
                del mergeddf
                del resdf
                del ldf
                del gwdf
                del df
            
            #always clear list    
            del list

        except Exception as e:
            context.logger.error('Error: '+str(e))  
    #end row  

    #close
    con.close()


    return context.Response(body='Done',
        headers={},
        content_type='text/plain',
        status_code=200)