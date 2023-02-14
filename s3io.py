
import pandas as pd
import io
import os
import re
import boto3
import yaml

with open('Monthly_Report.yaml') as yaml_file:
    conf = yaml.load(yaml_file, Loader=yaml.Loader)

session = boto3.Session(profile_name=conf['profile'], region_name=conf['aws_region'])
s3 = session.client('s3', verify=conf['ssl_cert'])
athena = session.client('athena', verify=conf['ssl_cert'])

# These set environment variables for this session only
credentials = session.get_credentials()
os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
os.environ['AWS_AWS_DEFAULT_REGION'] = conf['aws_region']

# ---------------------------

def s3read_using(FUN, Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        s3.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=f)
        f.seek(0)
        df = FUN(f, **kwargs)
    return df


def s3_write_parquet(df, Bucket, Key, **kwargs):
    for col in df.dtypes[df.dtypes=='datetime64[ns]'].index.values:
        df[col] = df[col].dt.round(freq='ms') # parquet doesn't support ns timestamps
    with io.BytesIO() as f:
        df.to_parquet(f, **kwargs)
        f.seek(0)
        s3.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())


def s3_write_excel(df, Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        df.to_excel(f, **kwargs)
        f.seek(0)
        s3.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())


def s3_write_feather(df, Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        df.to_feather(f, **kwargs)
        s3.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())


def s3_write_csv(df, Bucket, Key, **kwargs):
    with io.StringIO() as f:
        df.to_csv(f, **kwargs)
        f.seek(0)
        s3.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())


def s3_read_parquet(Bucket, Key, **kwargs):
    return s3read_using(pd.read_parquet, Bucket, Key, **kwargs)


def s3_read_excel(Bucket, Key, **kwargs):
    return s3read_using(pd.read_excel, Bucket, Key, **kwargs)


def s3_read_feather(Bucket, Key, **kwargs):
    return s3read_using(pd.read_feather, Bucket, Key, **kwargs)


# Uses io.String() instead of io.Bytes()
def s3_read_csv(Bucket, Key, **kwargs):
    with io.String() as f:
        s3.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=f)
        f.seek(0)
        df = pd.read_csv(f, **kwargs)
    return df


s3_list_objects = s3.list_objects_v2

def s3_read_parquet_hive(bucket, key):
    if 'Contents' in s3_list_objects(Bucket = bucket, Prefix = key):
        date_ = re.search('\d{4}-\d{2}-\d{2}', key).group(0)
        df = (s3_read_parquet(Bucket=bucket, Key=key)
                .assign(Date = lambda x: pd.to_datetime(date_, format='%Y-%m-%d'))
                .rename(columns = {'Timestamp': 'TimeStamp'}))
    else:
        df = pd.DataFrame()

    return df


def get_keys(s3, bucket, prefix, callback=lambda x: x):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix)
    if 'Contents' in response.keys():
        for cont in response['Contents']:
            yield callback(cont['Key'])

    while 'NextContinuationToken' in response.keys():
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=response['NextContinuationToken'])
        for cont in response['Contents']:
            yield callback(cont['Key'])


def get_signalids(date_, conf):
    date_str = date_.strftime('%Y-%m-%d')
    bucket = conf['bucket']
    key_prefix = conf['key_prefix']
    prefix = f'detections/date={date_str}'
    keys = get_keys(s3, bucket, prefix, callback = lambda k: re.search('(?<=_)\d+(?=_)', k).group())
    return keys


def get_det_config(date_, conf):
    '''
    date_ [Timestamp]
    conf [dict]
    '''

    def read_det_config(s3, bucket, key):
        dc = read_feather(Bucket=bucket, Key=key)
        dc.loc[dc.DetectionTypeDesc.isna(), 'DetectionTypeDesc'] = '[]'
        return dc

    date_str = date_.strftime('%Y-%m-%d')

    bucket = conf['bucket']

    bd = read_parquet(Bucket=bucket, Key=f'mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet')
    bd.SignalID = bd.SignalID.astype('int64')
    bd.Detector = bd.Detector.astype('int64')

    dc_prefix = f'atspm_det_config_good/date={date_str}'
    dc_keys = get_keys(s3, bucket, dc_prefix)

    dc = pd.concat(list(map(lambda k: read_det_config(s3, bucket, k), dc_keys)))

    df = pd.merge(dc, bd, how='outer', on=['SignalID','Detector']).fillna(value={'Good_Day': 1})
    df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])

    return df
