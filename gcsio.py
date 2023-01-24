
import pandas as pd
import io
import re
from google.cloud import storage

storage_client = storage.Client()


def s3read_using(FUN, Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        storage_client.bucket(Bucket).blob(Key).download_to_file(f)
        f.seek(0)
        df = FUN(f, **kwargs)
    return df


def s3_write_parquet(df, Bucket, Key):
    for col in df.dtypes[df.dtypes=='datetime64[ns]'].index.values:
        df[col] = df[col].dt.round(freq='ms') # parquet doesn't support ns timestamps
    with io.BytesIO() as f:
        df.to_parquet(f)
        f.seek(0)
        storage_client.bucket(Bucket).blob(Key).upload_from_file(f)


def s3_write_excel(df, Bucket, Key):
    with io.BytesIO() as f:
        df.to_excel(f)
        f.seek(0)
        storage_client.bucket(Bucket).blob(Key).upload_from_file(f)


def s3_write_csv(df, Bucket, Key):
    with io.StringIO() as f:
        df.to_csv(f)
        f.seek(0)
        storage_client.bucket(Bucket).blob(Key).upload_from_file(f)


def s3_read_parquet(Bucket, Key, **kwargs):
    return s3read_using(pd.read_parquet, Bucket, Key, **kwargs)


def s3_read_excel(Bucket, Key, **kwargs):
    return s3read_using(pd.read_excel, Bucket, Key, **kwargs)


def s3_read_feather(Bucket, Key, **kwargs):
    return s3read_using(pd.read_feather, Bucket, Key, **kwargs)


def s3_read_csv(Bucket, Key, **kwargs):
    with io.String() as f:
        storage_client.bucket(Bucket).blob(Key).download_to_file(f)
        f.seek(0)
        df = pd.read_csv(f, **kwargs)
    return df


def s3_list_objects(Bucket, Prefix, **kwargs):
    blobs = storage_client.bucket(Bucket).list_blobs(prefix=Prefix, **kwargs)
    return [blob.name for blob in blobs]


def s3_read_parquet_hive(bucket, key):

    if len(s3_list_objects(Bucket = bucket, Prefix = key, max_results = 1)) > 0:
        date_ = re.search('\d{4}-\d{2}-\d{2}', key).group(0)
        df = (s3_read_parquet(Bucket=bucket, Key=key)
                .assign(Date = lambda x: pd.to_datetime(date_, format='%Y-%m-%d'))
                .rename(columns = {'Timestamp': 'TimeStamp'}))
    else:
        df = pd.DataFrame()

    return df


# Read Corridors File from S3
def get_corridors(bucket, key, keep_signalids_as_strings = True):
    corridors = s3_read_parquet(Bucket=bucket, Key=key)
    corridors = (corridors[~corridors.SignalID.isna()]
        .drop(['Description'], axis=1))

    if not keep_signalids_as_strings:
        corridors.SignalID = corridors.SignalID.astype('int')

    return corridors


def get_signalids(date_, conf):
    date_str = date_.strftime('%Y-%m-%d')
    bucket = conf['bucket']
    key_prefix = conf['key_prefix']
    prefix = f'{key_prefix}/detections/date={date_str}'
    keys = s3_list_objects(Bucket=bucket, Prefix=prefix)
    return [re.search('(?<=de_)\d+(?=_)', k).group() for k in keys]


def get_det_config(date_, conf):
    '''
    date_ [Timestamp]
    conf [dict]
    '''

    def read_det_config(bucket, key):
        dc = s3_read_parquet(Bucket=bucket, Key=key)
        dc.loc[dc.DetectionTypeDesc.isna(), 'DetectionTypeDesc'] = '[]'
        return dc

    date_str = date_.strftime('%Y-%m-%d')

    bucket = conf['bucket']
    key_prefix = conf['key_prefix']

    dc_prefix = f'{key_prefix}/config/atspm_det_config_good/date={date_str}'
    dc_keys = s3_list_objects(bucket, dc_prefix)

    dc = pd.concat(list(map(lambda k: read_det_config(bucket, k), dc_keys)))

    bd_key = f'{key_prefix}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet'
    if len(s3_list_objects(Bucket=bucket, Prefix=bd_key)) > 0:
        bd = s3_read_parquet(Bucket=bucket, Key=bd_key)
        bd.Detector = bd.Detector.astype('int64')

        df = pd.merge(dc, bd, how='outer', on=['SignalID','Detector']).fillna(value={'Good_Day': 1})
        df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])
    else:
        df = dc

    return df
