
import pandas as pd
import io
import re
from google.cloud import storage

storage_client = storage.Client()


def s3read_using(FUN, Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        storage_client.bucket(Bucket).blob(Key).download_to_file(f)
        df = FUN(f, **kwargs)
    return df


def s3_write_parquet(df, Bucket, Key):
    for col in df.dtypes[df.dtypes=='datetime64[ns]'].index.values:
        df[col] = df[col].dt.round(freq='ms') # parquet doesn't support ns timestamps
    with io.BytesIO() as f:
        df.to_parquet(f)
        storage_client.bucket(Bucket).blob(Key).upload_from_file(f)


def s3_write_excel(df, Bucket, Key):
    with io.BytesIO() as f:
        df.to_excel(f)
        storage_client.bucket(Bucket).blob(Key).upload_from_file(f)


def s3_write_csv(df, Bucket, Key):
    with io.StringIO() as f:
        df.to_csv(f)
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
def get_corridors(bucket, key, keep_signalids_as_strings = False):
    corridors = s3_read_parquet(Bucket=bucket, Key=key)
    corridors = (corridors[~corridors.SignalID.isna()]
        .drop(['Description'], axis=1))

    if not keep_signalids_as_strings:
        corridors.SignalID = corridors.SignalID.astype('int')

    return corridors


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

    bd = s3_read_parquet(Bucket=bucket, Key=f'{key_prefix}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet')
    bd.SignalID = bd.SignalID.astype('int64')
    bd.Detector = bd.Detector.astype('int64')

    dc_prefix = f'{key_prefix}/atspm_det_config_good/date={date_str}'
    dc_keys = s3_list_objects(bucket, dc_prefix)

    dc = pd.concat(list(map(lambda k: read_det_config(bucket, k), dc_keys)))

    df = pd.merge(dc, bd, how='outer', on=['SignalID','Detector']).fillna(value={'Good_Day': 1})
    df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])

    return df
