import sys
import yaml
import posixpath
import pandas as pd
import numpy as np
import dask.dataframe as dd
from distributed import Client
import pyarrow.dataset as ds
from pyarrow import fs
import boto3

from config import get_date_from_string
import s3io


def time_in_transition(df):
    df = df[df.EventCode==150].sort_values(['SignalID', 'Timestamp'])

    if df.empty:
        return pd.DataFrame()

    df['Date'] = df.Timestamp.dt.date

    # Start of Transition
    p0 = df[df.EventParam.isin([2,3,4])].sort_values('Timestamp').reset_index(drop=True)

    # End of Transition
    p1 = df[(df.EventParam==5) & (df.EventParam.shift(1)==1)].sort_values('Timestamp').reset_index(drop=True)
    p1['endTimestamp'] = p1['Timestamp']

    # Cycle Lengths
    pc = df[df.EventParam==5].sort_values(['SignalID', 'Timestamp'])
    pc['CycleLength'] = (pc.Timestamp - pc.groupby('SignalID').shift(1).Timestamp).dt.total_seconds()
    pc = pc.rename(columns={'Timestamp': 'endTimestamp'})[['SignalID', 'endTimestamp', 'CycleLength']]

    # Create Transition Periods from start/end transitions
    df = pd.merge_asof(
        left=p0,
        right=p1,
        on=['Timestamp'],
        by=['SignalID', 'EventCode', 'Date'],
        suffixes=('0', '1'),
        direction='forward'
    ).sort_values(['SignalID', 'Timestamp'])

    df = df[~df.EventParam1.isna()]
    df.EventParam1 = df.EventParam1.astype(int)
    df['TimeInTransition'] = (df.endTimestamp - df.Timestamp).dt.total_seconds()

    # Take the max duration if there a multiple matches, e.g., 5 3 6 3 5 1 5 should match the first 3 and the 1 and ignore the second 3
    idx = df.groupby(['SignalID', 'endTimestamp'])['TimeInTransition'].transform(max) == df['TimeInTransition']
    df = df[idx]

    # Append cycle length we're transitioning into, to get number of cycles of each transition
    df = pd.merge(
        left=df,
        right=pc,
        on=['SignalID', 'endTimestamp']
    )
    df['CyclesInTransition'] = np.ceil(df.TimeInTransition/df.CycleLength)
    df.loc[df.CyclesInTransition.isna(), 'CyclesInTransition'] = 1

    df['TimeInTransition'] = df.TimeInTransition/60 # convert seconds to minutes

    # Summarize and prepare for output
    df = df.groupby(['SignalID', 'Date'])[['TimeInTransition', 'CyclesInTransition']].agg(['min', 'median', 'max', 'count', 'sum'])
    df.columns = ['_'.join(col) for col in df.columns.to_flat_index()]
    df = df.rename(columns={
        'TimeInTransition_sum': 'tint',
        'TimeInTransition_count': 'Transitions'})
    df = df.drop(columns=['CyclesInTransition_count'])
    df = df.reset_index()

    # df = df.groupby(['SignalID', 'Date'])['TimeInTransition'].agg(['sum', 'count']).reset_index()
    # df = df.rename(columns={'sum': 'tint_s', 'count': 'n'})

    return df


if __name__=='__main__':

    client = Client()
    print(client.dashboard_link)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    session = boto3.session.Session(profile_name=conf['profile'])
    credentials = session.get_credentials()

    s3_filesystem = fs.S3FileSystem(
        secret_key=credentials.secret_key,
        access_key=credentials.access_key,
        region=session.region_name,
        session_token=credentials.token)

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)

    dates = pd.date_range(start_date, end_date, freq='1D')

    bucket = conf['bucket']
    key_prefix = conf['key_prefix'] or ''

    for date_ in dates:
        date_str = date_.strftime('%F')

        df = dd.read_parquet(posixpath.join('s3://', bucket, key_prefix, f'atspm/date={date_str}'), storage_options={'profile': conf['profile']})
        transitions = df[df.EventCode==150].compute().sort_values(['SignalID', 'Timestamp'])
        # df = ds.dataset(posixpath.join(bucket, key_prefix, f'atspm/date={date_str}'), filesystem=s3_filesystem)
        # transitions = df.to_table(filter=ds.field('EventCode') == 150).to_pandas().sort_values(['SignalID', 'Timestamp'])

        tints = time_in_transition(transitions)

        # tints = tints[['SignalID', 'tint_s']].groupby(['SignalID']).agg(['sum', 'std', 'count'])['tint_s'].fillna(0)
        # tints['std'] = tints['std'].round(1)

        s3io.s3_write_parquet(
            tints,
            Bucket=bucket,
            Key=posixpath.join(key_prefix, f'mark/time_in_transition/date={date_str}/tint_{date_str}.parquet'))

