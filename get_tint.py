import pandas as pd
import dask.dataframe as dd

def time_in_transition(df):
    df = df[df.EventCode==150].sort_values(['SignalID', 'Timestamp'])
    df['Date'] = df.Timestamp.dt.date

    p0 = df[df.EventParam.isin([2,3,4])].sort_values('Timestamp').reset_index(drop=True)
    p1 = df[(df.EventParam==5) & (df.EventParam.shift(1)==1)].sort_values('Timestamp').reset_index(drop=True)
    p1['endTimestamp'] = p1['Timestamp']

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

    df = df.groupby(['SignalID', 'Date'])['TimeInTransition'].agg(['sum', 'count']).reset_index()
    df = df.rename(columns={'sum': 'tint_s', 'count': 'n'})

    return df


if __name__=='__main__':

    from distributed import Client

    client = Client()
    print(client.dashboard_link)

    df = dd.read_parquet('s3://vdot-spm2/atspm/date=2022-12-02/*', storage_options={'profile': 'KH_VDOT'})
    transitions = df[df.EventCode==150].compute().sort_values(['SignalID', 'Timestamp'])

    tints = time_in_transition(transitions)

    tints = tints[['SignalID', 'tint_s']].groupby(['SignalID']).agg(['sum', 'std', 'count'])['tint_s'].fillna(0)
    tints['std'] = tints['std'].round(1)
