import sys
import yaml
import pandas as pd
import posixpath
from config import get_date_from_string
import gcsio
from pull_atspm_data import get_atspm_engine 


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


def main(start_date, end_date, conf, engine):

    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in dates:

        date_str = date_.strftime('%F')
        print(date_str)

        query = f"""SELECT * FROM Controller_Event_Log
                    WHERE Timestamp >= '{date_str}' 
                    AND Timestamp < '{(date_ + pd.Timedelta(1, unit='D')).strftime('%F')}' 
                    AND EventCode = 150 
                    ORDER BY SignalID, Timestamp"""
    
        with engine.connect() as conn:
            df = pd.read_sql_query(query, con=conn)
            
        tints = time_in_transition(df)
        tints = tints[['SignalID', 'tint_s']].groupby(['SignalID']).agg(['sum', 'std', 'count'])['tint_s'].fillna(0)
        tints['std'] = tints['std'].round(1)
        
        gcsio.s3_write_parquet(
            tints,
            Bucket=conf['bucket'],
            Key=posixpath.join(conf['key_prefix'], f'mark/time_in_transition/date={date_str}/tint_{date_str}.parquet'))
        print(tints.head())
    


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)
    
    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)
    
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)
    
    engine = get_atspm_engine(cred)

    main(start_date, end_date, conf, engine)
