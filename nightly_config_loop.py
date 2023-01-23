import sys
from config import get_date_from_string
from nightly_config import *

if __name__ == '__main__':

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = 'yesterday'
        end_date = datetime.today().date().strftime('%F')

    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)
    
    engine = get_atspm_engine(cred)

    # Code to go back and calculate past days
    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in reversed(dates):
        print(date_)
        nightly_config(engine, date_, conf)
