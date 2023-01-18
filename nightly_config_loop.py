from nightly_config import *

if __name__ == '__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)
    
    engine = get_atspm_engine(cred)

    # Code to go back and calculate past days
    dates = pd.date_range('2022-09-29', '2022-12-31', freq='1D')

    for date_ in reversed(dates):
        print(date_)
        nightly_config(engine, date_, conf)
