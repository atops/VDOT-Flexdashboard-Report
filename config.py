
from datetime import datetime, timedelta
import re

def get_date_from_string(x):
    if type(x) == str:
        re_da = re.compile('\d+(?= *days ago)')
        if x == 'yesterday':
            x = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        elif re_da.search(x):
            d = int(re_da.search(x).group())
            x = (datetime.today() - timedelta(days=d)).strftime('%Y-%m-%d')
    else:
        x = x.strftime('%Y-%m-%d')
    return x

