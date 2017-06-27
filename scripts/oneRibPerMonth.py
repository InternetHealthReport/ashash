from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 

years = range(2001, 2018)
months = range(1,13)
days = [15]

for ye in years:
    for mo in months:
        for da in days:
            date = datetime(ye, mo, da, tzinfo = pytz.utc)
            start = date - timedelta(hours=2)
            end = date + timedelta(hours=2)

            tss = calendar.timegm(start.timetuple())
            tse = calendar.timegm(end.timetuple())

            print date
            print('python2 src/ashash.py -g -w 900 "@bgpstream:%s,%s" "None"' % (tss, tse))
            os.system('python2 src/ashash.py -g -w 900 "@bgpstream:%s,%s" "None"' % (tss, tse))
