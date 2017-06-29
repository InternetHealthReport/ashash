from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 

years = range(2002, 2016)
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
            if not os.path.exists("results/asgraph_@bgpstream:%s,%s.txt" % (tss,tse)):
                os.system('python2 src/ashash.py -g -w 900 "@bgpstream:%s,%s" "None"' % (tss, tse))
            else:
                print("skipping this one")
