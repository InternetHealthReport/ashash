from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 

years = range(2013, 2014)
months = range(1,13)
days = [15]

af = 4

if af == 4:
    outputDirectory = "resultsv4/"
    spatialResolution = 1
elif af == 6:
    outputDirectory = "resultsv6/"
    spatialResolution = 0

print "IPv%s, spatial resolution=%s" % (af, spatialResolution)

for ye in years:
    for mo in months:
        for da in days:
            date = datetime(ye, mo, da, tzinfo = pytz.utc)
            start = date - timedelta(hours=2)
            end = date + timedelta(hours=2)

            tss = calendar.timegm(start.timetuple())
            tse = calendar.timegm(end.timetuple())

            print date
            print('python2 src/ashash.py -a %s -s %s -g -w 900 -o %s "@bgpstream:%s,%s" "None"' % (af, spatialResolution, outputDirectory, tss, tse ))
            if not os.path.exists("%sasgraph_@bgpstream:%s,%s.txt" % (tss,tse, outputDirectory)):
                os.system('python2 src/ashash.py -a %s -s %s -g -w 900 -o %s "@bgpstream:%s,%s" "None"' % (af, spatialResolution, outputDirectory, tss, tse))
            else:
                print("skipping this one")
