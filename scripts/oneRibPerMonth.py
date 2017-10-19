from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 

years = range(2014, 2018)
months = range(1,13)
days = [15]

af = 6

if af == 4:
    spatialResolution = 1
elif af == 6:
    spatialResolution = 0

print "IPv%s, spatial resolution=%s" % (af, spatialResolution)

for ye in years:
    for mo in months:
        for da in days:
            date = "%s-%s-%sT00:00" % (ye,mo,da) 

            print date
            print('python2 src/ashash.py -a %s -s %s -g -w 900 -o @psql/ %s %s' % (af, spatialResolution, date, date))
            if not os.path.exists("@psql/asgraph_%s.txt" % (date)):
                os.system('python2 src/ashash.py -a %s -s %s -g -w 900 -o @psql/ %s %s' % (af, spatialResolution, date, date))
            else:
                print("skipping this one")
