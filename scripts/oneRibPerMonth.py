from datetime import datetime
from datetime import timedelta
import calendar
import sys
import os
import pytz 

if len(sys.argv)<4:
    print "usage %s af startYear endYear" % (sys.argv[0])
    sys.exit()

years = range(int(sys.argv[2]), int(sys.argv[3])+1)
months = range(1,13)
days = [15]

af = int(sys.argv[1])

if af == 4:
    spatialResolution = 1
elif af == 6:
    spatialResolution = 0
else:
    print "Error: af should be 4 or 6"
    sys.exit()

print "IPv%s, spatial resolution=%s" % (af, spatialResolution)

for ye in years:
    for mo in months:
        for da in days:
            date = "%s-%s-%sT00:00" % (ye,mo,da) 

            print date
            print('python2 src/ashash.py -a %s -s %s -g -w 900 -o newResults%s/ %s %s' % (af, spatialResolution, af, date, date))
            if not os.path.exists("@psql/asgraph_%s.txt" % (date)):
                os.system('python2 src/ashash.py -a %s -s %s -g -w 900 -o newResults%s/ %s %s' % (af, spatialResolution, af, date, date))
            else:
                print("skipping this one")
