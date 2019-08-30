from datetime import datetime
from datetime import timedelta
import calendar
import sys
import os
import pytz 

if len(sys.argv)<2:
    print "usage %s af" % (sys.argv[0])
    sys.exit()

af = int(sys.argv[1])

today = datetime.utcnow()
ye = today.year
mo = today.month
da = today.day 

if af == 4:
    spatialResolution = 1
elif af == 6:
    spatialResolution = 0
else:
    print "Error: af should be 4 or 6"
    sys.exit()

print "IPv%s, spatial resolution=%s" % (af, spatialResolution)

dateStart = "%s-%s-%sT00:00" % (ye,mo,da) 
dateEnd = "%s-%s-%sT23:59" % (ye,mo,da) 
cmd = 'python2 src/ashash.py -a %s -s %s -w 900 -s 1 -sK 1 -k 1 -o ~/log/ihr-kafka-ashash/resultsv%s/ %s %s' % (af, spatialResolution, af, dateStart, dateEnd)

print(cmd)
os.system(cmd)
