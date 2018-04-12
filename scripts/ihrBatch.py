from datetime import datetime
from datetime import timedelta
import calendar
import sys
import os
import pytz 

if len(sys.argv)<2:
    print "usage %s af" % (sys.argv[0])
    sys.exit()

years = [2018]
months = [2] #range(1,13)
days = range(11,20)

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
            dateStart = "%s-%s-%sT00:00" % (ye,mo,da) 
            dateEnd = "%s-%s-%sT23:59" % (ye,mo,da) 
            cmd = 'python2 src/ashash.py -a %s -s %s -w 900 -p -o resultsv%s/ihr/ %s %s' % (af, spatialResolution, af, dateStart, dateEnd)

            print(cmd)
            if not os.path.exists("resultsv%s/ihr/log_%s:00.txt" % (af,dateStart.replace("T"," "))):
                os.system(cmd)
            else:
                print("skipping this one")
