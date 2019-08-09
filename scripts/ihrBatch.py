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
days = [15] # [2,3,5,6,7,9,10,30,31] # 1,4,8,11,15,18,22,25

start = True

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
            #start analysis from this date:
            # if ye == 2016 and mo == 1 and da == 31:
                # start=True

            if not start:
                continue

            cmd = "python2 scripts/ihrOneShot.py {} {} {} {}".format(af, ye, mo, da)
            print(cmd)
            os.system(cmd)




            # cmd = 'python2 src/ashash.py -a %s -s %s -w 900 -p 1 -o /ssd/ashash/oneRibPerMonth/ %s %s' % (af, spatialResolution, dateStart, dateEnd)

            # print dateStart
            # rmcmd = """ psql -U romain -d ihr -c "DELETE from ihr_hegemony where timebin>='%s' and timebin<='%s'" """ % (dateStart, dateEnd)
            # print(rmcmd)
            # os.system(rmcmd)
            # rmcmd = """ psql -U romain -d ihr -c "DELETE from ihr_hegemonycone where timebin>='%s' and timebin<='%s'" """ % (dateStart, dateEnd)
            # print(rmcmd)
            # os.system(rmcmd)
            # print(cmd)
            # os.system(cmd)
