from datetime import datetime
from datetime import timedelta
import calendar
import sys
import os
import pytz 

if len(sys.argv)<5:
    print "usage %s af year month day" % (sys.argv[0])
    sys.exit()

af = int(sys.argv[1])

ye = int(sys.argv[2])
mo = int(sys.argv[3])
da = int(sys.argv[4])

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
tmp_dir = "/ssd/ashash/tmp_{}/".format(dateStart)

cmd = 'python2 src/ashash.py -a %s -s %s -w 900 -p 1 -o %s %s %s' % (
        af, spatialResolution, tmp_dir, dateStart, dateEnd)

print dateStart
rmcmd = """ psql -U romain -d ihr -c \
        "DELETE from ihr_hegemony where timebin>='%s' and timebin<='%s'" """ \
        % (dateStart, dateEnd)
print(rmcmd)
os.system(rmcmd)
rmcmd = """ psql -U romain -d ihr -c \
        "DELETE from ihr_hegemonycone where timebin>='%s' and timebin<='%s'" """\
        % (dateStart, dateEnd)
print(rmcmd)
os.system(rmcmd)
print(cmd)
os.system(cmd)
# archive the sqlite db
os.system("mv %s/*  resultsv%s/ihr/" % (tmp_dir, af))







# cmd = 'python2 src/ashash.py -a %s -s %s -w 900 -p 1 -o /ssd/ashash/resultsv%s/ihr/ %s %s' % (af, spatialResolution, af, dateStart, dateEnd)

# print(cmd)
# if not os.path.exists("resultsv%s/ihr/log_%s:00.txt" % (af,dateStart.replace("T"," "))):
    # os.system(cmd)
    # # archive the sqlite db
    # os.system("mv /ssd/ashash/resultsv%s/ihr/*  resultsv%s/ihr/" % (af, af))
# else:
    # print("skipping this one")
