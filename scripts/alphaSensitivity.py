from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 
import numpy as np

date = datetime(2017, 12, 15, tzinfo = pytz.utc)
dateStr = "%s-%s-%sT%02d:%02d" % (date.year, date.month, date.day, date.hour, date.minute)
alphaValues = np.arange(0.05,0.51, 0.1)
# alphaValues = np.arange(0,0.51, 0.1)
# alphaValues[-1] = 0.49
outputDirectory = "results/alphaSensitivity/"

for alpha in alphaValues:
    cmd = 'python2 src/ashash.py --alpha %s -o %s %s %s' % (alpha, outputDirectory, dateStr, dateStr )
    print(cmd)
    os.system(cmd)

