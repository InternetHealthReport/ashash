import argparse
import ConfigParser
import os
import sys
import logging
import errno
from collections import deque
from datetime import datetime
import Queue
from multiprocessing import Pipe as mpPipe
from multiprocessing import JoinableQueue as mpQueue
from multiprocessing import Process

import pathCounter
import asHegemony
import pathMonitor
import graphMonitor
import saverSQLite
import asGraph
# import outlierDetection

def valid_date(s):
    try:
        return datetime.strptime(s+"UTC", "%Y-%m-%dT%H:%M%Z")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser()
parser.add_argument("-C","--config_file", help="Get all parameters from the specified config file", type=str, default="conf/default.ini")
parser.add_argument("-a","--af", help="address family (4, 6)", type=str)
parser.add_argument("--alpha", help="Alpha values for AS hegemony", type=str)
parser.add_argument("-c","--collector", help="BGP collectors. e.g. route-views.linx,rrc00", type=str)
parser.add_argument("-N", help="number of hash functions for sketching", type=str) #16
parser.add_argument("-M", help="number of sketches per hash function", type=str)
parser.add_argument("-d","--distThresh", help="simhash distance threshold", type=str)
parser.add_argument("-g","--asGraph", help="dump the global AS graph")
parser.add_argument("-p","--postgre", help="send results to postgreSQL")
parser.add_argument("-r","--minVoteRatio", help="Minimum ratio of sketches to detect anomalies (should be between 0 and 1)", type=str)
parser.add_argument("-s", "--spatial", help="spatial resolution (0 for prefix, 1 for address)", type=str)
parser.add_argument("-w", "--window", help="Time window: time resolution in seconds", type=str)
parser.add_argument("-o", "--output", help="output directory")
parser.add_argument("-f", "--inputFile", help="txt input file", type=str)
parser.add_argument("starttime", help="UTC start time, e.g. 2017-10-17T00:00 (should correspond to a date and time when RIB files are available)",  type=str)
parser.add_argument("endtime", help="UTC end time", type=str)
args = parser.parse_args()
argsDict = vars(args)

# variables not in the config file
inputFile = argsDict["inputFile"]

# hack to remove variables with default values of None
for (key,value) in argsDict.items():
    if value is None:
        del argsDict[key]

config_parser = ConfigParser.ConfigParser()
config_parser.read(args.config_file)

# Read config file
starttime = valid_date(config_parser.get("date","starttime",False,argsDict))
endtime = valid_date(config_parser.get("date","endtime",False,argsDict))
collector = [x.strip() for x in config_parser.get("peers","collector",False,argsDict).split(",")  if x.strip() != ""]
includedPeers = [x.strip() for x in config_parser.get("peers","include",False,argsDict).split(",")  if x.strip() != ""]
excludedPeers = [x.strip() for x in config_parser.get("peers","exclude",False,argsDict).split(",")  if x.strip() != ""]
onlyFullFeed = bool(int(config_parser.get("peers","onlyfullfeed")))
af = int(config_parser.get("origins","af",False,argsDict))
spatial = int(config_parser.get("origins","spatial",False,argsDict))
includedOrigins = [x.strip() for x in config_parser.get("origins","include",False,argsDict).split(",") if x.strip() != ""]  
excludedOrigins = [x.strip() for x in config_parser.get("origins","exclude",False,argsDict).split(",") if x.strip() != ""]
alpha = float(config_parser.get("hegemony","alpha",False,argsDict))
window = int(config_parser.get("hegemony","window",False,argsDict))
N = int(config_parser.get("detection","N",False,argsDict))
M = int(config_parser.get("detection","M",False,argsDict))
distThresh = int(config_parser.get("detection","distThresh",False,argsDict))
minVoteRatio = float(config_parser.get("detection","minVoteRatio",False,argsDict))
output = config_parser.get("output","output",False,argsDict)
writeASGraph = bool(int(config_parser.get("output","asGraph",False,argsDict)))
postgre = bool(int(config_parser.get("output","postgre",False,argsDict)))

try:
    os.makedirs(os.path.dirname(output))
except OSError as exc: # Guard against race condition
    if exc.errno != errno.EEXIST:
        raise

FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(format=FORMAT, filename=output+'log_%s.log' % starttime, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Started: %s" % sys.argv)
logging.info("Arguments: %s" % args)
for sec in config_parser.sections():
    logging.info("Config: [%s] %s" % (sec,config_parser.items(sec,False,argsDict)))

# Initialisation
ribQueue = None
countQueue = Queue.Queue(10)
hegemonyQueue = Queue.Queue(60000)
saverQueue = mpQueue(10000)
announceQueue = None
nbGM = N/2 
pipeGM = []
for i in range(nbGM):
    pipeGM.append(mpPipe(False))


# Analysis Modules
ag = None
if writeASGraph:
    ribQueue = Queue.Queue(5000)
    ag = asGraph.asGraph(ribQueue)

gm = []
pm = None
if nbGM:
    announceQueue = Queue.Queue(5000)
    hegemonyQueuePM = Queue.Queue(60000)
    for i in range(nbGM):
        gm.append( Process(target=graphMonitor.graphMonitor, args=(pipeGM[i][0], N, M, distThresh, minVoteRatio, saverQueue), name="GM%s" % i ))
    pm = pathMonitor.pathMonitor(hegemonyQueuePM, announceQueue, saverQueue=saverQueue)

outlierDetection = False
if outlierDetection:
    pipeOD = mpPipe(False)
    od = Process(target=outlierDetection.outlierDetection, args=(pipeOD[0], 3.0, 5), name="OD")

pc = pathCounter.pathCounter(starttime, endtime, announceQueue, countQueue,
        ribQueue, spatialResolution=spatial, af=af, 
         timeWindow=window, collectors=collector, excludedPeers=excludedPeers, 
         includedPeers=includedPeers, includedOrigins=includedOrigins, 
         excludedOrigins=excludedOrigins, onlyFullFeed=onlyFullFeed, txtFile=inputFile)
ash = asHegemony.asHegemony(countQueue, hegemonyQueue, alpha=alpha, saverQueue=saverQueue)

saverQueuePostgre = None
if postgre:
    logging.info("Will push results to Postgresql")
    import saverPostgresql
    saverQueuePostgre = mpQueue(10000)
    sp = Process(target=saverPostgresql.saverPostgresql, args=(starttime, af, saverQueuePostgre), name="saverPostgresql")
    sp.start()

sqldb = output+"results_%s.sql" % starttime
ss = Process(target=saverSQLite.saverSQLite, args=(sqldb, saverQueue, saverQueuePostgre), name="saverSQLite")
ss.start()
saverQueue.put(("experiment", [datetime.now(), str(sys.argv), str(args)]))

for g in gm: 
    g.start();

if outlierDetection:
    od.start()

if not ag is None:
    ag.start()
ash.start()
pc.start()

# Broadcast AS hegemony results to pathMonitor and graphMonitor
firstTime = True
while pc.isAlive() or (not hegemonyQueue.empty()) or (not countQueue.empty()):
    try:
        elem = hegemonyQueue.get(timeout=5)
        if firstTime:
            firstTime=False
            if pm is not None:
                pm.start() # start late to avoid looping unecessarily
            if ag is not None:
                logging.debug("writing graph")
                ag.saveGraph(output+"asgraph_%s.txt" % starttime)
        # logging.debug("(main) dispatching hegemony %s" % elem[1])
        if nbGM:
            if elem[1] == "all":
                pipeGM[0][1].send( elem )
            else:
                pipeGM[int(elem[1])%nbGM][1].send( elem )
                hegemonyQueuePM.put( elem )
        if outlierDetection:
            pipeOD[1].send( elem )

    except Queue.Empty:
        pass

logging.debug("Outside the main loop")
if announceQueue is not None:
    announceQueue.join()
countQueue.join()
logging.debug("Waiting for saver module")
saverQueue.join()

if saverQueuePostgre is not None:
    logging.debug("Waiting for PostgreSQL")
    saverQueuePostgre.join()
    sp.terminate()


logging.debug("Killing child processes")
ss.terminate()
for g in gm: 
    g.terminate()

logging.info("Good bye!")
