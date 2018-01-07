import argparse
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
parser.add_argument("-a","--af", help="address family (4, 6)", type=int, default=4)
parser.add_argument("alpha", help="Alpha values for AS hegemony", type=float, default=0.1)
parser.add_argument("-c","--collector", help="BGP collectors. e.g. route-views.linx rrc00", nargs="+", type=str, default=[ "route-views.linx", "route-views2", "rrc00", "rrc10"])
parser.add_argument("-N", help="number of hash functions for sketching", type=int, default=0) #16
parser.add_argument("-M", help="number of sketches per hash function", type=int, default=128)
parser.add_argument("-d","--distThresh", help="simhash distance threshold", type=int, default=3)
parser.add_argument("-f","--filter", help="filter per origin AS (Deprecated, will be removed soon)", type=str, default=None)
parser.add_argument("-g","--asGraph", help="dump the global AS graph", action="store_true")
parser.add_argument("-p","--postgre", help="send results to postgreSQL", action="store_true")
parser.add_argument("-r","--minVoteRatio", help="Minimum ratio of sketches to detect anomalies (should be between 0 and 1)", type=float, default=0.5)
parser.add_argument("-s", "--spatial", help="spatial resolution (0 for prefix, 1 for address)", type=int, default=1)
parser.add_argument("-w", "--window", help="Time window: time resolution in seconds", type=int, default=900)
parser.add_argument("-o", "--output", help="output directory", default="results/")
parser.add_argument("starttime", help="UTC start time, e.g. 2017-10-17T00:00 (should correspond to a date and time when RIB files are available)",  type=valid_date)
parser.add_argument("endtime", help="UTC end time", type=valid_date)
args = parser.parse_args()

try:
    os.makedirs(os.path.dirname(args.output))
except OSError as exc: # Guard against race condition
    if exc.errno != errno.EEXIST:
        raise

FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(format=FORMAT, filename=args.output+'log_%s.log' % args.starttime, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Started: %s" % sys.argv)
logging.info("Arguments: %s" % args)

# Initialisation
ribQueue = None
countQueue = Queue.Queue(10)
hegemonyQueue = Queue.Queue(60000)
saverQueue = mpQueue(10000)
announceQueue = None
nbGM = args.N/2 
pipeGM = []
for i in range(nbGM):
    pipeGM.append(mpPipe(False))


# Analysis Modules
ag = None
if args.asGraph:
    ribQueue = Queue.Queue(5000)
    ag = asGraph.asGraph(ribQueue)

gm = []
pm = None
if nbGM:
    announceQueue = Queue.Queue(5000)
    hegemonyQueuePM = Queue.Queue(60000)
    for i in range(nbGM):
        gm.append( Process(target=graphMonitor.graphMonitor, args=(pipeGM[i][0], args.N, args.M, args.distThresh, args.minVoteRatio, saverQueue), name="GM%s" % i ))
    pm = pathMonitor.pathMonitor(hegemonyQueuePM, announceQueue, saverQueue=saverQueue)

outlierDetection = False
if outlierDetection:
    pipeOD = mpPipe(False)
    od = Process(target=outlierDetection.outlierDetection, args=(pipeOD[0], 3.0, 5), name="OD")

pc = pathCounter.pathCounter(args.starttime, args.endtime, announceQueue, countQueue,
        ribQueue, spatialResolution=args.spatial, af=args.af, 
        asnFilter=args.filter, timeWindow=args.window, collectors=args.collector )
ash = asHegemony.asHegemony(countQueue, hegemonyQueue, alpha=args.alpha, saverQueue=saverQueue)

saverQueuePostgre = None
if args.postgre:
    logging.info("Will push results to Postgresql")
    import saverPostgresql
    saverQueuePostgre = mpQueue(10000)
    sp = Process(target=saverPostgresql.saverPostgresql, args=(args.starttime, args.af, saverQueuePostgre), name="saverPostgresql")
    sp.start()

sqldb = args.output+"results_%s.sql" % args.starttime
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
                ag.saveGraph(args.output+"asgraph_%s.txt" % args.starttime)
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
