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

parser = argparse.ArgumentParser()
parser.add_argument("-a","--af", help="address family (4, 6)", type=int, default=4)
parser.add_argument("-N", help="number of hash functions for sketching", type=int, default=16)
parser.add_argument("-M", help="number of sketches per hash function", type=int, default=128)
parser.add_argument("-d","--distThresh", help="simhash distance threshold", type=int, default=3)
parser.add_argument("-g","--asGraph", help="dump the global AS graph", action="store_true")
parser.add_argument("-r","--minVoteRatio", help="Minimum ratio of sketches to detect anomalies (should be between 0 and 1)", type=float, default=0.5)
parser.add_argument("-s", "--spatial", help="spatial resolution (0 for prefix, 1 for address)", type=int, default=1)
parser.add_argument("-w", "--window", help="Time window: time resolution in seconds (works only with  bgpstream)", type=int, default=900)
parser.add_argument("-o", "--output", help="output directory", default="results/")
parser.add_argument("ribs", help="RIBS files")
parser.add_argument("updates", help="UPDATES files", nargs="+")
args = parser.parse_args()

try:
    os.makedirs(os.path.dirname(args.output))
except OSError as exc: # Guard against race condition
    if exc.errno != errno.EEXIST:
        raise

FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(format=FORMAT, filename=args.output+'log_%s.log' % args.ribs, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Started: %s" % sys.argv)
logging.info("Arguments: %s" % args)

# Initialisation
ribQueue = None
if args.asGraph:
    ribQueue = Queue.Queue(5000)
announceQueue = Queue.Queue(5000)
countQueue = Queue.Queue(10)
hegemonyQueue = Queue.Queue(10000)
hegemonyQueuePM = Queue.Queue(10000)
saverQueue = mpQueue(10000)
nbGM = 6 
pipeGM = []
for i in range(nbGM):
    # recv, send = 
    pipeGM.append(mpPipe(False))



# Analysis Modules
gm = []
for i in range(nbGM):
    gm.append( Process(target=graphMonitor.graphMonitor, args=(pipeGM[i][0], args.N, args.M, args.distThresh, args.minVoteRatio, saverQueue), name="GM%s" % i ))

pc = pathCounter.pathCounter(args.ribs, args.updates, announceQueue, countQueue, ribQueue, spatialResolution=args.spatial, af=args.af, timeWindow=args.window )
pm = pathMonitor.pathMonitor(hegemonyQueuePM, announceQueue, saverQueue=saverQueue)
ash = asHegemony.asHegemony(countQueue, hegemonyQueue, saverQueue=saverQueue)
ag = None
if args.asGraph:
    ag = asGraph.asGraph(ribQueue)

sqldb = args.output+"results_%s.sql" % args.ribs
ss = Process(target=saverSQLite.saverSQLite, args=(sqldb, saverQueue), name="saverSQLite")
ss.start()
saverQueue.put(("experiment", [datetime.now(), str(sys.argv), str(args)]))

for g in gm: 
    g.start();
if not ag is None:
    ag.start()
pm.start()
ash.start()
pc.start()

# Broadcast AS hegemony results to pathMonitor and graphMonitor
firstTime = True
while pc.isAlive():
    elem = hegemonyQueue.get()
    if firstTime:
        firstTime=False
        logging.debug("writing graph")
        if not ag is None:
            ag.saveGraph(args.output+"asgraph_%s.txt" % args.ribs)
    # logging.debug("(main) dispatching hegemony %s" % elem[1])
    if elem[1] == "all":
        pipeGM[0][1].send( elem )
    else:
        pipeGM[int(elem[1])%nbGM][1].send( elem )
        hegemonyQueuePM.put( elem )

announceQueue.join()
countQueue.join()
saverQueue.join()

ss.terminate()
for g in gm: 
    g.terminate()

logging.info("Good bye!")
