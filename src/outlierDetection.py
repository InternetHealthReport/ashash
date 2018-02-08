import sys
import logging
from collections import defaultdict
import numpy as np
import pandas as pd
from multiprocessing import Process
from multiprocessing import Pipe as mpPipe
import apsw


class outlierDetection():
    def __init__(self, hegemonyPipe, threshold=50.0, historySize=2*4*24, epsilon=0.01, saverQueue=None ):
        # threading.Thread.__init__(self)

        self.hegemonyPipe = hegemonyPipe
        self.threshold = threshold
        self.epsilon = epsilon
        self.historySize = historySize
        self.history = {}

        self.ts = None
        self.saverQueue = saverQueue
        self.bootstrap = True

        logging.debug(" New outlier detector")
        self.run()


    def run(self):
        # TODO compute median mad when waiting for too long

        while True:
            # logging.debug("Waiting for data")
            ts, scope, hegemony = self.hegemonyPipe.recv() 
            if ts is None:
                logging.warn("Stopping outlier detector")
                return

            if ts != self.ts:
                logging.debug("time bin: %s" % ts)
                self.ts = ts

            # logging.debug("Sketching done")
            if scope not in self.history:
                #add a new scope
                self.addScope(ts, scope, hegemony)

            self.detect(ts, scope, hegemony) 
            self.update(ts, scope, hegemony)


    def addScope(self, ts, scope, hegemony):
        """ Add a new local graph (i.e. scope) to the data structure"""

        self.history[scope]={
                "nbSeen": 0,   # Number of time this scope has been seen in the data
                "lastSeen": ts, # Last time bin when this scope was seen
                "ready": False, # Are the normal references updated?
                "ref": pd.DataFrame(), # Reference values
                "data": pd.DataFrame(
                    columns=[str(k) for k in hegemony.keys()],
                    index=range(self.historySize)),
                                # Contain past hegemony values for each asn
                "currBin":-1,   # Index 
                }


    def computeReference(self, scope):
        """ Compute median and mad values. If new data have been added to the 
        structure this step is required for the detection."""

        hist = self.history[scope]
        hist["ref"]["median"] = hist["data"].median()
        hist["ref"]["mad"] = (np.abs(hist["data"] - hist["ref"]["median"])).median()

        # if MAD = 0 then range is function of epsilon
        nullmad = hist["ref"].index[hist["ref"]["mad"]<self.epsilon]
        hist["ref"]["mad"][nullmad] = self.epsilon #hist["ref"]["median"][nullmad]*self.epsilon

        hist["ref"]["upperBound"] = hist["ref"]["median"]+(self.threshold*hist["ref"]["mad"])
        hist["ref"]["lowerBound"] = hist["ref"]["median"]-(self.threshold*hist["ref"]["mad"]) 
        self.history[scope]["ready"] = True


    def detect(self, ts, scope, hegemony):
        
        hist = self.history[scope]
        if hist["nbSeen"] < self.historySize:
            return 

        if self.bootstrap:
            self.bootstrap=False
            logging.debug("Start detection, ts=%s" % ts)

        # Outlier detection
        if not hist["ready"]:
            # logging.debug("Compute ref, ts=%s" % ts)
            self.computeReference(scope)

        dftest = pd.DataFrame(hegemony.values(), index=[str(k) for k in hegemony.keys()], columns=["test"])
        detection = pd.merge(dftest, hist["ref"], how="outer", 
                            left_index=True, right_index=True)

        # Change Nan values to zeros
        detection["upperBound"] = detection["upperBound"].fillna(self.threshold*self.epsilon)
        detection = detection.fillna(0)

        # Compute the thresholds
        detection["high"] = detection["test"]>detection["upperBound"]
        detection["low"] = detection["test"]<detection["lowerBound"]

        # Abnormally high hegemony scores
        ano = detection.index[detection["high"]]
        for asn in ano:
            logging.debug("(scope %s, ts %s) median=%s, upper bound=%s" % (scope,ts,detection["median"][asn],detection["upperBound"][asn]))
            logging.debug("(scope %s, ts %s) AS%s above the ref: hege=%s" % (scope,ts, asn, detection["test"][asn])) 

        # Abnormally low hegemony scores
        ano = detection.index[detection["low"]]
        for asn in ano:
            logging.debug("(scope %s, ts %s) median=%s, lower bound=%s" % (scope,ts,detection["median"][asn],detection["lowerBound"][asn]))
            logging.debug("(scope %s, ts %s) AS%s below the ref: hege=%s" % (scope,ts,asn, detection["test"][asn])) 

        ## TODO: remove the following if we replace NaN per zeros
        # New transit ASN
        # ano = detection.index[detection["median"].isnull()]
        # for asn in ano:
            # logging.debug("(scope %s) AS%s is new" % (scope,asn)) 

        # Lost transit ASN
        # ano = detection.index[detection["test"].isnull()]
        # for asn in ano:
            # logging.debug("(scope %s) AS%s disappeared" % (scope,asn)) 

        #print detection

    def update(self, ts, scope, hegemony):
        # print "new update, ts=%s, scope=%s" % (ts, scope)

        hist = self.history[scope]
        hist["ready"] = False
        hist["nbSeen"] += 1
        hist["lastSeen"] = ts
        newBin = (hist["currBin"]+1)%self.historySize
        hist["currBin"] = newBin
        hist["data"].at[newBin, :] = 0
        for asn, hege in hegemony.iteritems():
            asn = str(asn)
            if asn not in hist["data"]:
                # Add new ASN to the history
                hist["data"][asn] = np.zeros(self.historySize)
            hist["data"].at[newBin, asn] = hege

        #print hist

if __name__ == "__main__":

    if len(sys.argv)<2:
        print("usage: %s results.sql [expid]" % sys.argv[0])
        sys.exit()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    db = sys.argv[1]
    expid=1
    if len(sys.argv)>2:
        expid = int(sys.argv[2])
    
    data = defaultdict(lambda: defaultdict(float))

    odPipe = mpPipe(False)
    od = Process(target=outlierDetection, args=(odPipe[0],), name="OD")
    od.start()
    
    # Get data from the sqlite db
    conn = apsw.Connection(db)
    cursor = conn.cursor()

    prevTs = None
    for scope, ts, asn, hege in cursor.execute("SELECT scope, ts, asn, hege FROM hegemony where expid=? ORDER BY ts", [expid]):
        if prevTs is None:
            prevTs = ts
        if ts!=prevTs:
            for scope, hegemony in data.iteritems():
                odPipe[1].send((prevTs, scope, hegemony))

            data = defaultdict(lambda: defaultdict(float))
            prevTs = ts

        data[scope][asn] = hege

    odPipe[1].send((None, None, None))
    logging.debug("Finished")
    od.join()

