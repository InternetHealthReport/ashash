import threading
import logging
from scipy import stats
from collections import defaultdict
from multiprocessing import Pool
import itertools

def asHegemonyMetric( param ): 
    (scope, counter), peersPerASN, alpha = param

    if scope.startswith("{"):
        #TODO handle set origins
        return None

    # # logging.debug("(AS hegemony) computing hegemony for graph %s" % asn)
    asHege = defaultdict(float)
    asHege_backup = defaultdict(float) #b
    # peersTotalCount = {p:float(counter["total"][p]) for p in peers if counter["total"][p]>0}
    peerASNTotalCount = {pasn:float(sum([counter["total"][p] for p in peers ])) for pasn, peers in peersPerASN.iteritems() }

    for asn in counter["asn"].iterkeys():
        # Don't do that: (for very distributed asn we want to report at least
        # the origin AS
        # if asn==scope:
            # continue

        # Compute betweenness centrality for each peer ASN 
        allScores = [sum([counter["asn"][asn][p] for p in peers])/peerASNTotalCount[pasn] if peerASNTotalCount[pasn] > 0 else 0 for pasn, peers in peersPerASN.iteritems() ]
        allScores_backup = [sum([counter["asn_backup"][asn][p] for p in peers])/peerASNTotalCount[pasn] if peerASNTotalCount[pasn] > 0 else 0 for pasn, peers in peersPerASN.iteritems()] #b
        
        # Adaptively filter low/high betweenness centrality scores
        hege = float(stats.trim_mean(allScores, alpha))
        hege_backup = float(stats.trim_mean(allScores_backup, alpha)) - hege #b

        # # Ignore ASN with hegemony = 0
        # This is useful for having a smaller db file, so it should be done
        # there

        asHege[asn] = hege
        asHege_backup[asn] = hege_backup #b

    return scope, asHege, asHege_backup #b


class asHegemony(threading.Thread):
    def __init__(self, countQueue, hegemonyQueue, alpha=0.1, saverQueue=None):
        threading.Thread.__init__(self)

        self.countQueue = countQueue
        self.hegemonyQueue = hegemonyQueue
        self.alpha = alpha
        self.daemon = True
        self.saverQueue = saverQueue
        self.workers = Pool(10)


    def run(self):
        while True:
            logging.debug("(AS hegemony) waiting for data")
            prevts = -1
            ts, peersPerASN, counts = self.countQueue.get()
            if not self.saverQueue is None:
                self.saverQueue.put("BEGIN TRANSACTION;")
            origAShege = []

            # AS hegemony for graph bound to originating AS
            logging.debug("(AS hegemony) making local graphs hegemony")
            params = itertools.izip(counts["origas"].iteritems(), itertools.repeat(peersPerASN), itertools.repeat(self.alpha) )
            logging.debug("(AS hegemony) sending tasks to workers")
            for hege in self.workers.imap_unordered(asHegemonyMetric, params, 1000):
                if prevts != ts:
                    prevts = ts
                    logging.debug("(AS hegemony) now it is time to work workers!")

                if hege is None or hege[0].startswith("{"):
                    continue

                # logging.debug("(AS hegemony) send hegemony results %s" % hege[0])
                self.hegemonyQueue.put( (ts, hege[0], hege[1]) )
                if not self.saverQueue is None:
                    self.saverQueue.put( ("hegemony", (ts, hege[0], hege[1])) )
                    self.saverQueue.put( ("hegemony_backup", (ts, hege[0], hege[2]))) #b

            # AS hegemony for the global graph
            logging.debug("(AS hegemony) making global graph hegemony")
            _, asHege, asHege_backup = asHegemonyMetric( (("all", counts["all"]), peersPerASN, self.alpha) ) #b
            self.hegemonyQueue.put( (ts, "all", asHege) )
            if not self.saverQueue is None:
                self.saverQueue.put( ("hegemony", (ts, 0, asHege)) )
                self.saverQueue.put( ("hegemony_backup", (ts, 0, asHege_backup))) #b

            if not self.saverQueue is None:
                self.saverQueue.put("COMMIT;")
            #let the pathCounter run again
            self.countQueue.task_done()


# def betweeness(counter, peers ):
    # asList = set(counter["asn"].iterkeys())
    # totalPaths = float(np.sum(counter["total"].values))

    # # Compute betweenness centrality 
    # asAggProb = {asn: np.sum(counter["asn"][asn].values())/totalPaths for asn in asList}

    # # logging.info("%s peers, %s ASN, %s addresses per peers, " % (len(peers), 
            # # len(asList), np.mean(counter["total"].values())))

    # # if not outFile is None:
        # # outFile.write("%s | %s | %s | %s | " % (nbPeers, len(allAsCount), len(asProb), np.mean(totalCountList)))

    # return asAggProb
