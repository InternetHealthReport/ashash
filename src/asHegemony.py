import threading
import logging
from scipy import stats
from collections import defaultdict
from multiprocessing import Pool
import itertools

def asHegemonyMetric( param ): 
    (asn, counter), peers, alpha = param

    if asn.startswith("{"):
        #TODO handle set origins
        return None

    asHege = defaultdict(float)
    peersTotalCount = {p:float(counter["total"][p]) for p in peers if counter["total"][p]>0}

    # Compute betweenness centrality for each peer 
    for asn in counter["asn"].iterkeys():
        allScores = [counter["asn"][asn][p]/peersTotalCount[p] for p in peersTotalCount.iterkeys()]
        
        # Adaptively filter low/high betweenness centrality scores
        asHege[asn] = float(stats.trim_mean(allScores, alpha))

    return asn, asHege 


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
            ts, peers, counts = self.countQueue.get()
            if not self.saverQueue is None:
                self.saverQueue.put("BEGIN TRANSACTION;")
            origAShege = []

            # AS hegemony for graph bound to originating AS
            logging.debug("(AS hegemony) making local graphs hegemony")
            params = itertools.izip(counts["origas"].iteritems(), itertools.repeat(peers), itertools.repeat(self.alpha) )
            logging.debug("(AS hegemony) sending tasks to workers")
            for hege in self.workers.imap_unordered(asHegemonyMetric, params):
            # for asn, count in  counts["origas"].iteritems():
                # # logging.debug("(AS hegemony) computing graph for ASN %s" % asn)

                # asHege = self.asHegemony(peers, count)
                # origAShege.append((ts, asn, asHege))
                self.hegemonyQueue.put((ts, hege[0], hege[1]))
                if not self.saverQueue is None:
                    self.saverQueue.put( ("hegemony", (ts, hege[0], hege[1])) )

            # AS hegemony for the global graph
            logging.debug("(AS hegemony) making global graph hegemony")
            asHege = self.asHegemony(peers, counts["all"])
            self.hegemonyQueue.put( (ts, "all", asHege) )
            if not self.saverQueue is None:
                self.saverQueue.put( ("hegemony", (ts, 0, asHege)) )

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
