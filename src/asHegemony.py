import threading
import logging
from scipy import stats
from collections import defaultdict
import numpy as np

class asHegemony(threading.Thread):
    def __init__(self, countQueue, hegemonyQueue, alpha=0.1):
        threading.Thread.__init__(self)
        self.countQueue = countQueue
        self.hegemonyQueue = hegemonyQueue
        self.alpha = alpha
        self.daemon = True


    def run(self):
        while True:
            logging.debug("(AS hegemony) waiting for data")
            ts, peers, counts = self.countQueue.get()

            # AS hegemony for the global graph
            logging.debug("(AS hegemony) making global graph hegemony")
            hege, _ = self.asHegemony(peers, counts["all"])
            self.hegemonyQueue.put( (ts, "total", hege) )

            # AS hegemony for graph bound to originating AS
            logging.debug("(AS hegemony) making local graphs hegemony")
            # for asn, count in  counts["origas"].iteritems():
                # hege, _ = self.asHegemony(peers, count)
                # self.hegemonyQueue.put( (ts, asn, hege) )

            self.countQueue.task_done()


    def asHegemony(self, peers, counter):
        asProb = defaultdict(list)
        asList = set(counter["asn"].iterkeys())

        # Compute betweenness centrality for each peer 
        for asn in asList:
            asProb[asn] = [counter["asn"][asn][p]/float(counter["total"][p]) if counter["total"][p]>0 else 0.0 for p in peers]

        # Adaptively filter low/high betweenness centrality scores
        asAggProb = {asn:float(stats.trim_mean(problist, self.alpha))for asn, problist in asProb.iteritems()}

        return asAggProb, asProb


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
