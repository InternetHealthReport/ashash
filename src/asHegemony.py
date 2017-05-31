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
            origAShege = []

            # AS hegemony for the global graph
            logging.debug("(AS hegemony) making global graph hegemony")
            asHege = self.asHegemony(peers, counts["all"])
            self.hegemonyQueue.put( (ts, "all", asHege) )

            # AS hegemony for graph bound to originating AS
            logging.debug("(AS hegemony) making local graphs hegemony")
            for asn, count in  counts["origas"].iteritems():
                if asn.startswith("{"):
                    #TODO handle set origins
                    continue
                hege = self.asHegemony(peers, count)
                # origAShege.append((ts, asn, hege))
                self.hegemonyQueue.put((ts, asn, hege))

            #let the pathCounter run again
            self.countQueue.task_done()

            # #push results for graph analysis
            # for res in origAShege:
                # self.hegemonyQueue.put(res)



    def asHegemony(self, peers, counter):
        asHege = dict()
        asList = set(counter["asn"].keys())
        peersTotalCount = {p:float(counter["total"][p]) for p in peers if counter["total"][p]>0}

        # Compute betweenness centrality for each peer 
        for asn in asList:
            allScores = [counter["asn"][asn][p]/peersTotalCount[p] for p in peersTotalCount.keys()]
            
            # Adaptively filter low/high betweenness centrality scores
            asHege[asn] = float(stats.trim_mean(allScores, self.alpha))

        return asHege 


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
