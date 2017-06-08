import logging
from multiprocessing import Pool
import mmh3
# import requests
import json
import itertools
from  more_itertools import unique_justseen
from collections import defaultdict
import simhash


def sketchesSimhash(sketches):

    hashes = {}
    for sketch, asProb in sketches.iteritems():
        hashes[sketch] = simhash.Simhash(asProb, f=64)

    return hashes


class graphMonitor():
    def __init__(self, hegemonyPipe, N, M, distThresh=3, minVoteRatio=0.5, saverQueue=None, nbProc=4 ):
        # threading.Thread.__init__(self)

        self.hegemonyPipe = hegemonyPipe
        self.N = N
        self.M = M
        self.distThresh = distThresh
        self.minVotes = minVoteRatio*N
        self.nbProc = nbProc
        self.seeds = [2**i for i in range(1,self.N+1)]

        self.ts = None
        self.scope = None
        self.hegemony = None
        self.previousResults = dict()
        # self.hashCache = defaultdict(dict)
        self.workers = Pool(nbProc)
        self.saverQueue = saverQueue

        logging.debug(" New Graph Monitor")
        self.run()


    def hash(self, asn, seed):
        if self.scope == "all":
            return mmh3.hash128(asn,seed=seed)%self.M
        else:
            return mmh3.hash128(asn,seed=seed)%(self.M/10)
        # try:
            # return self.hashCache[asn][seed]
        # except KeyError:
            # h = mmh3.hash128(asn,seed=seed)%self.M
            # self.hashCache[asn][seed] = h
            # return h 


    def run(self):
        while True:
            # logging.debug("Waiting for data")
            self.ts, self.scope, self.hegemony = self.hegemonyPipe.recv() 
            logging.debug("Before sketching (AS %s)" % self.scope)
            res = self.sketching()

            # logging.debug("Sketching done")

            if self.scope in self.previousResults:
                ano = "%s: %s" % (self.ts, self.compareSimhash(res))

            self.previousResults[self.scope] = res
            
            logging.debug("done (AS %s)" % self.scope)


    def sketching(self):
        sketches = defaultdict(lambda : defaultdict(dict)) 
        for seed in self.seeds:
            for asn, prob in self.hegemony.iteritems():
                sketches[seed][self.hash(asn,seed)][asn] = prob

        # compute the simhash for each hash function
        hashes= self.workers.map(sketchesSimhash, sketches.itervalues())
        # hashes= map(sketchesSimhash, sketches.itervalues())

        return dict(zip(sketches.keys(), hashes)), sketches


    def compareSimhash(self, results):
        prevHash, prevSketches = self.previousResults[self.scope]
        currHash, currSketches = results
        cumDistance = 0
        nbAnomalousSketches = 0
        votes = defaultdict(int)
        diff = defaultdict(int)
        for seed, sketchSet in prevHash.iteritems():
            for m, prevHash in sketchSet.iteritems():
                if not m in currHash[seed]:
                    continue

                distance = prevHash.distance(currHash[seed][m]) 
                cumDistance += distance
                if distance > self.distThresh:
                    nbAnomalousSketches+=1
                    for asn, currProb in currSketches[seed][m].iteritems():
                        votes[asn]+=1
                        prevProb = 0.0
                        if asn in prevSketches[seed][m]:
                            prevProb = prevSketches[seed][m][asn]
                        diff[asn] = currProb-prevProb

        anomalies = [(asn, count, diff[asn]) for asn, count in votes.iteritems() if count >= self.minVotes and diff[asn]]

        if anomalies:
            for ano in anomalies:
                self.saverQueue.put( ("graphchange", [self.ts, self.scope, ano[0], ano[1], ano[2]]) )

        # return anomalousAsn, nbAnomalousSketches, cumDistance


    # def hashfunc(self, x):
        # return int(hashlib.sha512(x).hexdigest(), 16)

    # def getPrefixPerCountry(self, cc):
            # geoinfo = "http://geoinfo.bgpmon.io/201601/prefixes_announced_from_country/%s" % cc
            # resp = requests.get(url=geoinfo)
            # geoinfodata = json.loads(resp.text)
            # prefixes = set([x["BGPPrefix"] for x in geoinfodata])

            # return prefixes

