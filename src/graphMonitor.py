import logging
import numpy as np
import hashlib
import cPickle as pickle
from multiprocessing import Pool
import mmh3
import requests
import json
import itertools
from  more_itertools import unique_justseen
import threading
from collections import defaultdict
import simhash


def sketchesSimhash(sketches):

    hashes = {}
    for sketch, asProb in sketches.iteritems():
        hashes[sketch] = simhash.Simhash(asProb, f=64)

    return hashes



class graphMonitor(threading.Thread):
    def __init__(self, hegemonyQueue, N, M, distThresh=3, minVotes=0.5, nbProc=12 ):
        threading.Thread.__init__(self)

        self.hegemonyQueue = hegemonyQueue
        self.N = N
        self.M = M
        self.distThresh = distThresh
        self.minVotes = minVotes
        self.nbProc = nbProc
        self.seeds = [2**i for i in range(1,self.N+1)]

        self.ts = None
        self.scope = None
        self.hegemony = None
        self.previousResults = None
        self.daemon = True


    def run(self):
        # self.pool = Pool(self.nbProc)
        while True:
            logging.debug("(graphMonitor) Waiting for data")
            self.ts, self.scope, self.hegemony = self.hegemonyQueue.get() 
            logging.debug("(graphMonitor) before sketching")
            res = self.sketching()

            logging.debug("(graphMonitor) Sketching done")

            if not self.previousResults is None:
                print self.compareSimhash(res)

            self.previousResults = res
            
            logging.debug("(graphMonitor) Comparison done")
            self.hegemonyQueue.task_done()


    def sketching(self):
        sketches = defaultdict(lambda : defaultdict(dict)) 
        for seed in self.seeds:
            for asn, prob in self.hegemony.iteritems():
                sketches[seed][mmh3.hash128(asn,seed=seed)%self.M][asn] = prob

        # compute the simhash for each hash function
        # hashes= self.pool.map(sketchesSimhash, sketches.itervalues())
        hashes= map(sketchesSimhash, sketches.itervalues())

        return dict(zip(sketches.keys(), hashes)), sketches


    def compareSimhash(self, results):
        prevHash, prevSketches = self.previousResults
        currHash, currSketches = results
        cumDistance = 0
        nbAnomalousSketches = 0
        votes = defaultdict(int)
        diff = defaultdict(int)
        for seed, sketchSet in prevHash.iteritems():
            for m, prevHash in sketchSet.iteritems():
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

        anomalousAsn = [(asn, count, diff[asn]) for asn, count in votes.iteritems() if count >= self.minVotes and diff[asn]]

        return anomalousAsn, nbAnomalousSketches, cumDistance


    # def hashfunc(self, x):
        # return int(hashlib.sha512(x).hexdigest(), 16)

    # def getPrefixPerCountry(self, cc):
            # geoinfo = "http://geoinfo.bgpmon.io/201601/prefixes_announced_from_country/%s" % cc
            # resp = requests.get(url=geoinfo)
            # geoinfodata = json.loads(resp.text)
            # prefixes = set([x["BGPPrefix"] for x in geoinfodata])

            # return prefixes

