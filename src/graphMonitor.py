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

class graphMonitor(threading.Thread):
    def __init__(self, countQueue, N, M, ):
        threading.Thread.__init__ (self)

    def run(self):

    def hashfunc(self, x):
        return int(hashlib.sha512(x).hexdigest(), 16)


    def sketchesSimhash(self, sketches):

        hashes = {}
        for sketch, asProb in sketches.iteritems():
            hashes[sketch] = simhash.Simhash(asProb, f=64)

        return hashes


    def sketching(self, asAggProb, pool, N, M):
        seeds = [2**i for i in range(1,N+1)]
        sketches = defaultdict(lambda : defaultdict(dict)) 
        for seed in seeds:
            for asn, prob in asAggProb.iteritems():
                sketches[seed][mmh3.hash128(asn,seed=seed)%M][asn] = prob

        # compute the simhash for each hash function
        hashes= pool.map(sketchesSimhash, sketches.itervalues())

        return dict(zip(sketches.keys(), hashes)), sketches


    def computeSimhash(self, rtree, pool, N, M, spatial, outFile=None, filterAS=None):
        # get AS centrality
        asAggProb, asProb, _ = computeCentrality(rtree.search_exact("0.0.0.0/0").data, spatial, outFile, filterAS)
        # sketching
        res = sketching(asAggProb, pool, N, M)
        return res, asProb 


    def compareSimhash(self, prevHash, curHash, prevSketches, currSketches, minVotes,  distThresh=3):
        cumDistance = 0
        nbAnomalousSketches = 0
        votes = defaultdict(int)
        diff = defaultdict(int)
        for seed, sketchSet in prevHash.iteritems():
            for m, prevHash in sketchSet.iteritems():
                distance = prevHash.distance(currHash[seed][m]) 
                cumDistance += distance
                if distance > distThresh:
                    nbAnomalousSketches+=1
                    for asn, currProb in currSketches[seed][m].iteritems():
                        votes[asn]+=1
                        prevProb = 0.0
                        if asn in prevSketches[seed][m]:
                            prevProb = prevSketches[seed][m][asn]
                        diff[asn] = currProb-prevProb

        anomalousAsn = [(asn, count, diff[asn]) for asn, count in votes.iteritems() if count >= minVotes and diff[asn]]

        return anomalousAsn, nbAnomalousSketches, cumDistance




    def getPrefixPerCountry(self, cc):
            geoinfo = "http://geoinfo.bgpmon.io/201601/prefixes_announced_from_country/%s" % cc
            resp = requests.get(url=geoinfo)
            geoinfodata = json.loads(resp.text)
            prefixes = set([x["BGPPrefix"] for x in geoinfodata])

            return prefixes

