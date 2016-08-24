import sys
import os
import errno
import argparse
from subprocess import Popen, PIPE
import glob
import radix
from collections import defaultdict
from datetime import datetime
import numpy as np
import simhash
import matplotlib.pylab as plt
import hashlib
import cPickle as pickle
from multiprocessing import Pool
import mmh3

def readrib(files, spatialResolution=1, af=4):
    
    rtree = radix.Radix() 
    root = rtree.add("0.0.0.0/0")

    p0 = Popen(["bzcat"]+files, stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)

    for line in p1.stdout: 
        res = line.split('|',16)
        zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res

        if af!=0:
            if af == 4 and ":" in zPfx:
                continue
            elif af == 6 and "." in zPfx:
                continue
        
        if zPfx == "0.0.0.0/0":
            continue

        if not zOrig in root.data:
            root.data[zOrig] = {"totalCount": 0, "asCount": defaultdict(int)}

        node = rtree.add(zPfx)
        node.data[zOrig] = {"path": set(sPath.split(" ")), "asCount": defaultdict(int)}

        count = 1
        if spatialResolution:
            pSize = int(zPfx.rpartition("/")[2])
            if "." in zPfx:
                count = 2**(32-pSize) 
            elif ":" in zPfx:
                count = 2**(128-pSize) 

        root.data[zOrig]["totalCount"] += count

        for asn in node.data[zOrig]["path"]:
            root.data[zOrig]["asCount"][asn] += count
    
    return rtree


def readupdates(filename, rtree, spatialResolution=1, af=4):

    p0 = Popen(["bzcat", filename], stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)
    
    root = rtree.search_exact("0.0.0.0/0")
    
    for line in p1.stdout:
        line=line.rstrip("\n")
        res = line.split('|',15)
        zOrig = res[3]
        zPfx  = res[5]

        if af != 0:
            if af == 4 and ":" in zPfx:
                continue
            elif af == 6 and "." in zPfx:
                continue

        if zPfx == "0.0.0.0/0":
            continue
        
        if not zOrig in root.data:
            root.data[zOrig] = {"totalCount": 0, "asCount": defaultdict(int)}

        count = 1
        if spatialResolution:
            pSize = int(zPfx.rpartition("/")[2])
            if "." in zPfx:
                count = 2**(32-pSize) 
            elif ":" in zPfx:
                count = 2**(128-pSize) 

        if res[2] == "W":
            node = rtree.search_exact(res[5])
            if not node is None and zOrig in node.data and len(node.data[zOrig]["path"]):

                root.data[zOrig]["totalCount"] -= count
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] -= count 
                node.data[zOrig]["path"] = []
        
        else:
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res

            if zPfx == "0.0.0.0/0":
                continue

            node = rtree.search_exact(zPfx)
            path_list = sPath.split(' ')

            if node is None or not zOrig in node.data:
                node = rtree.add(zPfx)
                node.data[zOrig] = {"path": set(sPath.split(" ")), "asCount": defaultdict(int)}
                root.data[zOrig]["totalCount"] += count
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] += count 

            else:
                if len(node.data[zOrig]["path"]):
                    for asn in node.data[zOrig]["path"]:
                        root.data[zOrig]["asCount"][asn] -= count
                else:
                    root.data[zOrig]["totalCount"] += count

                node.data[zOrig]["path"] = set(sPath.split(" "))
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] += count

    return rtree


def hashfunc(x):
    return int(hashlib.sha512(x).hexdigest(), 16)

def sketchesSimhash(sketches):

    hashes = {}
    for sketch, asProb in sketches.iteritems():
        hashes[sketch] = simhash.Simhash(asProb, f=64)

    return hashes

def sketchSet():
    return defaultdict(dict)

def sketching(asProb, pool, N=8, M=32):

    seeds = [2**i for i in range(N)]
    sketches = defaultdict(sketchSet) 
    for seed in seeds:
        for asn, prob in asProb.iteritems():
            sketches[seed][mmh3.hash128(asn,seed=seed)%M][asn] = prob

    # compute the simhash for each hash function
    hashes= pool.map(sketchesSimhash, sketches.itervalues())

    return dict(zip(sketches.keys(), hashes)), sketches


def computeSimhash(rtree, pool, N, M, outFile=None):

    root = rtree.search_exact("0.0.0.0/0")
    asProb = defaultdict(list)
    totalCountList = []
    # For each RIB from our peers
    for peer, count in root.data.iteritems():
        totalCount = count["totalCount"]
        if totalCount <= 400000:
            continue

        asCount = count["asCount"]
        totalCountList.append(totalCount)

        for asn, nbPath in asCount.iteritems():
            asProb[asn].append(nbPath/float(totalCount))

    if len(totalCountList) == 0:
        # There is no peers?
        return None, None

    asAggProb = {}
    for asn, problist in asProb.iteritems():
        mu = np.median(problist)
        #if asn == "4788" or asn == "200759" or asn == "65021":
            #print "%s: %s" % (asn, mu)
        asAggProb[asn] = mu

    sys.write("%s/%s peers, %s ASN, %s prefixes per peers, " % (len(totalCountList), 
            len(root.data), len(asProb), np.mean(totalCountList)))

    if not outFile is None:
        outFile.write("%s | %s | %s %s | " % (len(totalCountList), len(root.data), len(asProb), np.mean(totalCountList)))

    # sketching
    return sketching(asAggProb, pool, N, M)


def compareSimhash(prevHash, curHash, prevSketches, currSketches,  distThresh=6, minVotes=6):
    cumDistance = 0
    nbAnomalousSketches = 0
    votes = defaultdict(int)
    diff = defaultdict(int)
    for seed, sketchSet in prevHash.iteritems():
        for m, prevHash in sketchSet.iteritems():
            distance = prevHash.distance(currHash[seed][m]) 
            cumDistance += distance
            if distance >= distThresh:
                nbAnomalousSketches+=1
                for asn, currProb in currSketches[seed][m].iteritems():
                    votes[asn]+=1
                    prevProb = 0.0
                    if asn in prevSketches[seed][m]:
                        prevProb = prevSketches[seed][m][asn]
                    diff[asn] = currProb-prevProb

    anomalousAsn = [(asn, count, diff[asn]) for asn, count in votes.iteritems() if count >= minVotes]

    return anomalousAsn, nbAnomalousSketches, cumDistance


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--af", help="address family (4, 6, or 0 for both)", type=int, default=4)
    parser.add_argument("-N", help="number of hash functions for sketching", type=int, default=8)
    parser.add_argument("-M", help="number of sketches per hash function", type=int, default=16)
    parser.add_argument("-p", "--proc", help="number of processes", type=int)
    parser.add_argument("-s", "--spatial", help="spatial resolution (0 for prefix, 1 for address)", type=int, default=0)
    parser.add_argument("--plot", help="plot figures", action="store_true")
    parser.add_argument("ribs", help="RIBS files")
    parser.add_argument("updates", help="UPDATES files", nargs="+")
    parser.add_argument("output", help="output directory")
    args = parser.parse_args()

    if args.proc is None:
        args.proc = args.N

    try:
        os.makedirs(os.path.dirname(args.output))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

    p=Pool(args.proc)
		
    # read rib files
    rib_files = glob.glob(args.ribs)
	
    if len(rib_files)==0:
        sys.stderr.write("Files not found!\n")
        sys.exit()

    rib_files.sort()
    rtree = readrib(rib_files, args.spatial, args.af)

    hashHistory = {"date":[], "hash":[], "distance":[], "reportedASN":[]}
    outFile = open(args.output+"/results.txt","w")
    prevHash = None

    for updates in args.updates:
		
        update_files = glob.glob(updates)
	
        if len(update_files)==0:
            sys.exit()
			
        update_files.sort()

        for fi in update_files:
            filename = fi.rpartition("/")[2]
            date = filename.split(".")
            sys.stdout.write("^[[2K\r %s:%s" % (date[1], date[2]))
            outFile.write("%s:%s | " % (date[1], date[2]) )
            rtree = readupdates(fi, rtree, args.spatial, args.af)
            currHash, currSketches = computeSimhash(rtree, p, args.N, args.M)

            if not prevHash is None:
                if currHash is None:
                    anomalousAsn = []
                    nbAnoSketch =  np.nan
                    distance = np.nan
                else:
                    anomalousAsn, nbAnoSketch, distance = compareSimhash(prevHash, currHash, prevSketches, currSketches)
                    #TODO put the following outside of the loop 

                # distance = prevHash.distance(currHash) 
                if args.plot:
                    hashHistory["date"].append( datetime.strptime(date[1]+date[2], "%Y%m%d%H%M"))
                    hashHistory["distance"].append(distance)
                    hashHistory["reportedASN"].append(len(anomalousAsn))

                sys.stdout.write("%s anomalous sketches (dist=%s), " % (nbAnoSketch, distance))
                if len(nbAnomalousAsn):
                    sys.stdout.write("%s" % (anomalousAsn))

                outFile.write("%s | %s | %s \n" % (nbAnoSketch, distance, anomalousAsn) )
            
            if not currHash is None:
                prevHash = currHash
                prevSketches = currSketches

    if args.plot :
        # pickle.dump(hashHistory, open("distance.pickle","wb"))

        plt.figure(figsize=(10,4))
        plt.plot(hashHistory["date"],hashHistory["distance"])
        plt.xticks(rotation=70)
        plt.ylabel("simhash distance")
        plt.tight_layout()
        plt.savefig("distance.eps")

        plt.figure(figsize=(10,4))
        plt.plot(hashHistory["date"],hashHistory["reportedASN"])
        plt.xticks(rotation=70)
        plt.ylabel("Number of reported ASN")
        plt.tight_layout()
        plt.savefig("reportedASN.eps")

