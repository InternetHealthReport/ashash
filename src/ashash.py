import sys
from subprocess import Popen, PIPE
import glob
import radix
from collections import defaultdict
import numpy as np
import simhash
import matplotlib.pylab as plt
import hashlib
import cPickle as pickle

def readrib(files):
    
    rtreedict = {}
    p0 = Popen(["bzcat"]+files, stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)

    for line in p1.stdout: 
        res = line.split('|',15)
        zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res
        
        if zPfx == "0.0.0.0/0":
            continue

        if rtreedict.has_key(zOrig) is False:
            rtreedict[zOrig] = radix.Radix()

        root = rtreedict[zOrig].search_exact("0.0.0.0/0")
        node = rtreedict[zOrig].add(zPfx)
        node.data["path"] = set(sPath.split(" "))
        root.data["nbPrefix"] += 1
        for asn in node.data["path"]:
            root.data["asCount"][asn] += 1
    
    return rtreedict


def readupdates(filename, rtreedict = {}):

    p0 = Popen(["bzcat", filename], stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)
    update_tag=""
    
    
    for line in p1.stdout:
        line=line.rstrip("\n")
        res = line.split('|',15)
        zOrig = res[3]
        
        if rtreedict.has_key(zOrig) is False:
            rtreedict[zOrig] = radix.Radix()

        root = rtreedict[zOrig].search_exact("0.0.0.0/0")
       
        if res[2] == "W":
            node = rtreedict[zOrig].search_exact(res[5])
            if not node is None:
                root.data["nbPrefix"] -= 1
                for asn in node.data["path"]:
                    root.data["asCount"][asn] -= 1
                rtreedict[zOrig].delete(res[5])
        
        else:
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res
            node = rtreedict[zOrig].search_exact(zPfx)
            path_list = sPath.split(' ')
            origin_as = path_list[-1]

            if node is None:
                node = rtreedict[zOrig].add(zPfx)
                node.data["path"] = set(sPath.split(" "))
                root.data["nbPrefix"] += 1
                for asn in node.data["path"]:
                    root.data["asCount"][asn] += 1

            else:
                for asn in node.data["path"]:
                    root.data["asCount"][asn] -= 1
                node.data["path"] = set(sPath.split(" "))
                for asn in node.data["path"]:
                    root.data["asCount"][asn] += 1

    return rtreedict


def hashfunc(x):
    return int(hashlib.sha512(x).hexdigest(), 16)


def computeSimhash(rtreedict):

    asProb = defaultdict(list)

    # For each RIB from our peers
    for rtree in rtreedict.values():
        root = rtree.search_exact("0.0.0.0/0")
        asCount = root.data["asCount"]
        nbPrefix = root.data["nbPrefix"]

        # asCount = defaultdict(int)

        # nbPrefix = 0
        # for node in rtree:
            # for asn in node.data["path"]:
                # asCount[asn] += 1

            # nbPrefix += 1

        for asn, count in asCount.iteritems():
            asProb[asn].append(count/float(nbPrefix))

    asAggProb = {}
    for asn, problist in asProb.iteritems():
        asAggProb[asn] = np.mean(problist)

    return simhash.Simhash(asAggProb f=512, hashfunc=hashfunc)


if __name__ == "__main__":
	
    arguments=sys.argv
    if len(arguments) < 3:
        print("usage: %s ribfiles*.bz2 updatefiles*.bz2" % arguments[0])
        sys.exit()
	
    arguments.pop(0)
		
    # read rib files
    rib_files = glob.glob(arguments[0])
	
    if len(rib_files)==0:
        print("Files not found!")
        sys.exit()

    rib_files.sort()
    rtreedict = readrib(rib_files)
    arguments.pop(0)

    hashHistory = {"date":[], "hash":[], "distance":[]}
    prevHash = None

    for arg in arguments:
		
        update_files = glob.glob(arg)
	
        if len(update_files)==0:
            sys.exit()
			
        update_files.sort()

        for fi in update_files:
            rtreedict = readupdates(fi, rtreedict)
            currHash = computeSimhash(rtreedict)

            if not prevHash is None:
                #TODO put the following outside of the loop 
                filename = fi.rpartition("/")[2]
                date = filename.split(".")
                date = "%s %s" % (date[1],date[2])

                distance = prevHash.distance(currHash) 

                hashHistory["date"].append(date)
                hashHistory["hash"].append(currHash)
                hashHistory["distance"].append(distance)

                print "%s: %s" % (date, distance)

            prevHash = currHash

    plt.figure()
    plt.plot(hashHistory["distance"])
    plt.tight_layout()
    plt.savefig("test.eps")

    pickle.dump(hashHistory, open("hashHistory.pickle", "w"))

