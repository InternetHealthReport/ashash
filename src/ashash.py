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
    
    rtree = radix.Radix() 
    root = rtree.add("0.0.0.0/0")

    p0 = Popen(["bzcat"]+files, stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)

    for line in p1.stdout: 
        res = line.split('|',16)
        zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res
        
        if zPfx == "0.0.0.0/0":
            continue

        if not zOrig in root.data:
            root.data[zOrig] = {"nbPrefix": 0, "asCount": defaultdict(int)}

        node = rtree.add(zPfx)
        node.data[zOrig]["path"] = set(sPath.split(" "))
        root.data[zOrig]["nbPrefix"] += 1
        for asn in node.data["path"]:
            root.data[zOrig]["asCount"][asn] += 1
    
    return rtree


def readupdates(filename, rtree):

    p0 = Popen(["bzcat", filename], stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)
    update_tag=""
    
    root = rtree.search_exact("0.0.0.0/0")
    
    for line in p1.stdout:
        line=line.rstrip("\n")
        res = line.split('|',15)
        zOrig = res[3]
        
        if not zOrig in root.data:
            root.data[zOrig] = {"nbPrefix": 0, "asCount": defaultdict(int)}

       
        if res[2] == "W":
            node = rtree.search_exact(res[5])
            if not node is None:
                root.data[zOrig]["nbPrefix"] -= 1
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] -= 1
                node.data[zOrig]["path"] = []
        
        else:
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res

            if zPfx == "0.0.0.0/0":
                continue

            node = rtree.search_exact(zPfx)
            path_list = sPath.split(' ')

            if node is None or not zOrig in node.data:
                node = rtree.add(zPfx)
                node.data[zOrig]["path"] = set(sPath.split(" "))
                root.data[zOrig]["nbPrefix"] += 1
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] += 1

            else:
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] -= 1
                node.data[zOrig]["path"] = set(sPath.split(" "))
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] += 1

    return rtree


def hashfunc(x):
    return int(hashlib.sha512(x).hexdigest(), 16)


def computeSimhash(rtree):

    root = rtree.search_exact("0.0.0.0/0")
    asProb = defaultdict(list)
    nbPrefixList = []
    # For each RIB from our peers
    for peer, count in root.data.iteritems():
        asCount = count["asCount"]
        nbPrefix = count["nbPrefix"]
        nbPrefixList.append(nbPrefix)

        if nbPrefix <= 0:
            continue

        # asCount = defaultdict(int)

        # nbPrefix = 0
        # for node in rtree:
            # for asn in node.data["path"]:
                # asCount[asn] += 1

            # nbPrefix += 1

        for asn, nbPath in asCount.iteritems():
            asProb[asn].append(nbPath/float(nbPrefix))

    asAggProb = {}
    for asn, problist in asProb.iteritems():
        mu = np.mean(problist)
        if mu > 0.0001:
            asAggProb[asn] = mu

    print "\t%s peers" % len(root.data)
    print "\t%s prefixes per peers" % np.mean(nbPrefixList)
    print "\tTotal number of ASN: %s" % len(asProb)
    print "\tNumber of ASN kept for the hash: %s" % len(asAggProb)

    return simhash.Simhash(asAggProb, f=128) # f=512, hashfunc=hashfunc)


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
    rtree = readrib(rib_files)
    arguments.pop(0)

    hashHistory = {"date":[], "hash":[], "distance":[]}
    prevHash = None

    for arg in arguments:
		
        update_files = glob.glob(arg)
	
        if len(update_files)==0:
            sys.exit()
			
        update_files.sort()

        for fi in update_files:
            rtree = readupdates(fi, rtree)
            currHash = computeSimhash(rtree)

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

