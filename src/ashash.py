import sys
import os
import errno
import argparse
from subprocess import Popen, PIPE
import glob
import radix
from collections import defaultdict
from collections import deque
from datetime import datetime
import numpy as np
from scipy import stats
import simhash
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pylab as plt
import hashlib
import cPickle as pickle
from multiprocessing import Pool
import mmh3
import json
import networkx as nx

def findParent(node, zOrig):
    parent = node.parent
    if parent is None or parent.prefix == "0.0.0.0/0":
        return None
    elif zOrig in parent.data and len(parent.data[zOrig]["path"]):
        return parent
    else:
        return findParent(parent, zOrig)


def readrib(ribfile, spatialResolution=1, af=4, rtree=None, filter=None, plot=False, g=None):
    sys.stderr.write("Loading RIB files...") 
    if rtree is None:
        rtree = radix.Radix() 
        root = rtree.add("0.0.0.0/0")
    else:
        root = rtree.search_exact("0.0.0.0/0")

    if plot and g is None:
        g = nx.Graph()

    if ribfile.startswith("@bgpstream:"):
        p1 = Popen(["bgpreader","-m", "-w",ribfile.rpartition(":")[2], "-p", "routeviews", "-c","route-views.linx", "-t","ribs"], stdout=PIPE)
    else:
        p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", ribfile], stdout=PIPE, bufsize=-1)

    for line in p1.stdout: 
        zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = line.split('|',16)

        if af!=0:
            if af == 4 and ":" in zPfx:
                continue
            elif af == 6 and "." in zPfx:
                continue
        
        if zPfx == "0.0.0.0/0":
            continue

        path = sPath.split(" ")
        if not zOrig in root.data:
            root.data[zOrig] = {"totalCount": 0, "asCount": defaultdict(int)}

            if plot:
                # Keep a list of BGP peers
                root.data[zOrig]["peerASN"] = path[0]

        # Check if the origin AS is in the filter:
        if not filter is None:
            try:
                # Check if the origin AS is in the filter:
                if not filter is None:
                    # Check if the prefixes has been announced by filtered ASs or
                    # if the origin AS is in the filter 
                    
                    covered = True
                    filteredOrig = True
                    node = rtree.search_exact(zPfx)

                    if node is None and rtree.search_best(zPfx).prefixlen==0:
                        # No peer has seen this prefix
                        # And it is not covered by known prefixes
                        covered = False

                    try:
                        origAS = int(path[-1])
                        if not origAS in filter:
                            filteredOrig = False
                    except ValueError:
                        # TODO: handle cases for origin from a set of ASs?
                        filteredOrig = False

                    if not covered and not filteredOrig:
                        continue

            except ValueError:
                # TODO: handle cases for origin from a set of ASs?
                continue

        node = rtree.add(zPfx)
        node.data[zOrig] = {"path": set(path), "count": 0}
        if plot:
            path = sPath.split(" ")
            for i in range(len(path)-1): 
                g.add_edge(path[i], path[i+1])

        count = 1
        if spatialResolution:
            if "." in zPfx:
                count = 2**(32-node.prefixlen) 
            elif ":" in zPfx:
                count = 2**(128-node.prefixlen) 

            countBelow = np.sum([n.data[zOrig]["count"] for n in rtree.search_covered(zPfx) if zOrig in n.data])
            count -= countBelow
            node.data[zOrig]["count"] = count

            # Update above nodes
            parent = findParent(node, zOrig)
            if parent is None:
                root.data[zOrig]["totalCount"] += count
            else:
                parent.data[zOrig]["count"] -= count
                for asn in parent.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] -= count 
        
        else: 
            root.data[zOrig]["totalCount"] += count

        for asn in node.data[zOrig]["path"]:
            root.data[zOrig]["asCount"][asn] += count
    
    sys.stderr.write("done!\n") 
    return rtree, g


def readupdates(filename, rtree, spatialResolution=1, af=4, filter=None, plot=False, g=None):

    if ribfile.startswith("@bgpstream:"):
        p1 = Popen(["bgpreader", "-m", "-w", filename.rpartition(":")[2], "-p", "routeviews", "-c", "route-views.linx", "-t", "updates"], stdout=PIPE)
    else:
        p1 = Popen(["bgpdump", "-m", "-v", filename],  stdout=PIPE, bufsize=-1)
    root = rtree.search_exact("0.0.0.0/0")
    stats = {"announce": 0, "withdraw": 0, "pathLen": [], "prefixLen": [], "originAS":set()}
    
    for line in p1.stdout:
        res = line[:-1].split('|',15)

        if res[5] == "0.0.0.0/0":
            continue
        
        if af != 0:
            if af == 4 and ":" in res[5]:
                continue
            elif af == 6 and "." in res[5]:
                continue

        if not res[3] in root.data:
            root.data[res[3]] = {"totalCount": 0, "asCount": defaultdict(int)}

        node = rtree.search_exact(res[5])


        if res[2] == "W":
            zOrig = res[3]
            # Withdraw: remove the corresponding node
            if not node is None and zOrig in node.data and len(node.data[zOrig]["path"]):
                stats["withdraw"] += 1

                if spatialResolution:
                    count = node.data[zOrig]["count"]
                    # Update count for above node
                    parent = findParent(node, zOrig) 
                    if parent is None:
                        # No above node, remove these ips from the total
                        root.data[zOrig]["totalCount"] -= count
                    else:
                        # Add ips to above node and corresponding ASes
                        parent.data[zOrig]["count"] += count
                        for asn in parent.data[zOrig]["path"]:
                            root.data[zOrig]["asCount"][asn] += count 

                    for asn in node.data[zOrig]["path"]:
                        root.data[zOrig]["asCount"][asn] -= count 

                else: 
                    root.data[zOrig]["totalCount"] -= 1
                    for asn in node.data[zOrig]["path"]:
                        root.data[zOrig]["asCount"][asn] -= 1 


                if plot:
                    # Remove edges in the graph
                    for asn in node.data[zOrig]["path"]:
                        allZero = True
                        for peer in root.data.keys():
                            if root.data[peer]["asCount"][asn]!=0:
                                allZero = False
                                break
                            if allZero and asn in g:
                                g.remove_node(asn)

                node.data[zOrig]["path"] = []
                node.data[zOrig]["count"] = 0
        
        else:
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res

            path = sPath.split(" ")
            # Check if the origin AS is in the filter:
            if not filter is None:
                # Check if the prefixes has been announced by filtered ASs or
                # if the origin AS is in the filter 
                
                covered = True
                filteredOrig = True

                if node is None and rtree.search_best(res[5]).prefixlen==0:
                    # No peer has seen this prefix
                    # And it is not covered by known prefixes
                    covered = False

                try:
                    origAS = int(path[-1])
                    if not origAS in filter:
                        filteredOrig = False
                except ValueError:
                    # TODO: handle cases for origin from a set of ASs?
                    filteredOrig = False

                if not covered and not filteredOrig:
                    continue

                if plot and covered and not filteredOrig:
                    print zPfx, sPath


            # Announce:
            stats["announce"] += 1
            stats["pathLen"].append(len(path))
            stats["originAS"].add(path[-1])
            stats["prefixLen"].append(int(zPfx.rpartition("/")[2]))
            if node is None or not zOrig in node.data or not len(node.data[zOrig]["path"]):
                # Add a new node 

                node = rtree.add(zPfx)
                if spatialResolution:
                    # Compute the exact number of IPs
                    if "." in zPfx:
                        count = 2**(32-node.prefixlen) 
                    elif ":" in zPfx:
                        count = 2**(128-node.prefixlen) 
                    countBelow = np.sum([n.data[zOrig]["count"] for n in rtree.search_covered(zPfx) if zOrig in n.data])
                    count -= countBelow

                    # Update above nodes
                    parent = findParent(node, zOrig)
                    if parent is None:
                        root.data[zOrig]["totalCount"] += count
                    else:
                        parent.data[zOrig]["count"] -= count
                        for asn in parent.data[zOrig]["path"]:
                            root.data[zOrig]["asCount"][asn] -= count 

                else:
                    root.data[zOrig]["totalCount"] += 1
                    count = 1

                # Update the ASes counts
                node.data[zOrig] = {"path": set(path), "count": count}
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] += count 

            else:
                #Update node path and counts
                if spatialResolution:
                    count = node.data[zOrig]["count"]
                else:
                    count = 1

                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] -= count
                    if plot and root.data[zOrig]["asCount"][asn]==0:
                        allZero = True
                        for peer in root.data.keys():
                            if root.data[peer]["asCount"][asn]!=0:
                                allZero = False
                                break
                            if allZero and asn in g:
                                g.remove_node(asn)

                node.data[zOrig]["path"] = set(path)
                for asn in node.data[zOrig]["path"]:
                    root.data[zOrig]["asCount"][asn] += count

            if plot:
                # add new edges
                for i in range(len(path)-1): 
                    g.add_edge(path[i], path[i+1])

    return rtree, stats


def hashfunc(x):
    return int(hashlib.sha512(x).hexdigest(), 16)


def sketchesSimhash(sketches):

    hashes = {}
    for sketch, asProb in sketches.iteritems():
        hashes[sketch] = simhash.Simhash(asProb, f=64)

    return hashes


def sketchSet():
    return defaultdict(dict)


def sketching(asAggProb, pool, N, M):
    seeds = [2**i for i in range(1,N+1)]
    sketches = defaultdict(sketchSet) 
    for seed in seeds:
        for asn, prob in asAggProb.iteritems():
            sketches[seed][mmh3.hash128(asn,seed=seed)%M][asn] = prob

    # compute the simhash for each hash function
    hashes= pool.map(sketchesSimhash, sketches.itervalues())

    return dict(zip(sketches.keys(), hashes)), sketches


def aggregateCentrality(asProb, trim=0.1):
    return {asn:float(stats.trim_mean(problist, trim))for asn, problist in asProb.iteritems()}

def computeCentrality(allAsCount, spatial, outFile=None, filter=None):
    # root = 
    asProb = defaultdict(list)
    totalCountList = []
    asList = set()
    for peer, count in allAsCount.iteritems():
        asList.update(count["asCount"].iterkeys())

    # For each RIB from our peers
    for peer, count in allAsCount.iteritems():
        totalCount = count["totalCount"]
        print totalCount

        if filter is None:
            # If there is no filter we want only full feeds
            if (totalCount <= 400000 and not spatial) or (totalCount <= 2000000000 and spatial):
                continue
        elif totalCount == 0:
            continue

        print "accepted!"
        asCount = count["asCount"]
        totalCountList.append(totalCount)

        for asn in asList:
            asProb[asn].append(asCount[asn]/float(totalCount))

    nbPeers = len(totalCountList)
    if nbPeers == 0:
        # There is no peers?
        sys.stderr.write("Warning: no peers!")
        return None, None, 0

    asAggProb = aggregateCentrality(asProb)

    if spatial:
        sys.stdout.write("%s/%s peers, %s ASN, %s addresses per peers, " % (len(totalCountList), 
            len(allAsCount), len(asList), np.mean(totalCountList)))
    else:
        sys.stdout.write("%s/%s peers, %s ASN, %s prefixes per peers, " % (len(totalCountList), 
            len(allAsCount), len(asList), np.mean(totalCountList)))

    if not outFile is None:
        outFile.write("%s | %s | %s | %s | " % (nbPeers, len(allAsCount), len(asProb), np.mean(totalCountList)))

    return asAggProb, asProb, nbPeers


def computeSimhash(rtree, pool, N, M, spatial, outFile=None, filter=None):
    # get AS centrality
    asAggProb, asProb, _ = computeCentrality(rtree.search_exact("0.0.0.0/0").data, spatial, outFile, filter)
    # sketching
    res = sketching(asAggProb, pool, N, M)
    return res, asProb 


def compareSimhash(prevHash, curHash, prevSketches, currSketches, minVotes,  distThresh=3):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--af", help="address family (4, 6, or 0 for both)", type=int, default=4)
    parser.add_argument("-N", help="number of hash functions for sketching", type=int, default=16)
    parser.add_argument("-M", help="number of sketches per hash function", type=int, default=128)
    parser.add_argument("-d","--distThresh", help="simhash distance threshold", type=int, default=3)
    parser.add_argument("-r","--minVoteRatio", help="Minimum ratio of sketches to detect anomalies (should be between 0 and 1)", type=float, default=0.5)
    parser.add_argument("-H","--historyDuration", help="Time duration of the history for computing the reference (mult 15min)", type=int, default=4)
    parser.add_argument("-p", "--proc", help="number of processes", type=int)
    parser.add_argument("-s", "--spatial", help="spatial resolution (0 for prefix, 1 for address)", type=int, default=1)
    parser.add_argument("-f", "--filter", help="Filter: list of ASNs to monitor", type=str, default=None)
    parser.add_argument("-w", "--window", help="Time window: time resolution in seconds (works only with  bgpstream)", type=int, default=900)
    parser.add_argument("--plot", help="plot figures", action="store_true")
    parser.add_argument("ribs", help="RIBS files")
    parser.add_argument("updates", help="UPDATES files", nargs="+")
    parser.add_argument("output", help="output directory")
    args = parser.parse_args()

    if args.proc is None:
        args.proc = args.N

    if not args.filter is None:
        filter = json.loads(args.filter)
        if isinstance(filter, int):
            filter = [filter]
        elif not isinstance(filter, list):
            sys.stderr.write("Filter: Wrong format! Should be a list of ASNs (e.g. [1,2,3]) or a single ASN.")
            sys.exit()
    else:
        filter = None

    try:
        os.makedirs(os.path.dirname(args.output))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

    p=Pool(args.proc)
		
    # read rib files
    if args.ribs.startswith("@bgpstream:"):
        rib_files = [args.ribs]
    else:
        rib_files = glob.glob(args.ribs)
        if len(rib_files)==0:
            sys.stderr.write("Files not found! (%s)\n" % args.ribs)
            sys.exit()

        rib_files.sort()
    
    rtree = None
    g = None
    for ribfile in rib_files:
        rtree, g = readrib(ribfile, args.spatial, args.af, rtree, filter, args.plot, g)
        if not filter is None:
            # Need a second pass to account for delegated prefixes
            rtree, g = readrib(ribfile, args.spatial, args.af, rtree, filter, args.plot, g)

        if args.plot:
            # Add centrality values
            asAggProb, asProb, _ = computeCentrality(rtree.search_exact("0.0.0.0/0").data, args.spatial, filter)
            nx.set_node_attributes(g, "AS hegemony", asAggProb)
            # Set nodes color (peers are blue and filtered ASN are red)
            root = rtree.search_exact("0.0.0.0/0")
            nodeColor = {data["peerASN"]:"b" for peer, data in root.data.iteritems() if data["peerASN"] in g}
            if not filter is None:
                for asn in filter:
                    nodeColor[str(asn)] = "r"
            nx.set_node_attributes(g, "color", nodeColor) 
            nx.write_gexf(g, args.output+"/graph_start.gexf")

    (currHash, currSketches), currAsProb = computeSimhash(rtree, p, args.N, args.M, args.spatial, filter=filter)

    # initialisation for the figures and output
    hashHistory = {"date":[], "hash":[], "distance":[], "reportedASN":[]}
    outFile = open(args.output+"/results_ip.txt","w")
    root = rtree.search_exact("0.0.0.0/0")
    refAsProb = defaultdict(lambda : deque(maxlen=args.historyDuration*len(root.data)))

    # Update the reference
    for asn, prob in currAsProb.iteritems():
        refAsProb[asn].extend(prob)

    # read update files
    for updates in args.updates:
		
        bgpstream = False
        if updates.startswith("@bgpstream:"):
            bgpstream = True
            w = updates.rpartition(":")[2].split(",")
            ts,te = int(w[0]),int(w[1]) 
            
            update_files = ["@bgpstream:%s,%s" % (t, t+args.window) for t in range(ts, te, args.window)]

        else:
            update_files = glob.glob(updates)
            update_files.sort()

        if len(update_files)==0:
            sys.exit()

        for fi in update_files:
            if bgpstream:
                date = datetime.utcfromtimestamp(int(fi.rpartition(":")[2].partition(",")[0]))
                date = ".%s%02d%02d.%02d%02d" % (date.year, date.month, date.day, date.hour, date.minute)
                date = date.split(".")
            else:
                filename = fi.rpartition("/")[2]
                date = filename.split(".")

            sys.stdout.write("\r %s:%s " % (date[1], date[2]))
            rtree, updateStats = readupdates(fi, rtree, args.spatial, args.af, filter, args.plot, g)

            outFile.write("%s:%s | %s | %s | %s | %s | %s | " % (date[1], date[2], updateStats["announce"], \
                    updateStats["withdraw"], np.median(updateStats["prefixLen"]), \
                    np.median(updateStats["pathLen"]), len(updateStats["originAS"]) ) )
            (currHash, currSketches), currAsProb = computeSimhash(rtree, p, args.N, args.M, args.spatial, outFile, filter)

            if currHash is None:
                anomalousAsn = []
                nbAnoSketch =  np.nan
                distance = np.nan
            else:
                aggProb = aggregateCentrality(refAsProb)
                refHash, refSketches = sketching(aggProb, p, args.N, args.M)
                anomalousAsn, nbAnoSketch, distance = compareSimhash(refHash, currHash, refSketches, currSketches, int(args.N*args.minVoteRatio), args.distThresh)

            if args.plot:
                hashHistory["date"].append( datetime.strptime(date[1]+date[2], "%Y%m%d%H%M"))
                hashHistory["distance"].append(distance)
                hashHistory["reportedASN"].append(len(anomalousAsn))

            sys.stdout.write("%s anomalous sketches (dist=%s), " % (nbAnoSketch, distance))
            if len(anomalousAsn):
                sys.stdout.write("%s" % (anomalousAsn))
            sys.stdout.flush()

            outFile.write("%s | %s | %s \n" % (nbAnoSketch, distance, anomalousAsn) )
            outFile.flush()
        
            if not currHash is None:
                # Update the reference
                for asn, prob in currAsProb.iteritems():
                    refAsProb[asn].extend(prob)


    if args.plot :

        # Add centrality values
        asAggProb, asProb, _ = computeCentrality(rtree.search_exact("0.0.0.0/0").data, args.spatial, filter)
        prob = {asn:val for asn, val in asAggProb.iteritems() if asn in g}
        nx.set_node_attributes(g, "centrality", prob)
        # Set nodes color (peers are blue and filtered ASN are red)
        root = rtree.search_exact("0.0.0.0/0")
        nodeColor = {data["peerASN"]:"b" for peer, data in root.data.iteritems() if data["peerASN"] in g}
        if not filter is None:
            for asn in filter:
                if str(asn) in g:
                    nodeColor[str(asn)] = "r"
        nx.set_node_attributes(g, "color", nodeColor)
        nx.write_gexf(g, args.output+"/graph_end.gexf")
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
