from subprocess import Popen, PIPE
import os
import glob
import radix
from collections import defaultdict

import threading
import copy
import logging


# Needed for pickling objects
def __ddint():
    return defaultdict(int)


def pathCountDict():
    return {"total": defaultdict(int), "asn": defaultdict(__ddint) ,}


class pathCounter(threading.Thread):

    def __init__(self, ribfile, updatefiles, announceQueue, countQueue, ribQueue, 
            spatialResolution=1, af=4, timeWindow=900, asnFilter=None ):
        threading.Thread.__init__ (self)
        self.__nbaddr = {4:{i: 2**(32-i) for i in range(33) }, 6: {i: 2**(128-i) for i in range(129) }}

        self.ribfile = ribfile
        self.updatefiles = updatefiles
        self.announceQueue = announceQueue
        self.countQueue = countQueue
        self.ribQueue = ribQueue

        self.spatialResolution = spatialResolution
        self.af = af
        self.asnFilter = asnFilter
        self.timeWindow = timeWindow

        self.rtree = radix.Radix()

        self.ts = None
        self.peers = None
        self.peersASN = defaultdict(set) 
        self.peersPerASN = defaultdict(list)

        self.counter = {
                "all": pathCountDict(),
                "origas": defaultdict(pathCountDict),
                }


    def run(self):
        logging.info("Reading RIB files...")
        self.readrib()
        self.peers = self.findFullFeeds()
        self.peersASN = {p:self.peersASN[p] for p in self.peers} 
        for p, a in self.peersASN.iteritems():
            if len(a)>1:
                logging.warn("(Path counter) peer %s maps to more than one AS (%s)" % (p,a))
                continue
            self.peersPerASN[list(a)[0]].append(p)

        logging.debug("(pathCounter) %s " % self.peersASN)
        self.cleanUnusedCounts()

        logging.info("Reading UPDATE files...")
        noUpdates = True
        for updatefile in self.updatefiles:
            if updatefile.startswith("@bgpstream") or os.path.exists(updatefile):
                noUpdates = False
                self.readupdates(updatefile)
            else:
                logging.info("(pathCounter) Ignoring update file: %s" % updatefile)

        if noUpdates:
            self.ts = 0
            self.slideTimeWindow(1)

        logging.info("(pathCounter) Finished to read data")


    def nbIPs(self, prefixlen):
        return self.__nbaddr[self.af][prefixlen]


    def findParent(self, node, zOrig):
        parent = node.parent
        if parent is None or parent.prefix == "0.0.0.0/0":
            return None
        elif zOrig in parent.data and len(parent.data[zOrig]["path"]):
            return parent
        else:
            return self.findParent(parent, zOrig)

    def findFullFeeds(self):
        logging.debug("(pathCounter) finding full feed peers...")
        nbPrefixes = defaultdict(int)
        nodes = self.rtree.nodes()

        for node in self.rtree.nodes():
            for peer in node.data.keys():
                nbPrefixes[peer] += 1

        res = set([peer for peer, nbPfx in nbPrefixes.iteritems() if nbPfx>len(nodes)*0.75])
        logging.debug("(pathCounter) %s full feed peers" % len(res))

        return res

    def saveGraph(self):
        pass
        

    def cleanUnusedCounts(self):

        toRemove = [peer for peer in self.counter["all"]["total"].keys() if not peer in self.peers]
        for peer in toRemove:
            # Remove from counter["all"]["total"]
            del self.counter["all"]["total"][peer]
            # Remove from counter["all"]["asn"]
            for asn, d in self.counter["all"]["asn"].iteritems():
                if peer in d:
                    del d[peer]

            for node in self.rtree.nodes():
                if peer in node.data:
                    origAS = node.data[peer]["origAS"]
                    # Remove from counter["origas"][..]["total"]
                    if peer in self.counter["origas"][origAS]["total"]:
                        del self.counter["origas"][origAS]["total"][peer]
                    del node.data[peer]

                    # Remove from counter["origas"][..]["asn"]
                    for asn, d in self.counter["origas"][origAS]["asn"].iteritems():
                        if peer in d:
                            del d[peer]


    def slideTimeWindow(self,ts):
        logging.debug("(pathCounter) sliding window... (ts=%s)" % self.ts)
        
        self.countQueue.put( (self.ts, self.peersPerASN, self.counter) )
        self.countQueue.join()
        self.ts = ts
        
        logging.debug("(pathCounter) window slided (ts=%s)" % self.ts)


    def incTotalCount(self, count, peerip, origAS, zAS):
        self.counter["all"]["total"][peerip] += count
        self.counter["origas"][origAS]["total"][peerip] += count
        # assert self.counter["origas"][origAS]["total"][peerip] >= 0


    def incCount(self, count, peerip, origAS, peerAS, asns):
        for asn in asns:
            self.counter["all"]["asn"][asn][peerip] += count
            self.counter["origas"][origAS]["asn"][asn][peerip] += count
            # assert self.counter["all"]["asn"][asn][peerip] >= 0
            # assert self.counter["origas"][origAS]["asn"][asn][peerip] >= 0


    def readrib(self):

        if self.ribfile.startswith("@bgpstream:"):
            if self.af == 6:
                bgprFilter = "ipversion 6"
            else:
                bgprFilter =  "ipversion 4"

            bgprFilter += " and collector route-views.linx and collector route-views2 and collector rrc00 and collector rrc10"

            if not self.asnFilter is None:
                bgprFilter += ' and path %s$' % self.asnFilter
            cmd = "bgpreader -m -w "+self.ribfile.rpartition(":")[2]+" -f '"+bgprFilter+"' -t ribs"
            p1 = Popen(cmd, shell=True ,stdout=PIPE)
        else:
            p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", self.ribfile], stdout=PIPE, bufsize=-1)

        for line in p1.stdout: 
            print line
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zOther = line.split('|',7)

            if self.af == 4 and ":" in zPfx:
                continue
            elif self.af == 6 and "." in zPfx:
                continue
            
            if zPfx == "0.0.0.0/0":
                continue

            self.peersASN[zOrig].add(zAS)

            path = sPath.split(" ")
            origAS = path[-1]
            if len(path) < 2:
                # Ignore paths with only one AS
                continue

            node = self.rtree.add(zPfx)
            if zOrig in node.data:
                # Already read this entry, we should read only one RIB per peer
                continue

            if not self.ribQueue is None:
                self.ribQueue.put( (zTd, zDt, zS, zOrig, zAS, zPfx, path, zOther) )


            node.data[zOrig] = {"path": set(path), "count": 0, "origAS":origAS}

            if self.spatialResolution:
                # compute weight for this path
                count = self.nbIPs(node.prefixlen)
                countBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(zPfx) if n.parent == node and zOrig in n.data])
                count -= countBelow
                # assert count >= 0
                node.data[zOrig]["count"] = count

                # Update above nodes
                parent = self.findParent(node, zOrig)
                if not parent is None:
                    pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if n.parent == parent and zOrig in n.data])
                    oldpCount = parent.data[zOrig]["count"]
                    pCount = self.nbIPs(parent.prefixlen) - pcountBelow
                    pdiff = pCount - oldpCount
                    parent.data[zOrig]["count"] = pCount 
                    pOrigAS = parent.data[zOrig]["origAS"]
                    asns = parent.data[zOrig]["path"]
                    self.incCount(pdiff, zOrig, pOrigAS, zAS, asns)
                    self.incTotalCount(pdiff, zOrig, pOrigAS, zAS)
            else:
                count = 1

            asns = node.data[zOrig]["path"]
            self.incTotalCount(count, zOrig, origAS, zAS)
            self.incCount(count, zOrig, origAS, zAS, asns)
        

    def readupdates(self, updatefile):

        if updatefile.startswith("@bgpstream:"):
            if self.af == 6:
                bgprFilter = "ipversion 6"
            else:
                bgprFilter =  "ipversion 4"

            if not self.asnFilter is None:
                bgprFilter += ' and path "%s$" ' % self.asnFilter

            cmd = "bgpreader -m -w '"+self.ribfile.rpartition(":")[2]+"' -f '"+bgprFilter+"' -c route-views.linx -c route-views2 -c rrc00 -c rrc10 -t updates"
            p1 = Popen(cmd, shell=True, stdout=PIPE)

            # p1 = Popen(["bgpreader", "-m", "-w", updatefile.rpartition(":")[2], "-c", "route-views.linx", "-t", "updates"], stdout=PIPE)
        else:
            p1 = Popen(["bgpdump", "-m", "-v", updatefile],  stdout=PIPE, bufsize=-1)
        
        for line in p1.stdout:
            res = line[:-1].split('|',15)
            zOrig = res[3]

            if res[5] == "0.0.0.0/0":
                continue
            
            if self.af == 4 and ":" in res[5]:
                continue
            elif self.af == 6 and "." in res[5]:
                continue
            
            msgTs = int(res[1])
            # set first time bin!
            if self.ts is None:
                self.ts = 0
                self.slideTimeWindow(msgTs)
            
            elif self.ts + self.timeWindow < msgTs:
                self.slideTimeWindow(msgTs)

            elif self.ts > msgTs:
                #Old update, ignore this to update the graph
                logging.warn("Ignoring old update (peer IP: %s, timestamp: %s, current time bin: %s): %s" % (res[3], res[1], self.ts, line))
                continue

            node = self.rtree.search_exact(res[5])

            if res[2] == "W":
                zAS = res[4]
                # Withdraw: remove the corresponding node
                if not node is None and zOrig in node.data and len(node.data[zOrig]["path"]):
                    origAS = node.data[zOrig]["origAS"]

                    if self.spatialResolution:
                        count = node.data[zOrig]["count"]
                        # Update count for above node
                        parent = self.findParent(node, zOrig) 
                        if parent is None:
                            # No above node, remove these ips from the total
                            self.incTotalCount(-count,  zOrig, origAS, zAS)
                        else:
                            # Add ips to above node and corresponding ASes
                            pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if n.parent == parent and zOrig in n.data])
                            oldpCount = parent.data[zOrig]["count"]
                            pCount = self.nbIPs(parent.prefixlen) - pcountBelow
                            pdiff = pCount - oldpCount
                            parent.data[zOrig]["count"] = pCount 
                            porigAS = parent.data[zOrig]["origAS"]
                            asns= parent.data[zOrig]["path"]
                            self.incCount(pdiff,  zOrig, porigAS, zAS, asns)
                            self.incTotalCount(pdiff, zOrig, porigAS, zAS)
                            self.incTotalCount(count, zOrig, origAS, zAS)

                        asns = node.data[zOrig]["path"]
                        self.incCount(count,  zOrig, origAS, zAS, asns)

                    else: 
                        self.incTotalCount(-1,  zOrig, origAS, zAS)
                        asns = node.data[zOrig]["path"]
                        self.incCount(-1,  zOrig, origAS, zAS, asns)

                    node.data[zOrig]["path"] = []
                    node.data[zOrig]["count"] = 0
                    node.data[zOrig]["origAS"] = "" 
            
            else:
                # Announce: update counters
                zTd, zDt, zS, zOrig, zAS, zPfx, sPath = res[:7]
                path = sPath.split(" ")

                if len(path) < 2:
                    # Ignoring paths with only one AS
                    continue

                self.announceQueue.put( res )

                if  zOrig not in self.peers:
                    # no need to update the counts for non-full feed peers
                    continue

                origAS = path[-1]

                # Announce:
                if node is None or not zOrig in node.data or not len(node.data[zOrig]["path"]):
                    # Add a new node 

                    node = self.rtree.add(zPfx)
                    if self.spatialResolution:
                        # Compute the exact number of IPs
                        count = self.nbIPs(node.prefixlen)
                        countBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(zPfx) if n.parent == node and zOrig in n.data])
                        count -= countBelow

                        parent = self.findParent(node, zOrig)
                        if parent is None:
                            self.incTotalCount(count,  zOrig, origAS, zAS)
                        else:
                        # Update above nodes 
                            pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if n.parent == parent and zOrig in n.data])
                            oldpCount = parent.data[zOrig]["count"]
                            pCount = self.nbIPs(parent.prefixlen) - pcountBelow
                            pdiff = pCount - oldpCount
                            parent.data[zOrig]["count"] = pCount 
                            porigAS = parent.data[zOrig]["origAS"]
                            asns = parent.data[zOrig]["path"]
                            self.incCount(pdiff,  zOrig, porigAS, zAS, asns)
                            self.incTotalCount(pdiff, zOrig, porigAS, zAS)
                            self.incTotalCount(count, zOrig, origAS, zAS)

                    else:
                        self.incTotalCount(1,  zOrig, origAS, zAS)
                        count = 1

                    # Update the ASes counts
                    node.data[zOrig] = {"path": set(path), "count": count, "origAS": origAS}
                    asn = node.data[zOrig]["path"]
                    self.incCount(count,  zOrig, origAS, zAS, asns)

                else:
                    #Update node path and counts
                    if self.spatialResolution:
                        count = node.data[zOrig]["count"]
                    else:
                        count = 1

                    porigAS = node.data[zOrig]["origAS"]
                    self.incTotalCount(-count,  zOrig, porigAS, zAS)
                    self.incTotalCount(count,  zOrig, origAS, zAS)

                    asns = node.data[zOrig]["path"]
                    self.incCount(-count,  zOrig, porigAS, zAS, asns)

                    node.data[zOrig]["path"] = set(path)
                    node.data[zOrig]["origAS"] = origAS
                    asns = node.data[zOrig]["path"]
                    self.incCount(count,  zOrig, origAS, zAS, asns)


