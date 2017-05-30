from subprocess import Popen, PIPE
import glob
import radix
from collections import defaultdict
import numpy as np

import threading
import copy
import logging

def pathCountDict():
    return {"total": defaultdict(int), "asn": defaultdict(lambda : defaultdict(int)) ,}


class pathCounter(threading.Thread):

    def __init__(self, ribfile, updatefile, announceQueue, countQueue, spatialResolution=1, af=4, timeWindow=900 ):
        threading.Thread.__init__ (self)
        self.__nbaddr = {4:{i: 2**(32-i) for i in range(33) }, 6: {i: 2**(128-i) for i in range(129) }}

        self.ribfile = ribfile
        self.updatefiles = updatefile
        self.announceQueue = announceQueue
        self.countQueue = countQueue

        self.spatialResolution = spatialResolution
        self.af = af
        self.timeWindow = timeWindow

        self.rtree = radix.Radix()
        root = self.rtree.add("0.0.0.0/0")

        self.ts = None

        self.counter = {
                "all": pathCountDict(),
                "origas": defaultdict(pathCountDict),
                }


    def run(self):
        logging.info("Reading RIB files...")
        self.readrib()
        logging.info("Reading UPDATE files...")
        for updatefile in self.updatefiles:
            self.readupdates(updatefile)


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

    def slideTimeWindow(self,ts):
        logging.debug("(pathCounter) sliding window...")
        if self.af == 4:
            minNbEntries = 400000
        elif self.af == 6:
            minNbEntries = 1000000000 

        peers = [peer for peer, count in self.counter["all"]["total"].iteritems() if count > minNbEntries]

        logging.debug("(pathCounter) push data")
        # self.countQueue.put( (self.ts, peers, copy.deepcopy(self.counter)) )
        self.countQueue.put( (self.ts, peers, self.counter) )
        self.countQueue.join()
        self.ts = ts
        
        logging.debug("(pathCounter) window slided")


    def incTotalCount(self, count, peerip, origAS, zAS):
        self.counter["all"]["total"][peerip] += count
        # self.counter["origas"][origAS]["total"][peerip] += count


    def incCount(self, count, peerip, origAS, peerAS, asns):
        for asn in asns:
            self.counter["all"]["asn"][asn][peerip] += count
            # self.counter["origas"][origAS]["asn"][asn][peerip] += count


    def readrib(self):

        root = self.rtree.search_exact("0.0.0.0/0")

        if self.ribfile.startswith("@bgpstream:"):
            p1 = Popen(["bgpreader","-m", "-w",self.ribfile.rpartition(":")[2], "-p", "routeviews", "-c","route-views.wide", "-t","ribs"], stdout=PIPE)
        else:
            p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", self.ribfile], stdout=PIPE, bufsize=-1)

        for line in p1.stdout: 
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = line.split('|',16)

            if self.af == 4 and ":" in zPfx:
                continue
            elif self.af == 6 and "." in zPfx:
                continue
            
            if zPfx == "0.0.0.0/0":
                continue

            # set first time bin!
            if self.ts is None:
                self.ts = int(zDt)

            path = sPath.split(" ")
            origAS = path[-1]

            node = self.rtree.add(zPfx)
            node.data[zOrig] = {"path": set(path), "count": 0, "origAS":origAS}

            count = 1
            if self.spatialResolution:
                count = self.nbIPs(node.prefixlen)

                countBelow = np.sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(zPfx) if zOrig in n.data])
                count -= countBelow
                node.data[zOrig]["count"] = count

                # Update above nodes
                parent = self.findParent(node, zOrig)
                if parent is None:
                    self.incTotalCount(count, zOrig, origAS, zAS)
                else:
                    parent.data[zOrig]["count"] -= count
                    pOrigAS = parent.data[zOrig]["origAS"]
                    asns = parent.data[zOrig]["path"]
                    self.incCount(-count, zOrig, pOrigAS, zAS, asns)
            
            else: 
                self.incTotalCount(count, zOrig, origAS, zAS)

            asns = node.data[zOrig]["path"]
            self.incCount(count, zOrig, origAS, zAS, asns)
        
        # self.cursor.execute("commit;")


    def readupdates(self, updatefile):

        if updatefile.startswith("@bgpstream:"):
            p1 = Popen(["bgpreader", "-m", "-w", updatefile.rpartition(":")[2], "-p", "routeviews", "-c", "route-views.wide", "-t", "updates"], stdout=PIPE)
        else:
            p1 = Popen(["bgpdump", "-m", "-v", updatefile],  stdout=PIPE, bufsize=-1)
        
        for line in p1.stdout:
            res = line[:-1].split('|',15)

            if res[5] == "0.0.0.0/0":
                continue
            
            if self.af == 4 and ":" in res[5]:
                continue
            elif self.af == 6 and "." in res[5]:
                continue
            
            msgTs = int(res[1])
            if self.ts + self.timeWindow < msgTs:
                self.slideTimeWindow(msgTs)

            if self.ts > msgTs:
                #Old update, ignore this to update the graph
                logging.warn("Ignoring old update (peer IP: %s, timestamp: %s, current time bin: %s" % (res[3], res[1], self.ts))
                continue

            node = self.rtree.search_exact(res[5])

            if res[2] == "W":
                zOrig = res[3]
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
                            parent.data[zOrig]["count"] += count
                            porigAS = parent.data[zOrig]["origAS"]
                            asns= parent.data[zOrig]["path"]
                            self.incCount(count,  zOrig, porigAS, zAS, asns)

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
                zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res
                path = sPath.split(" ")

                self.announceQueue.put( (zTd, zDt, zS, zOrig, zAS, zPfx, path, zPro, zOr, z0, z1, z2, z3, z4, z5 ) )

                origAS = path[-1]

                # Announce:
                if node is None or not zOrig in node.data or not len(node.data[zOrig]["path"]):
                    # Add a new node 

                    node = self.rtree.add(zPfx)
                    if self.spatialResolution:
                        # Compute the exact number of IPs
                        count = self.nbIPs(node.prefixlen)
                        countBelow = np.sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(zPfx) if zOrig in n.data])
                        count -= countBelow

                        parent = self.findParent(node, zOrig)
                        if parent is None:
                            self.incTotalCount(count,  zOrig, porigAS, zAS)
                        else:
                        # Update above nodes 
                            parent.data[zOrig]["count"] -= count
                            porigAS = parent.data[zOrig]["origAS"]
                            asns = parent.data[zOrig]["path"]
                            self.incCount(count,  zOrig, porigAS, zAS, asns)

                    else:
                        self.incTotalCount(1,  zOrig, porigAS, zAS)
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


        # self.cursor.execute("commit;")




