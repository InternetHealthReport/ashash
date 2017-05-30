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

__nbaddr = {4:{i: 2**(32-i) for i in range(33) }, 6: {i: 2**(128-i) for i in range(129) }}

class pathCounter(threading.Thread):

    def __init__(self, ribfile, updatefile, announceQueue, countQueue, spatialResolution=1, af=4, filterAS=None, pathCountDB="pathCount.db"):
        threading.Thread.__init__ (self)

        self.ribfile = ribfile
        self.updatefile = updatefile
        self.announceQueue = announceQueue
        self.countQueue = countQueue

        self.spatialResolution = spatialResolution
        self.af = af
        self.filterAS = filterAS

        self.rtree = radix.Radix()
        root = self.rtree.add("0.0.0.0/0")

        self.ts = None

        self.counter = {
                "all": pathCountDict,
                "origas": defaultdict(pathCountDict),
                }
        # self.conn = apsw.Connection(":memory:", statementcachesize=500000)
        # # Initialize the database
        # self.cursor = self.conn.cursor()
        # ## tables for the whole graph
        # self.cursor.execute("CREATE TABLE IF NOT EXISTS all_total (date integer NOT NULL, peerip text, count integer default 0)")
        # self.cursor.execute("CREATE TABLE IF NOT EXISTS all_asn (date integer NOT NULL, peerip text, asn integer, count integer default 0)")
        # self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peerip ON all_total (peerip)")
        # self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peerip_asn ON all_asn (peerip, asn)")

        # ## tables for "origin AS" graphs
        # self.cursor.execute("CREATE TABLE IF NOT EXISTS origas_total (date integer NOT NULL, peerip text, origas integer, count integer default 0)")
        # self.cursor.execute("CREATE TABLE IF NOT EXISTS origas_asn (date integer NOT NULL, peerip text, origas integer, asn integer, count integer default 0)")
        # self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peerip_origas ON origas_total (peerip, origas)")
        # self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peerip_origas_asn ON origas_asn (peerip, origas, asn)")

        # ## tables for "peer AS" graphs
        # self.cursor.execute("CREATE TABLE IF NOT EXISTS peeras_total (date integer NOT NULL, peerip text, peeras integer, count integer default 0)")
        # self.cursor.execute("CREATE TABLE IF NOT EXISTS peeras_asn (date integer NOT NULL, peerip text, peeras integer, asn integer, count integer default 0)")
        # self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peerip_peeras ON peeras_total (peerip, peeras)")
        # self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peerip_peeras_asn ON peeras_asn (peerip, peeras, asn)")


    def run(self):
        logging.info("Reading RIB files...")
        self.readrib()
        logging.info("Reading UPDATE files...")
        self.readupdates()


    def nbIPs(self, prefixlen):
        return __nbaddr[self.af][prefixlen]


    def findParent(self, node, zOrig):
        parent = node.parent
        if parent is None or parent.prefix == "0.0.0.0/0":
            return None
        elif zOrig in parent.data and len(parent.data[zOrig]["path"]):
            return parent
        else:
            return self.findParent(parent, zOrig)

    def slideTimeWindow(self,ts):
        if self.af == 4:
            peers = [peer for peer, count in self.counter["all"]["total"].iteritems() if count > 400000]
        else self.af == 6:
            peers = [peer for peer, count in self.counter["all"]["total"].iteritems() if count > 1000000000]

        self.countQueue.put( (self.ts, peers, copy.deepcopy(self.counter)) )
        self.ts = ts

    def incTotalCount(self, count, peerip, origAS, zAS):
        self.counter["all"]["total"][peerip] += count
        self.counter["origas"][origAS]["total"][peerip] += count
        # self.cursor.execute("INSERT OR IGNORE INTO all_total (date, peerip) VALUES (?,?)", (self.ts, zOrig))
        # self.cursor.execute("UPDATE all_total SET count = count + ? WHERE date=? AND peerip=?", (count, self.ts, zOrig))
        # self.cursor.execute("INSERT OR IGNORE INTO origas_total (date, peerip, origas) VALUES (?,?,?)", (self.ts, zOrig, origAS) )
        # self.cursor.execute("UPDATE origas_total SET count = count + ? WHERE date=? AND peerip=? AND origas=?", (count, self.ts, zOrig, origAS) )
        # self.cursor.execute("INSERT OR IGNORE INTO peeras_total (date, peerip, peeras) VALUES (?,?,?)", (self.ts, zOrig, zAS) )
        # self.cursor.execute("UPDATE peeras_total SET count = count + ? WHERE date=? AND peerip=? AND peeras=?", (count, self.ts, zOrig, zAS) )

    def incCount(self, count, peerip, origAS, peerAS, asns):
        for asn in asns:
            self.counter["all"]["asn"][asn][peerip] += count
            self.counter["origas"][origAS]["asn"][asn][peerip] += count

        # counts = [count]*len(asns)
        # tss = [self.ts]*len(asns)
        # peerips = [zOrig]*len(asns)
        # porigass = [pOrigAS]*len(asns)
        # peerass = [zAS]*len(asns)
        # self.cursor.executemany("INSERT OR IGNORE INTO all_asn (date, peerip, asn) VALUES (?,?,?)", zip(tss, peerips, asns))
        # self.cursor.executemany("UPDATE all_asn SET count = count + ? WHERE date=? AND peerip=? AND asn=?", zip(counts, tss, peerips, asns))
        # self.cursor.executemany("INSERT OR IGNORE INTO origas_asn (date, peerip, origas, asn) VALUES (?,?,?,?)", zip(tss, peerips, porigass, asns) )
        # self.cursor.executemany("UPDATE origas_asn SET count = count + ? WHERE date=? AND peerip=? AND origas=? AND asn=?", zip(counts, tss, peerips, porigass, asns))
        # self.cursor.executemany("INSERT OR IGNORE INTO peeras_asn (date, peerip, peeras, asn) VALUES (?,?,?,?)",  zip(tss, peerips, peerass, asns))
        # self.cursor.executemany("UPDATE peeras_asn SET count = count + ? WHERE date=? AND peerip=? AND peeras=? AND asn=?", zip(counts, tss, peerips, peerass, asns))


    def readrib(self):

        # self.cursor.execute("begin transaction;")
        if not self.filterAS is None:
            # Need a second pass to account for delegated prefixes
            self.readrib()

        root = self.rtree.search_exact("0.0.0.0/0")

        if self.ribfile.startswith("@bgpstream:"):
            p1 = Popen(["bgpreader","-m", "-w",self.ribfile.rpartition(":")[2], "-p", "routeviews", "-c","route-views.linx", "-t","ribs"], stdout=PIPE)
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
                self.ts = zDt

            path = sPath.split(" ")
            origAS = path[-1]

            # Check if the origin AS is in filterAS:
            if not self.filterAS is None:
                try:
                    # Check if the prefixes has been announced by filtered ASs or
                    # if the origin AS is in the filter 
                    
                    covered = True
                    filteredOrig = True
                    node = self.rtree.search_exact(zPfx)
                    best = self.rtree.search_best(zPfx)
                    if best is None or (node is None and best.prefixlen==0):
                        # No peer has seen this prefix
                        # And it is not covered by known prefixes
                        covered = False

                    try:
                        if not int(origAS) in self.filterAS:
                            filteredOrig = False
                    except ValueError:
                        # TODO: handle cases for origin from a set of ASs?
                        filteredOrig = False

                    if not covered and not filteredOrig:
                        continue

                except ValueError:
                    # TODO: handle cases for origin from a set of ASs?
                    continue


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


    def readupdates(self):
        # self.cursor.execute("begin transaction;")

        if self.updatefile.startswith("@bgpstream:"):
            p1 = Popen(["bgpreader", "-m", "-w", self.updatefile.rpartition(":")[2], "-p", "routeviews", "-c", "route-views.linx", "-t", "updates"], stdout=PIPE)
        else:
            p1 = Popen(["bgpdump", "-m", "-v", self.updatefile],  stdout=PIPE, bufsize=-1)
        
        for line in p1.stdout:
            res = line[:-1].split('|',15)

            if res[5] == "0.0.0.0/0":
                continue
            
            if self.af == 4 and ":" in res[5]:
                continue
            elif self.af == 6 and "." in res[5]:
                continue

            if self.ts + self.timeWindow < res[1]:
                self.slideTimeWindow(int(res[1]))

            if self.ts > res[1]:
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

                self.announceQueue.put(zTd, zDt, zS, zOrig, zAS, zPfx, path, zPro, zOr, z0, z1, z2, z3, z4, z5 )

                origAS = path[-1]
                # Check if the origin AS is in the filterAS:
                if not self.filterAS is None:
                    # Check if the prefixes has been announced by filtered ASs or
                    # if the origin AS is in the filter 
                    
                    covered = True
                    filteredOrig = True

                    if node is None and self.rtree.search_best(res[5]).prefixlen==0:
                        # No peer has seen this prefix
                        # And it is not covered by known prefixes
                        covered = False

                    try:
                        if not int(origAS) in self.filterAS:
                            filteredOrig = False
                    except ValueError:
                        # TODO: handle cases for origin from a set of ASs?
                        filteredOrig = False

                    if not covered and not filteredOrig:
                        continue


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




