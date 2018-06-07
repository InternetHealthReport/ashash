from subprocess import Popen, PIPE
import os
import glob
import radix
from collections import defaultdict
from datetime import datetime

import threading
import copy
import logging
import txtReader

from _pybgpstream import BGPStream, BGPRecord, BGPElem


# Needed for pickling objects
def __ddint():
    return defaultdict(int)


def pathCountDict():
    return {"total": defaultdict(int), "asn": defaultdict(__ddint) ,}


def dt2ts(dt):
    return (dt - datetime(1970, 1, 1)).total_seconds()



class pathCounter(threading.Thread):

    def __init__(self, starttime, endtime, announceQueue, countQueue, ribQueue, 
            spatialResolution=1, af=4, timeWindow=900, #asnFilter=None, 
            collectors=[ "route-views.linx", "route-views3", "rrc00", "rrc10"],
            includedPeers=[], excludedPeers=[], includedOrigins=[], excludedOrigins=[], 
            onlyFullFeed=True, txtFile=None):

        threading.Thread.__init__ (self)
        self.__nbaddr = {4:{i: 2**(32-i) for i in range(33) }, 6: {i: 2**(128-i) for i in range(129) }}

        self.startts = int(dt2ts(starttime))
        self.endts = int(dt2ts(endtime))
        self.livemode = False
        if endtime > datetime.utcnow():
            self.livemode = True
        self.announceQueue = announceQueue
        self.countQueue = countQueue
        self.ribQueue = ribQueue

        self.spatialResolution = spatialResolution
        self.af = af
        # self.asnFilter = asnFilter
        self.timeWindow = timeWindow

        self.rtree = radix.Radix()

        self.collectors = collectors
        self.excludedPeers = set([int(x) for x in excludedPeers])
        self.includedPeers = set([int(x) for x in includedPeers])
        self.excludedOriginASN = set([x.strip() for x in excludedOrigins if "/" not in x])
        self.includedOriginASN = set([x.strip() for x in includedOrigins if "/" not in x])
        self.excludedPrefix = set([x.strip() for x in excludedOrigins if "/" in x])
        self.includedPrefix = set([x.strip() for x in includedOrigins if "/" in x])
        self.ts = None
        self.peers = None
        self.peersASN = defaultdict(set) 
        self.peersPerASN = defaultdict(list)
        self.onlyFullFeed = onlyFullFeed

        self.counter = {
                "all": pathCountDict(),
                "origas": defaultdict(pathCountDict),
                }

        self.txtFile = txtFile


    def run(self):
        logging.info("Reading RIB files...")
        self.readrib()
        if self.onlyFullFeed: 
            self.peers = self.findFullFeeds(0.75)
        else:
            self.peers = self.findFullFeeds(0)
        self.peersASN = {p:self.peersASN[p] for p in self.peers} 
        for p, a in self.peersASN.iteritems():
            if len(a)>1:
                logging.warn("(Path counter) peer %s maps to more than one AS (%s)" % (p,a))
                continue
            self.peersPerASN[list(a)[0]].append(p)

        logging.debug("(pathCounter) %s " % self.peersASN)
        self.cleanUnusedCounts()

        logging.info("Reading UPDATE files...")
        if self.startts != self.endts:
            self.readupdates()
        else:
            self.slideTimeWindow(0)

        logging.info("(pathCounter) Finished to read data")


    def nbIPs(self, prefixlen):
        return self.__nbaddr[self.af][prefixlen]


    def findParent(self, node, zOrig):
        parent = node.parent
        if parent is None or parent.prefix == "0.0.0.0/0":
            return None
        elif zOrig in parent.data:
            return parent
        else:
            return self.findParent(parent, zOrig)

    def findFullFeeds(self, threshold):
        # logging.debug("(pathCounter) finding full feed peers...")
        nbPrefixes = defaultdict(int)
        nodes = self.rtree.nodes()

        for node in self.rtree.nodes():
            for peer in node.data.keys():
                nbPrefixes[peer] += 1

        res = set([peer for peer, nbPfx in nbPrefixes.iteritems() if nbPfx>len(nodes)*threshold])
        logging.debug("(pathCounter) Using %s peers out of %s (threshold=%s)" % (len(res), len(nbPrefixes), threshold))

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
        self.ts = ts

        self.countQueue.put( (self.ts, self.peersPerASN, self.counter) )
        self.countQueue.join()
        
        logging.debug("(pathCounter) window slided (ts=%s)" % self.ts)


    def incTotalCount(self, count, peerip, origAS, zAS):
        """Increment the number of ip seen by peerip in total and per origAS."""
        self.counter["all"]["total"][peerip] += count
        self.counter["origas"][origAS]["total"][peerip] += count
        # assert self.counter["origas"][origAS]["total"][peerip] >= 0


    def incCount(self, count, peerip, origAS, peerAS, asns):
        """Increment the number of ip passing through asns in total and for the
        given origAS."""
        for asn in asns:
            self.counter["all"]["asn"][asn][peerip] += count
            self.counter["origas"][origAS]["asn"][asn][peerip] += count

            # assert self.counter["all"]["asn"][asn][peerip] >= 0
            # assert self.counter["origas"][origAS]["asn"][asn][peerip] >= 0


    def readrib(self):
        stream = None
        rec = None
        if self.txtFile is None:
            # create a new bgpstream instance
            stream = BGPStream()

            # create a reusable bgprecord instance
            rec = BGPRecord()
            bgprFilter = "type ribs"

            if self.af == 6:
                bgprFilter += " and ipversion 6"
            else:
                bgprFilter +=  " and ipversion 4"

            for c in self.collectors:
                bgprFilter += " and collector %s " % c

            # if not self.asnFilter is None:
                # bgprFilter += ' and path %s$' % self.asnFilter
            for p in self.includedPeers:
                bgprFilter += " and peer %s " % p

            for p in self.includedPrefix:
                bgprFilter += " and prefix more %s " % p

            
            logging.info("Connecting to BGPstream... (%s)" % bgprFilter)
            logging.info("Timestamps: %s, %s" % (self.startts-3600, self.startts+3600))
            stream.parse_filter_string(bgprFilter)
            stream.add_interval_filter(self.startts-3600, self.startts+3600)
            if self.livemode:
                stream.set_live_mode()

            stream.start()

        else:
            rec = txtReader.txtReader(self.txtFile)

        # for line in p1.stdout: 
        while(self.txtFile and not rec.running ) or (stream and stream.get_next_record(rec)):
            if rec.status  != "valid":
                print rec.project, rec.collector, rec.type, rec.time, rec.status
            zDt = rec.time
            elem = rec.get_next_elem()

            while(elem):
                zOrig = elem.peer_address
                zAS = elem.peer_asn
                if zAS in self.excludedPeers or (len(self.includedPeers) and zAS not in self.includedPeers):
                    elem = rec.get_next_elem()
                    continue
                zPfx = elem.fields["prefix"]
                sPath = elem.fields["as-path"]
                # print("%s: %s, %s, %s" % (zDt, zAS, zPfx, elem.fields))

                if zPfx == "0.0.0.0/0" or zPfx in self.excludedPrefix or (len(self.includedPrefix) and zPfx not in self.includedPrefix):
                    elem = rec.get_next_elem()
                    continue

                path = sPath.split(" ")
                origAS = path[-1]
                if origAS in self.excludedOriginASN or (len(self.includedOriginASN) and origAS not in self.includedOriginASN):
                    elem = rec.get_next_elem()
                    continue
                    # FIXME: this is not going to work in the case of
                    # delegated prefixes (and using IP addresses as spatial
                    # resolution) 

                self.peersASN[zOrig].add(zAS)

                if len(path) < 2:
                    # Ignore paths with only one AS
                    elem = rec.get_next_elem()
                    continue

                node = self.rtree.add(zPfx)
                if zOrig in node.data:
                    # Already read this entry, we should read only one RIB per peer
                    elem = rec.get_next_elem()
                    continue

                if self.ribQueue is not None:
                    self.ribQueue.put( (zDt, zOrig, zAS, zPfx, path ) )

                node.data[zOrig] = {"path": set(path), "count": 0, "origAS":origAS}

                # print "%s, %s, %s, %s, %s" % (elem.time, elem.type, elem.peer_address, elem.peer_asn, elem.fields)

                if self.spatialResolution:
                    # compute weight for this path
                    count = self.nbIPs(node.prefixlen)
                    countBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(zPfx) if zOrig in n.data and n!=node ])
                    count -= countBelow
                    # assert count >= 0
                    node.data[zOrig]["count"] = count

                    # Update above nodes
                    parent = self.findParent(node, zOrig)
                    if not parent is None:
                        # pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if n.parent == parent and zOrig in n.data])
                        pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if zOrig in n.data and n!=parent])
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
                    node.data[zOrig]["count"] = count

                asns = node.data[zOrig]["path"]
                self.incTotalCount(count, zOrig, origAS, zAS)
                self.incCount(count, zOrig, origAS, zAS, asns)
            
                elem = rec.get_next_elem()

    def readupdates(self):
        #TODO implement txt file for update messages?
        if self.txtFile:
            return

        # create a new bgpstream instance
        stream = BGPStream()
        bgprFilter = "type updates"

        if self.af == 6:
            bgprFilter += " and ipversion 6"
        else:
            bgprFilter +=  " and ipversion 4"

        # bgprFilter += " and collector rrc10 "
        for c in self.collectors:
            bgprFilter += " and collector %s " % c


        # if self.asnFilter is not None:
            # # TOFIX filter is now deprecated, we need to have both
            # # announcements and withdrawals
            # bgprFilter += ' and (path %s$ or elemtype withdrawals)' % self.asnFilter
        
        logging.info("Connecting to BGPstream... (%s)" % bgprFilter)
        logging.info("Timestamps: %s, %s" % (self.startts, self.endts))
        stream.parse_filter_string(bgprFilter)
        stream.add_interval_filter(self.startts, self.endts)
        if self.livemode:
            stream.set_live_mode()

        stream.start()
        # for line in p1.stdout: 
        # create a reusable bgprecord instance
        rec = BGPRecord()
        while(stream.get_next_record(rec)):
            if rec.status  != "valid":
                logging.warn("Invalid BGP record: %s, %s, %s, %s, %s" % ( rec.project, rec.collector, rec.type, rec.time, rec.status) )
            zDt = rec.time
            elem = rec.get_next_elem()
            while(elem):
                zOrig = elem.peer_address
                if  zOrig not in self.peers:
                    # no need to update the counts for non-full feed peers
                    elem = rec.get_next_elem()
                    continue

                zAS = elem.peer_asn
                if zAS in self.excludedPeers or (len(self.includedPeers) and zAS not in self.includedPeers):
                    elem = rec.get_next_elem()
                    continue
                zPfx = elem.fields["prefix"]
                if zPfx == "0.0.0.0/0" or zPfx in self.excludedPrefix or (len(self.includedPrefix) and zPfx not in self.includedPrefix):
                    elem = rec.get_next_elem()
                    continue

                msgTs = zDt
                # set first time bin!
                if self.ts is None:
                    self.slideTimeWindow(msgTs)
            
                elif self.ts + self.timeWindow <= msgTs:
                    self.slideTimeWindow(msgTs)

                elif self.ts > msgTs:
                    #Old update, ignore this to update the graph
                    logging.warn("Ignoring old update (peer IP: %s, timestamp: %s, current time bin: %s): %s" % (zOrig, zDt, self.ts, (elem.type, zAS, elem.fields)))
                    elem = rec.get_next_elem()
                    continue

                node = self.rtree.search_exact(zPfx)

                if elem.type == "W":
                    # Withdraw: remove the corresponding node
                    if not node is None and zOrig in node.data:
                        origAS = node.data[zOrig]["origAS"]

                        if self.spatialResolution:
                            count = node.data[zOrig]["count"]
                            # Update count for above node
                            parent = self.findParent(node, zOrig) 
                            if parent is None:
                                self.incTotalCount(-count,  zOrig, origAS, zAS)
                                asns = node.data[zOrig]["path"]
                                self.incCount(-count,  zOrig, origAS, zAS, asns)
                            else:
                                node.data[zOrig]["count"] = 0
                                # Add ips to above node and corresponding ASes
                                # pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if zOrig in n.data and n!=parent])
                                # pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if n.parent == parent and zOrig in n.data])
                                # oldpCount = parent.data[zOrig]["count"]
                                # pCount = self.nbIPs(parent.prefixlen) - pcountBelow
                                # parent.data[zOrig]["count"] = pCount 
                                # pdiff = pCount - oldpCount
                                # assert pdiff==count

                                # Update count for origAS and path from the
                                # parent node
                                porigAS = parent.data[zOrig]["origAS"]
                                pasns= parent.data[zOrig]["path"]
                                self.incCount(count,  zOrig, porigAS, zAS, pasns)
                                self.incTotalCount(count, zOrig, porigAS, zAS)

                                # Update count for withdrawn origAS and path 
                                asns = node.data[zOrig]["path"]
                                self.incCount(-count,  zOrig, origAS, zAS, asns)
                                self.incTotalCount(-count,  zOrig, origAS, zAS)

                        else: 
                            asns = node.data[zOrig]["path"]
                            self.incCount(-1,  zOrig, origAS, zAS, asns)
                            self.incTotalCount(-1,  zOrig, origAS, zAS)

                        del node.data[zOrig]
            
                else:
                # Announce: update counters
                    sPath = elem.fields["as-path"]
                    path = sPath.split(" ")
                    origAS = path[-1]

                    if origAS in self.excludedOriginASN or (len(self.includedOriginASN) and origAS not in self.includedOriginASN):
                        elem = rec.get_next_elem()
                        continue
                        # FIXME: this is not going to work in the case of
                        # delegated prefixes or implicit withdraws

                    if len(path) < 2:
                        # Ignoring paths with only one AS
                        elem = rec.get_next_elem()
                        continue
                    
                    if self.announceQueue is not None:
                        self.announceQueue.put( (zDt, zOrig, zAS, zPfx, path) )

                    # Announce:
                    if node is None or not zOrig in node.data :
                        # Add a new node 

                        node = self.rtree.add(zPfx)
                        if self.spatialResolution:
                            # Compute the exact number of IPs
                            count = self.nbIPs(node.prefixlen)
                            countBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(zPfx) if zOrig in n.data  and n!=node])
                            count -= countBelow
                            # Update the ASes counts
                            node.data[zOrig] = {"path": set(path), "count": count, "origAS": origAS}
                            asns = node.data[zOrig]["path"]
                            self.incCount(count,  zOrig, origAS, zAS, asns)
                            self.incTotalCount(count, zOrig, origAS, zAS)

                            parent = self.findParent(node, zOrig)
                            if not parent is None:
                                # Update above nodes 
                                # print("%s: (%s) %s, %s, %s" % (zDt, elem.type, zAS, zPfx, count))
                                pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if zOrig in n.data and n!=parent ])
                                # pcountBelow = sum([n.data[zOrig]["count"] for n in self.rtree.search_covered(parent.prefix) if n.parent == parent and zOrig in n.data])
                                oldpCount = parent.data[zOrig]["count"]
                                pCount = self.nbIPs(parent.prefixlen) - pcountBelow
                                pdiff = pCount - oldpCount
                                parent.data[zOrig]["count"] = pCount 
                                # print("parent %s: (%s) %s, %s, %s" % (zDt, zAS, parent.prefix, oldpCount, pCount))
                                # print [(n.prefix,n.data[zOrig]["count"]) for n in self.rtree.search_covered(parent.prefix) if zOrig in n.data and n!=parent ]
                                porigAS = parent.data[zOrig]["origAS"]
                                pasns = parent.data[zOrig]["path"]
                                self.incCount(pdiff,  zOrig, porigAS, zAS, pasns)
                                self.incTotalCount(pdiff, zOrig, porigAS, zAS)

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
                        asns = node.data[zOrig]["path"]
                        self.incCount(-count,  zOrig, porigAS, zAS, asns)
                        self.incTotalCount(-count,  zOrig, porigAS, zAS)

                        node.data[zOrig]["path"] = set(path)
                        node.data[zOrig]["origAS"] = origAS
                        asns = node.data[zOrig]["path"]
                        self.incCount(count,  zOrig, origAS, zAS, asns)
                        self.incTotalCount(count,  zOrig, origAS, zAS)

                elem = rec.get_next_elem()

