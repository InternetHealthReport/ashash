import os
import networkx as nx
import glob
import sqlite3
from collections import defaultdict

bnthresh = 0.1 # bottleneck threshold
dbfiles = glob.glob("results/results_@bgpstream:14*.sql")

for f in dbfiles[:]:
    print f
    conn = sqlite3.connect(f)
    cursor = conn.cursor()

    bns = cursor.execute("select scope, asn, max(hege) from hegemony where scope!=asn and scope!=0 group by scope")
    asgraphfile = f.replace("results", "asgraph").replace("sql","txt").replace("asgraph","results",1)
    if not os.path.exists(asgraphfile):
        continue

    asgraph = nx.read_adjlist(asgraphfile)
    print "%s: %s nodes" % (asgraphfile, len(asgraph))
    nbnab = defaultdict(int)
    unihome = 0
    nbab = 0

    for bn in bns:
        scope = str(bn[0])
        asn = str(bn[1])
        hege = bn[2]

        if hege<bnthresh:
            continue
       
        if not scope in asgraph:
            continue

        if len(asgraph[scope])==1:
            unihome += 1
        elif asn in asgraph[scope] :
            # the asn with the highest hegemony score is adjacent to the origin AS
            # print "adjacent bottleneck scope=%s, asn=%s, hege=%s" % bn
            nbab += 1
        else:
            # print "non-adjacent bottleneck scope=%s, asn=%s, hege=%s" % bn
            splen = nx.shortest_path_length(asgraph, scope, asn)
            nbnab[splen] += 1
            if splen>3:
                print "far away: scope=%s, asn=%s, splen=%s, path: %s" % (scope, asn, splen, list(nx.shortest_paths.all_shortest_paths(asgraph, scope, asn)))

    print "Number of uni-homed: %s" % unihome
    print "Number of adjacent bottleneck: %s" % nbab
    print "Number of non-adjacent bottleneck: %s" % nbnab
