import sys
import re
import json
import networkx as nx

# Load the adjency matrix
filename = sys.argv[1]
if len(sys.argv)>2:
    asOrigin = sys.argv[2]
G = nx.read_adjlist(filename)
nx.write_gml(G, filename.replace(".txt",".gml"))

# Retrieve peer ASN
peersLine = None
logfilename = filename.replace(".txt", ".log").replace("asgraph","log")
with open(logfilename) as logfile:
    for line in logfile:
        if "{" in line:
            peersLine = line
peers = peersLine.rpartition(" (pathCounter) ")[2]
peers = eval(peers)
peersSet = set([str(e) for v in peers.values() for e in v])

for p in peersSet:
    if p in G:
        G.node[p]["color"] = "#0000FF"

if asOrigin in G:
    G.node[asOrigin]["color"] = "#FF0000"

# TODO add hegemony score 

# TODO add non-full feed peers?


# Write the graph in gml format
nx.write_gml(G, filename+".gml")
