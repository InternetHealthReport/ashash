import sys
import networkx as nx

filename = sys.argv[1]
G = nx.read_adjlist(filename)
nx.write_gml(G, filename+".gml")

