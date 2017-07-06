import threading
import logging
import networkx as nx


class asGraph(threading.Thread):
    """Read bgp messages from a queue and constuct the corresponding AS graph. """

    def __init__(self, bgpQueue):
        threading.Thread.__init__(self)

        self.bgpQueue = bgpQueue
        self.asgraph = nx.Graph()
        self.daemon = True


    def run(self):
        while True:
            msg = self.bgpQueue.get()
            self.addPath(msg)


    def addPath(self, msg):
        zTd, zDt, zS, zOrig, zAS, zPfx, path, zOther = msg

        for i, as0 in enumerate(path[:-1]):
            self.asgraph.add_edge(as0, path[i+1])


    def saveGraph(self, filename):
        nx.write_adjlist(self.asgraph, filename)
