from matplotlib import pylab as plt
import sqlite3
import numpy as np
from collections import defaultdict


def ecdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, yvals, **kwargs )


def eccdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, 1-yvals, **kwargs )

    return {k:v for k,v in zip(sorted, 1-yvals)}



class Plotter(object):

    """Read results from the sqlite database and plot interesting stuff. """

    def __init__(self, db="results/ashash_results.sql"):
        if not isinstance(db,list):
            db = [db]

        self.cursor = []
        for d in db:
            conn = sqlite3.Connection(d) 
            self.cursor.append(conn.cursor())


    def dataSanityCheck(self):

        for cursor in self.cursor:

            data=cursor.execute("SELECT * FROM hegemony where hege>1").fetchall()
            if len(data) > 0:
                print "Error: database contains %s entries with hegemony scores > 1" % len(data)

            data=cursor.execute("SELECT * FROM hegemony where hege<0").fetchall()
            if len(data) > 0:
                print "Error: database contains %s entries with  hegemony scores < 0" % len(data)

            data=cursor.execute("SELECT * FROM hegemony where scope=asn and hege!=1").fetchall()
            if len(data) > 0:
                print "Error: database contains %s entries with hege!=1 for scope=asn" % len(data)



    def avgData(self, req):
        alldata = defaultdict(list)
        for c in self.cursor:
            data = c.execute(req).fetchall()
            for x in data:
                alldata[x[0]].append(x[1])

        return {k:np.mean(v) for k, v in alldata.iteritems()}


    def hegemonyDistLocalGraph(self, asn, label=None, title=None, fignum=13, filename="results/fig/hegemonyDistLocalGraph.pdf", color=None, contour=None):
        """Plot the distribution of AS hegemony for all local graphs"""

        plt.figure(fignum)

        # All local graphs
        # data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=0 AND scope!=asn")
        # eccdf(data.values(), label="All graphs")

        # Google
        # data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=15169")
        # eccdf(data.values(), label="Google")

        # AMSIX
        # data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=1200")
        # eccdf(data.values(), label="AMS-IX")

        # UT
        # data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=2501")
        # eccdf(data.values(), label="U. Tokyo")

        # Stanford
        # data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=32")
        # eccdf(data.values(), label="Stanford")

        data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=%s" % asn)
        if color is None:
            eccdf(data.values(), label=label)
        else:
            eccdf(data.values(), label=label, c=color)

        plt.xlabel("AS hegemony")
        plt.ylabel("CCDF")
        plt.title(title)
        plt.tight_layout()
        # plt.xscale("log")
        plt.yscale("log")
        plt.xlim([-0.02, 1.02])
        # plt.legend(loc="best")
        if not contour is None:
            plt.colorbar(contour)

        plt.savefig(filename)

        return data


    def hegemonyDistGlobalGraph(self, fignum=10, filename="results/fig/hegemonyDistGlobalGraph.pdf", label="Global Graph", color=None, contour=None):
        """Plot the distribution of AS hegemony for the global graph"""

        plt.figure(fignum)
        data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope==0")
        if color is None:
            yval = eccdf(data.values(), label=label)
        else:
            yval = eccdf(data.values(), label=label, c=color)

        plt.xlabel("AS hegemony")
        plt.ylabel("CCDF")
        # plt.xscale("log")
        plt.yscale("log")
        # plt.xlim([0, 0.25])

        if not contour is None:
            plt.colorbar(contour)

        plt.tight_layout()
        plt.savefig(filename)

        return data, yval


    def nbNodeDistLocalGraph(self, fignum=11, allNodes=False, noZeroNodes=True, filename="results/fig/nbNodeDistLocalGraph.pdf", labelNoZero="$\mathcal{H}>0$", color=None):
        """Plot the distribution of the number of nodes in the local graphs"""

        plt.figure(fignum)

        dataAll = None
        if allNodes:
            dataAll = self.avgData("SELECT scope, count(*) FROM hegemony WHERE ts=0 AND scope!=asn AND scope!=0 and scope in (select scope from hegemony where asn=scope and hege=1) group by scope")
            if color is None:
                ecdf(dataAll.values(), label="All Nodes")
            else:
                ecdf(dataAll.values(), label="All Nodes", c=color)
        
        dataNoZero = None
        if noZeroNodes:
            dataNoZero = self.avgData("SELECT scope, sum(case when hege>0.001 then 1 else 0 end) FROM hegemony WHERE ts=0 AND scope!=asn AND scope!=0 and scope in (select scope from hegemony where asn=scope and hege=1) group by scope")
            if color is None:
                ecdf(dataNoZero.values(), label=labelNoZero)
            else:
                ecdf(dataNoZero.values(), label=labelNoZero, c=color)

        # print "median number of nodes (H>0) %s" % np.median(data.values())
        # print "mean number of nodes (H>0) %s (std=%s)" % (np.mean(data.values()), np.std(data.values()))

        if not allNodes and noZeroNodes:
            plt.xlabel("Number of transit nodes")
        else:
            plt.xlabel("Number of nodes")
        plt.ylabel("CDF")
        plt.tight_layout()
        plt.xscale("log")
        # plt.yscale("log")
        # plt.xlim([10e-8, 1])
        # plt.legend(loc="best")

        plt.savefig(filename)

        return {"all":dataAll, "noZero":dataNoZero}


if __name__ == "__main__":
    # plot everything
    pr = Plotter(db="results/results_@bgpstream:1152914400,1152928800.sql")

    pr.hegemonyDistLocalGraph()
    pr.hegemonyDistGlobalGraph()
    pr.nbNodeDistLocalGraph()


