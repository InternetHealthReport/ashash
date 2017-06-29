from matplotlib import pylab as plt
import sqlite3
import numpy as np

def ecdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, yvals, **kwargs )


def eccdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, 1-yvals, **kwargs )



class PlotResults(object):

    """Read results from the sqlite database and plot interesting stuff. """

    def __init__(self, db="results/ashash_results.sql"):
        self.conn = sqlite3.Connection(db) 
        self.cursor = self.conn.cursor()


    def hegemonyDistLocalGraph(self):
        """Plot the distribution of AS hegemony for all local graphs"""

        plt.figure(100)

        # All local graphs
        data = self.cursor.execute("SELECT hege FROM hegemony WHERE ts=0 AND scope!=0 AND scope!=asn").fetchall()
        data = [x[0] for x in data]
        eccdf(data, label="All graphs")

        # Google
        data = self.cursor.execute("SELECT hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=15169").fetchall()
        data = [x[0] for x in data]
        eccdf(data, label="Google")

        # AMSIX
        data = self.cursor.execute("SELECT hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=1200").fetchall()
        data = [x[0] for x in data]
        eccdf(data, label="AMS-IX")

        # UT
        data = self.cursor.execute("SELECT hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=2501").fetchall()
        data = [x[0] for x in data]
        eccdf(data, label="U. Tokyo")

        # Stanford
        data = self.cursor.execute("SELECT hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=32").fetchall()
        data = [x[0] for x in data]
        eccdf(data, label="Stanford")

        plt.xlabel("AS hegemony")
        plt.ylabel("CDF")
        plt.tight_layout()
        # plt.xscale("log")
        plt.yscale("log")
        # plt.xlim([10e-8, 1])
        plt.legend(loc="best")

        plt.savefig("results/fig/hegemonyDistLocalGraph.pdf")


    def hegemonyDistGlobalGraph(self):
        """Plot the distribution of AS hegemony for the global graph"""

        plt.figure(101)

        data = self.cursor.execute("SELECT hege FROM hegemony WHERE ts=0 AND scope==0").fetchall()
        data = [x[0] for x in data]
        eccdf(data, label="Global Graph")

        plt.xlabel("AS hegemony")
        plt.ylabel("CDF")
        plt.tight_layout()
        # plt.xscale("log")
        plt.yscale("log")
        # plt.xlim([10e-8, 1])
        plt.legend(loc="best")

        plt.savefig("results/fig/hegemonyDistGlobalGraph.pdf")


    def nbNodeDistLocalGraph(self):
        """Plot the distribution of the number of nodes in the local graphs"""

        plt.figure(201)

        data = self.cursor.execute("SELECT count(*) FROM hegemony WHERE ts=0 AND scope!=asn AND scope!=0 group by scope").fetchall()
        data = [x[0] for x in data]
        ecdf(data, label="All Nodes")

        data = self.cursor.execute("SELECT count(*) FROM hegemony WHERE ts=0 AND scope!=asn AND scope!=0 and hege!=0 group by scope").fetchall()
        data = [x[0] for x in data]
        ecdf(data, label="$\mathcal{H}>0$")

        plt.xlabel("Number of nodes")
        plt.ylabel("CDF")
        plt.tight_layout()
        # plt.xscale("log")
        # plt.yscale("log")
        # plt.xlim([10e-8, 1])
        plt.legend(loc="best")

        plt.savefig("results/fig/nbNodeDistLocalGraph.pdf")


if __name__ == "__main__":
    # plot everything
    pr = PlotResults(db="results/results_@bgpstream:1152914400,1152928800.sql")

    pr.hegemonyDistLocalGraph()
    pr.hegemonyDistGlobalGraph()
    pr.nbNodeDistLocalGraph()


