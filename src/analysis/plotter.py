import os
from matplotlib import pylab as plt
import itertools
import sqlite3
import numpy as np
from collections import defaultdict
from datetime import datetime
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset
import matplotlib.dates as mdates
import networkx as nx

def ecdf(a, ax=None, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    if ax is None:
        plt.plot( sorted, yvals, **kwargs )
    else:
        ax.plot( sorted, yvals, **kwargs )


def eccdf(a, ax=None, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    if ax is None:
        plt.plot( sorted, 1-yvals, **kwargs )
    else:
        ax.plot( sorted, 1-yvals, **kwargs )

    return {k:v for k,v in zip(sorted, 1-yvals)}



class Plotter(object):

    """Read results from the sqlite database and plot interesting stuff. """

    def __init__(self, db="results/ashash_results.sql"):
        if not isinstance(db,list):
            db = [db]

        self.dbfiles = db
        self.cursor = []
        for d in db:
            if os.path.getsize(d) > 100000:
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



    def avgData(self, req, appearance=False):
        alldata = defaultdict(list)
        nbDates = float(len(self.cursor))
        for c in self.cursor:
            data = c.execute(req).fetchall()
            for x in data:
                alldata[x[0]].append(x[1])

        if appearance :
            # res = {k:np.sum(v)/float(appearance[str(k)]) for k, v in alldata.iteritems() if appearance[str(k)]>nbDates/2 }
            res = {k:np.mean(v) for k, v in alldata.iteritems()}
        else:
            # if "scope=15169" in req:
                # print alldata
            res = {k:np.sum(v)/nbDates for k, v in alldata.iteritems()}

        # print "mean nodes: %s" % np.mean(res.values())
        return res

    def hegemonyDistLocalGraph(self, asn, label=None, title=None, fignum=13, filename="hegemonyDistLocalGraph.pdf", color=None, contour=None):
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

        data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope!=asn AND scope=%s AND expid=1" % asn)
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
        plt.xlim([-0.02, 0.5])
        plt.ylim([0.002, 1.1])
        # plt.legend(loc="best")
        if not contour is None:
            plt.colorbar(contour)

        plt.savefig(filename)

        return data


    def hegemonyDistGlobalGraph(self, fignum=10, filename="hegemonyDistGlobalGraph.pdf", label="Global Graph", color=None, contour=None, subfig=False, fig=None, ax=None, axins=None):
        """Plot the distribution of AS hegemony for the global graph"""

        if fig is None:
            fig, ax = plt.subplots(num=fignum)

        data = self.avgData("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope=0 AND expid=1")
        if color is None:
            yval = eccdf(data.values(), label=label, ax=ax)
        else:
            yval = eccdf(data.values(), label=label, c=color, ax=ax)

        if subfig:
            if axins is None:
                axins=zoomed_inset_axes(ax, 1.8, loc=1)
            if color is None:
                yval = eccdf(data.values(), label=label, ax=axins)
            else:
                yval = eccdf(data.values(), label=label, c=color, ax=axins)
            axins.set_xlim(0.0, 0.2)
            axins.set_ylim(10e-6, 10e-4)
            axins.set_yscale("log")
            plt.yticks(visible=False)
            plt.xticks(visible=False)
            mark_inset(ax, axins, loc1=2, loc2=4, fc="none", ec="0.3")

        ax.set_xlabel("AS hegemony")
        ax.set_ylabel("CCDF")
        # plt.xscale("log")
        ax.set_yscale("log")
        ax.set_xlim(-0.02, 0.5)
        ax.set_ylim(10e-6, 1.1)

        if False and not contour is None and not subfig:
            fig.colorbar(contour, orientation='horizontal' )

        fig.tight_layout()
        fig.savefig(filename)

        return data, yval, fig, ax, axins


    def nbNodeDistLocalGraph(self, fignum=11, allNodes=False, noZeroNodes=True, filename="nbNodeDistLocalGraph.pdf", labelNoZero="$\mathcal{H}>0$", color=None):
        """Plot the distribution of the number of nodes in the local graphs"""

        plt.figure(fignum)

        # count the number of times ASN appeared in the data:
        appearance = defaultdict(int)
        for f in self.dbfiles:
            graphfile = f.replace("results_", "asgraph_").replace("sql", "txt")
            g = nx.read_adjlist(graphfile)

            for node in set(g.nodes):
                if node=="65200":
                    print graphfile
                appearance[node]+=1

        dataAll = None
        if allNodes:
            # dataAll = self.avgData("SELECT scope, count(*) FROM hegemony WHERE expid=1 AND ts=0 AND scope!=asn AND scope!=0 and scope in (select scope from hegemony where expid=1 and asn=scope and hege=1) group by scope")
            dataAll = self.avgData("SELECT scope, count(*) FROM hegemony WHERE expid=1 AND ts=0 AND scope!=0  group by scope", True)
            if color is None:
                ecdf(dataAll.values(), label="All Nodes")
            else:
                ecdf(dataAll.values(), label="All Nodes", c=color)
        
        dataNoZero = None
        if noZeroNodes:
            dataNoZero = self.avgData("SELECT scope, sum(case when hege>0.01 then 1 else 0 end) FROM hegemony WHERE expid=1 AND ts=0 AND scope!=asn AND scope!=0 and scope in (select scope from hegemony where expid=1 and asn=scope and hege=1) group by scope", True)
            # dataNoZero = self.avgData("SELECT scope, sum(case when hege>0.001 then 1 else 0 end) FROM hegemony WHERE expid=1 AND ts=0 AND scope!=0 group by scope", appearance)
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

    def hegemonyEvolutionLocalGraph(self, scope, filename="AS%s_hegeEvolution.pdf", fileDate=False, expid=1):

        filename = filename % scope
        hege = defaultdict(lambda: defaultdict(list))
        marker = itertools.cycle(('^', '.', 'x', '+', 'v','*'))
        color = itertools.cycle(('C1', 'C0', 'C2', 'C4', 'C3'))
        # fig = plt.figure(figsize=(5,2.5))
        fig = plt.figure()
        ax = plt.subplot()
        for cursor_id, cursor in enumerate(self.cursor):
            data=cursor.execute("SELECT ts, asn, hege  FROM hegemony where expid=%s and hege>0 and scope=%s and asn!=scope order by ts" % (expid, scope))
            
            for ts, asn, h in data:
                if ts==0 :
                    if fileDate:
                        ts = int(self.dbfiles[cursor_id].rpartition(",")[2].partition(".")[0])
                    else:
                        continue
                xval = datetime.utcfromtimestamp(ts)
                hege[asn]["ts"].append(xval)
                hege[asn]["hege"].append(h)

        for asn, data in hege.iteritems():
            # plt.plot(data["ts"], data["hege"], marker=marker.next(), color=color.next(), label=str(asn))
            plt.plot(data["ts"], data["hege"], marker=marker.next(), label=str(asn))

        # plt.yscale("log")
        plt.ylabel("AS hegemony")
        # plt.xlabel("Time")
        plt.ylim([0.0, 1.05])
        plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.2), fontsize=8 )
        # if len(self.dbfiles)==1:
            # myFmt = mdates.DateFormatter('%H:%M')
            # ax.xaxis.set_major_formatter(myFmt)
        fig.autofmt_xdate() 
        # plt.tight_layout()
        plt.savefig(filename)



if __name__ == "__main__":
    # plot everything
    pr = Plotter(db="results/results_@bgpstream:1152914400,1152928800.sql")

    # pr.hegemonyDistLocalGraph()
    pr.hegemonyDistGlobalGraph()
    # pr.nbNodeDistLocalGraph()


