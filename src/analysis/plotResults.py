import plotter
from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 
import matplotlib as mpl
from matplotlib import pyplot as plt
import numpy as np
from matplotlib.ticker import MaxNLocator


def listDBfiles(ye, months = range(1,13), days = [15] ):

    dbList = []
    for mo in months:
        for da in days:
            date = datetime(ye, mo, da, tzinfo = pytz.utc)
            start = date - timedelta(hours=2)
            end = date + timedelta(hours=2)

            tss = calendar.timegm(start.timetuple())
            tse = calendar.timegm(end.timetuple())

            dbfile="results/results_@bgpstream:%s,%s.sql" % (tss, tse)
            if not os.path.exists(dbfile):
                continue
            dbList.append(dbfile)

    return dbList

def longitudinalHegemony():

    years = range(2002, 2018)

    localGraph = {15169:"Google", 2497:"IIJ", 2500:"WIDE", 2501:"U. Tokyo", 32: "Stanford", 1200: "AMSIX", 3356: "Level(3)", 3549:"Global Crossing", 174:"Cogent", 2914:"NTT America", 54113:"Fastly", 16509:"Amazon", 13335:"Cloudflare", 32934:"Facebook", 13414:"Twitter", 35994:"Akamai", 20940:"Akamai"}


    meanH = []
    medianH = []

    ccmap = mpl.cm.get_cmap('copper_r')
    # Using contourf to provide my colorbar info, then clearing the figure
    Z = [[0,0],[0,0]]
    CS3 = plt.contourf(Z, years, cmap=ccmap)
    plt.clf()

    for yidx, ye in enumerate(years):

        dbList = listDBfiles(ye)
        pr = plotter.Plotter(db=dbList)
        # pr.dataSanityCheck()
        contour=CS3
        if ye!=years[-1]:
            contour=None

        data = pr.hegemonyDistGlobalGraph(1, "results/fig/longitudinalHegemonyDistGlobal.pdf", label="%s" % (ye), color=ccmap(yidx/float(len(years))), contour=contour)
        meanH.append(np.mean(data.values()))
        medianH.append(np.median(data.values()))
        print "%s: mean hegemony=%s" % (ye, meanH[-1])
        print "\t3356 hegemony: %s" % (data[3356])
        print "\t3549 hegemony: %s" % (data[3549])

        for asn, name in localGraph.iteritems():
            data = pr.hegemonyDistLocalGraph(asn, title="AS%s %s" % (asn, name), fignum=asn, filename="results/fig/longitudinalAS%s.pdf" % asn, color=ccmap(yidx/float(len(years))), contour=None)
            if ye == 2017 and asn==15169:
                print [(k,v) for k,v in data.iteritems() if v>0]

    plt.figure()
    plt.plot(years, meanH)
    # plt.plot(years, medianH)
    plt.xlim(2001,years[-1]+1)
    ax = plt.axes()
    # ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xticklabels(years, rotation=45)
    plt.ylabel("Mean AS Hegemony")
    plt.xlabel("Time")
    plt.tight_layout()
    plt.savefig("results/fig/meanHegemonyEvolution.pdf")

def localGraphNbnodeDist():
    years = [2017] #range(2002,2018)

    if len(years)>1:
        ccmap = mpl.cm.get_cmap('copper_r')
        # Using contourf to provide my colorbar info, then clearing the figure
        Z = [[0,0],[0,0]]
        CS3 = plt.contourf(Z, years, cmap=ccmap)
        plt.clf()
    
    for yidx, ye in enumerate(years):
        dbList = listDBfiles(ye)
        pr = plotter.Plotter(db=dbList)
        if len(years)>1:
            contour=CS3
            if ye!=years[-1]:
                contour=None

            data = pr.nbNodeDistLocalGraph(2, filename="results/fig/localGraphNbNodesCDF_%s.pdf" % ye, labelNoZero=ye, color=ccmap(yidx/float(len(years))))
        else:
            data = pr.nbNodeDistLocalGraph(2, filename="results/fig/localGraphNbNodesCDF_%s.pdf" % ye, labelNoZero=ye)

        print ye
        # print "\tMean number of nodes in local graphs: %s (std=%s, median=%s)" % (np.mean(data["all"].values()), np.std(data["all"].values()), np.median(data["all"].values()))
        print "\tMean number of nodes in local graphs (non-null hegemony): %s (std=%s, median=%s)" % (np.mean(data["noZero"].values()), np.std(data["noZero"].values()), np.median(data["noZero"].values()))

        # ASN with no transit nodes
        notransit = [(k,v) for k,v in data["noZero"].iteritems() if v<=1]
        print "\t %s ASN with less than one transit nodes:" % len(notransit)
        for asn, nnode in notransit:
            print "\t\tAS%s %s transit node" % (asn, nnode) 


        # ASN with many transit nodes
        manytransit = [(k,v) for k,v in data["noZero"].iteritems() if v>50]
        print "\t %s ASN with more than 50 transit nodes:" % len(manytransit)
        for asn, nnode in manytransit:
            print "\t\tAS%s %s transit nodes" % (asn, nnode) 


        print data["noZero"][15169]

def localGraphNullHegemony():

    pass

if __name__ == "__main__":
    # plot everything

    localGraphNbnodeDist()
    longitudinalHegemony()


