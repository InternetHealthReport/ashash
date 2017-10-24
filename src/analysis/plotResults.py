import plotter
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import calendar
import os
import pytz 
import matplotlib as mpl
from matplotlib import pyplot as plt
import numpy as np
from matplotlib.ticker import MaxNLocator

import networkx as nx
import json
import sys


resultsDirectory = "resultsv4/"
# resultsDirectory = "results/"
# resultsDirectory = "resultsv6/"

def listFiles(prefix, suffix, ye, months = range(1,13), days = [15] ):

    fileList = []
    for mo in months:
        for da in days:
            date = datetime(ye, mo, da, tzinfo = pytz.utc)
            start = date - timedelta(hours=2)
            end = date + timedelta(hours=2)

            tss = calendar.timegm(start.timetuple())
            tse = calendar.timegm(end.timetuple())

            filename=resultsDirectory+"%s_@bgpstream:%s,%s.%s" % (prefix, tss, tse, suffix)
            if not os.path.exists(filename):
                continue
            fileList.append(filename)

    return fileList


def computeLongitudinalSmallCoefficient(ye):
    # years = range(2004, 2018)
    y = []
    # for ye in years:
    print ye
    graphFiles = listFiles("asgraph","txt",ye)
    
    coeff = []
    for fi  in graphFiles:
        g = nx.read_adjlist(fi)
        print "(%s) graph loaded" % ye
        coeff.append(nx.algorithms.smallworld.omega(g))
        print "(%s) %s" % (ye,coeff)

    # y.append(np.mean(coeff))

    return coeff
    # plt.figure()
    # plt.plot(years, y )
    # plt.ylabel(Small-coefficient)
    # plt.tight_layout()
    # plt.savefig(resultsDirectory+"fig/smallworld.pdf")



def longitudinalHegemony():

    years = range(2004, 2018)

    localGraph = {6939:"Hurricane Electric", 8075:"Microsoft", 15169:"Google", 
            2497:"IIJ", 2500:"WIDE", 2501:"U. Tokyo", 32: "Stanford", 
            1200: "AMSIX", 3356: "Level(3)", 3549:"Global Crossing", 
            174:"Cogent", 2914:"NTT America", 54113:"Fastly", 16509:"Amazon", 
            13335:"Cloudflare", 32934:"Facebook", 13414:"Twitter", 
            35994:"Akamai", 20940:"Akamai", 15133:"EdgeCast CDN",
            # DNS root servers
            26415: "A,J Root", 394353: "B Root", 2149: "C Root", 27: "D Root", 
            21556:"E Root", 3557:"F Root", 5927:"G Root", 1508:"H Root", 
            29216:"I Root", 25152:"K Root", 20144:"L Root", 7500:"M Root"}
    globalHege = {3356:("Level(3)",[],[]), 7018:("ATT",[],[]), 3257:("GTT",[],[]), 
            4134:("ChinaNet",[],[]), 174:("Cogent",[],[]), 2914:("NTT",[],[]), 
            6939:("HE",[],[]), 1299:("Telia",[],[]), 2497:("IIJ",[],[])} #3549:("GlbX", [],[]), 

    meanH = []
    medianH = []
    if os.path.exists(resultsDirectory+"rootData.json"):
        print("loading rootdata.json")
        rootdata = json.load(open(resultsDirectory+"rootData.json", "r"))
        transitPerAsn = {k: set([asn for ye, topasn in v.iteritems() for asn, hege in topasn]) for k,v in rootdata.iteritems()}
        rootdata = defaultdict(dict)
        print transitPerAsn
    else:
        rootdata = defaultdict(dict)
        transitPerAsn = None

    fig = None
    ax = None
    axins = None

    ccmap = mpl.cm.get_cmap('copper_r')
    # Using contourf to provide my colorbar info, then clearing the figure
    Z = [[0,0],[0,0]]
    CS3 = plt.contourf(Z, years, cmap=ccmap)
    plt.clf()
    
    for yidx, ye in enumerate(years):

        dbList = listFiles("results","sql",ye)
        pr = plotter.Plotter(db=dbList)
        # pr.dataSanityCheck()
        contour=CS3
        if ye!=years[-1]:
            contour=None

        subfig = False
        if resultsDirectory=="resultsv4/":
            subfig = True

        data, yval, fig, ax, axins = pr.hegemonyDistGlobalGraph(1, resultsDirectory+"fig/longitudinalHegemonyDistGlobal.pdf", label="%s" % (ye), color=ccmap(yidx/float(len(years))), contour=contour, subfig=subfig, fig=fig, ax=ax, axins=axins)
        meanH.append(np.mean(data.values()))
        medianH.append(np.median(data.values()))
        if len(data):
            maxasn = max(data, key=data.get)
            print "%s: mean hegemony=%s, max=%s (AS%s)" % (ye, meanH[-1], data[maxasn], maxasn) 

        for asn in globalHege.keys():
            if asn in data: 
                print "\t%s hegemony: %s" % (asn, data[asn]);
                globalHege[asn][1].append(data[asn])
                globalHege[asn][2].append(yval[data[asn]])

        # if ye == 2017:
            # print "-------"
            # for asn, h in [(asn,h) for asn,h in  data.iteritems() if h > 0.05]:
                # print "\t%s hegemony: %s" % (asn, h)

        for asn, name in localGraph.iteritems():
            data = pr.hegemonyDistLocalGraph(asn, title="", fignum=asn, filename=resultsDirectory+"fig/longitudinalAS%s.pdf" % asn, color=ccmap(yidx/float(len(years))), contour=None)
            # if ye == 2017 or ye == 2016 :
            if asn==15169:
                print("Google")
                print [(k,v) for k,v in data.iteritems() if v>0.001]
            # if asn==20940:
                # print("Akamai")
                # print [(k,v) for k,v in data.iteritems() if v>0.01]
            if name.endswith("Root"):
                if transitPerAsn is None:
                    rootdata[name][ye]= [(k,v) for k,v in data.iteritems() if v>0.05]
                else:
                    rootdata[name][ye]= [(k,v) for k,v in data.iteritems() if k in transitPerAsn[name]]


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
    plt.savefig(resultsDirectory+"fig/meanHegemonyEvolution.pdf")

    plt.figure(figsize=(8,3))
    # mk = [".", "^", "<", ">", "v", "s", "x", "d", "p"]
    for i, (asn, values) in enumerate(globalHege.iteritems()):
        plt.plot(years, values[1], label="%s (%s)" % (values[0], asn))
    plt.legend(ncol=4)
    plt.ylim([0, 0.25])
    plt.ylabel("AS hegemony")
    plt.xlabel("Time")
    plt.tight_layout()
    plt.savefig(resultsDirectory+"fig/tier1Hegemony.pdf")

    json.dump(rootdata, open(resultsDirectory+"rootData.json", "w"))
    print json.dumps(rootdata)

def localGraphNbnodeDist():
    years = [2017] #range(2002,2018)

    if len(years)>1:
        ccmap = mpl.cm.get_cmap('copper_r')
        # Using contourf to provide my colorbar info, then clearing the figure
        Z = [[0,0],[0,0]]
        CS3 = plt.contourf(Z, years, cmap=ccmap)
        plt.clf()
    
    for yidx, ye in enumerate(years):
        dbList = listFiles("results", "sql", ye)
        print dbList
        pr = plotter.Plotter(db=dbList)
        if len(years)>1:
            contour=CS3
            if ye!=years[-1]:
                contour=None

            data = pr.nbNodeDistLocalGraph(2, filename=resultsDirectory+"fig/localGraphNbNodesCDF_%s.pdf" % ye, labelNoZero=ye, color=ccmap(yidx/float(len(years))))
        else:
            data = pr.nbNodeDistLocalGraph(2, filename=resultsDirectory+"fig/localGraphNbNodesCDF_%s.pdf" % ye, labelNoZero=ye)

        print ye
        # print "\tMean number of nodes in local graphs: %s (std=%s, median=%s)" % (np.mean(data["all"].values()), np.std(data["all"].values()), np.median(data["all"].values()))
        print "\tMean number of nodes in local graphs (non-null hegemony): %s (std=%s, median=%s)" % (np.mean(data["noZero"].values()), np.std(data["noZero"].values()), np.median(data["noZero"].values()))

        # ASN with no transit nodes
        notransit = [(k,v) for k,v in data["noZero"].iteritems() if v<1]
        print "\t %s ASN with less than one transit nodes:" % len(notransit)
        for asn, nnode in notransit:
            print "\t\tAS%s %s transit node" % (asn, nnode) 


        # ASN with many transit nodes
        manytransit = [(k,v) for k,v in data["noZero"].iteritems() if v>25]
        print "\t %s ASN with more than 50 transit nodes:" % len(manytransit)
        for asn, nnode in manytransit:
            print "\t\tAS%s %s transit nodes" % (asn, nnode) 

        print data["noZero"][15169]

def localGraphTransitEvolution(scope, name, dbList=None, years=[2017]):
    if dbList is None:
        dbList = []
        for ye in years:
            dbList.extend(listFiles("results", "sql", ye))
        pr = plotter.Plotter(db=dbList)
        pr.hegemonyEvolutionLocalGraph(scope, fileDate=True)

    else:
        pr = plotter.Plotter(db=dbList)
        pr.hegemonyEvolutionLocalGraph(scope)

    # plt.title(name+"\n\n")
    # plt.tight_layout()
    plt.savefig(resultsDirectory+"fig/%s_AS%s_transitEvolution.pdf" % (name.replace(" ", "_"), scope))

def localGraphNullHegemony():

    pass

if __name__ == "__main__":
    print sys.argv
    if len(sys.argv) > 1:
        ye = sys.argv[1]
        # plot everything
        smallCoeff = computeLongitudinalSmallCoefficient(int(ye))
        json.dump(smallCoeff, open(resultsDirectory+"smallCoeff_%s.json" % ye,"w"))

    else:
        # localGraphTransitEvolution(4713, dbList= ["results/GoogleLeak_20170825new/results_@bgpstream:1503615600,1503622801.sql"])
        # root_servers = {26415: "AJ Root", 394353: "B Root", 2149: "C Root", 27: "D Root", 
            # 21556:"E Root",42:"E Root", 3557:"F Root", 5927:"G Root", 1508:"H Root", 
            # 29216:"I Root", 25152:"K Root", 20144:"L Root", 7500:"M Root"}
        # for asn, name in root_servers.iteritems(): 
            # localGraphTransitEvolution(asn, name)
        # localGraphTransitEvolution(25152, years=range(2004,2018))
        # localGraphNbnodeDist()
        longitudinalHegemony()



