import os
from subprocess import Popen, PIPE
import datetime
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pylab as plt
import glob
import json
import cPickle as pickle
import ashash
import numpy as np
import errno
import scipy

def ecdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, yvals, **kwargs )


def tableResults(path, newFormat=True):
    for filename in glob.glob(path):
        for line in open(filename,"r"):

            time = line.partition("|")[0]
            anomalies = line.rpartition("|")[2]
            anomalies = anomalies.replace("(", "[").replace(")", "]").replace("'", '"')
            anomalies = json.loads(anomalies)

            if len(anomalies):
                for ano in anomalies:
                    if ano[1]>11:
                        print time, ano, filename
            


def extractData(start, end, monitoredAS=None, filterBGPmsg=None):

    #set filenames
    output = "../results/studyCases/AS{ma}_{ye}{mo:02d}{da:02d}T{ho:02d}{mi:02d}/".format(ma=monitoredAS, ye=start.year, mo=start.month, da=start.day, ho=start.hour, mi=start.minute)
    dataDirectory = "/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/{ye}.{mo:02d}/".format(ye=start.year, mo=start.month)
    ribfile = dataDirectory+"RIBS/rib.{ye}{mo:02d}{da:02d}.{ho:02d}00.bz2".format(ye=start.year, mo=start.month, da=start.day, ho=start.hour)
    updatefiles = []
    d = start
    while d < end:
        updatefiles.append(dataDirectory+"UPDATES/updates.{ye}{mo:02d}{da:02d}.{ho:02d}{mi:02d}.bz2".format(ye=d.year, mo=d.month, da=d.day, ho=d.hour, mi=d.minute))
        d += datetime.timedelta(minutes=15)


    #make the graph before and after the event
    if monitoredAS is None:
        params = ["python2", "ashash.py", "--plot", ribfile]
    else:
        params = ["python2", "ashash.py", "-f "+str(monitoredAS), "--plot", ribfile]

    if len(updatefiles)==0:
        updatefiles.append("None")

    params.extend(updatefiles)
    params.append(output)
    p1 = Popen(params)


    #retrieve BGP messages if filter is given
    if not filterBGPmsg is None:
        outFile = open(output+"bgpmessages_%s.txt" % filterBGPmsg, "w")
        for fi in updatefiles:
            print fi
            p2 = Popen(["bgpdump", "-m", "-v", fi], stdout=PIPE) 
            p3 = Popen(["grep",str(filterBGPmsg)], stdin=p2.stdout, stdout=outFile)
            p3.wait()

    p1.wait()

def longStats(filter=None):
    dataDirectory = "/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/"
    af = 4
    space = 1
    yearRange = range(2004, 2017)
    monthRange = range(1,13)
    day = 15

    tier1 = {"3356":[], "1299":[], "174":[] ,"2914":[],"3257":[], "6453":[], "3491":[], "701":[], "1239":[], "6762":[]}

    # Find the first RIB files for each year
    ribFiles = []
    for ye in yearRange:
        for month in monthRange:
            ribs = glob.glob(dataDirectory+"{ye}.{mo:02d}/RIBS/rib.{ye}{mo:02d}{da:02d}.*.bz2".format(ye=ye, mo=month, da=day))
            ribs.sort()
            ribFiles.append(((ye,month,day),ribs[0]))

    outDir = "../results/longStats_space%s_ipv%s/" % (space, af)
    try:
        os.makedirs(os.path.dirname(outDir))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

    plt.figure()
    ccmap = mpl.cm.get_cmap('copper_r')
    # Using contourf to provide my colorbar info, then clearing the figure
    Z = [[0,0],[0,0]]
    CS3 = plt.contourf(Z, yearRange, cmap=ccmap)
    plt.clf()
    for i, (date, ribFile) in enumerate(ribFiles):
        if filter is None:
            centralityFile = outDir+"/%s%02d%02d.pickle" % (date[0], date[1], date[2])
        else:
            centralityFile = outDir+"/%s%02d%02d_AS%s.pickle" % (date[0], date[1], date[2],filter)

        if not os.path.exists(centralityFile):
            rtree, _ = ashash.readrib(ribFile, space, af) 
            asAggProb, asProb = ashash.computeCentrality(rtree, af)
            pickle.dump((asAggProb, asProb), open(centralityFile, "wb"))
        else:
            asAggProb, asProb = pickle.load(open(centralityFile,"rb"))

        if filter:
            for k,v in tier1.iteritems():
                v.append(asAggProb[str(k)])

        ecdf(asAggProb.values(), lw=1.3, label=date[0],c=ccmap(i/float(len(ribFiles)) ) )

    plt.grid(True)
    #plt.legend(loc="right")
    plt.colorbar(CS3)
    plt.xscale("log")
    #plt.yscale("log")
    plt.xlim([10**-8, 10**-2])
    plt.ylim([10**-3, 1])
    plt.tight_layout()
    if filter is None:
        plt.savefig(centralityFile.rpartition("/")[0]+"/betweenessLongitudinal.eps")
    else:
        plt.title("AS%s" % filter)
        plt.savefig(centralityFile.rpartition("/")[0]+"/betweenessLongitudinal_AS%s.eps" % filter)

    if filter is None:
        plt.figure()
        for k,v in tier1.iteritems():
            plt.plot(yearRange, v, label=k)
        plt.grid(True)
        plt.ylabel("AS betweenness")
        plt.legend(loc="best")
        plt.tight_layout()
        plt.savefig(centralityFile.rpartition("/")[0]+"/tier1.eps")


def compareToCaidaRank():
    space = 1
    af = 4
    dataDirectory = "/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/"

    # Load Caida as rank
    caida = np.genfromtxt("../data/20160901_asRank.csv", dtype=str, delimiter=",")

    centralityFile = "../results/caidaRank/20160901.2200.pickle"
    ribFile = dataDirectory+"2016.09/RIBS/rib.20160901.2200.bz2"
    if not os.path.exists(centralityFile):
        rtree, _ = ashash.readrib(ribFile, space, af) 
        asAggProb, asProb = ashash.computeCentrality(rtree, af)
        pickle.dump((asAggProb, asProb), open(centralityFile, "wb"))
    else:
        asAggProb, asProb = pickle.load(open(centralityFile,"rb"))

    sortedAs = sorted(asAggProb, key=lambda k: asAggProb[k], reverse=True) 

    r = np.arange(1,10)
    topList = list(r*10)
    topList.extend(r*100)
    topList.extend(r*1000)
    topList.extend(range(10000, 50000, 10000))
    x=[]
    y=[]
    for top in topList:
        print top
        x.append(top)
        us = [] 
        caidaFiltered = []
        for rank, asn in caida[:top]:
            if asn in asAggProb:
                us.append(sortedAs.index(asn)+1)
                caidaFiltered.append(rank)

        res = scipy.stats.spearmanr(caidaFiltered, us)
        y.append(res[0])
        
    plt.figure()
    plt.plot(x,y,"x-")
    plt.xscale("log")
    plt.xlabel("CAIDA top ASN")
    plt.ylabel("Spearman correlation coeff.")
    plt.tight_layout()
    plt.savefig("../results/caidaRank/spearmanCorrelation.eps")

    return caidaFiltered, us
