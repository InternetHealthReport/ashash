from subprocess import Popen, PIPE
import datetime
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pylab as plt
import glob
import json


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
            


def extractData(start, end, monitoredAS, filterBGPmsg=None):

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
    params = ["python2", "ashash.py", "-f "+str(monitoredAS), "--plot", ribfile]
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

