import argparse
import glob
from matplotlib import pylab as plt
import plotter

def localGraphTransitEvolution(scope, name, dbList=None, outdir="./", expid=1):
    """ Plot the time evolution of AS hegemony scores for the local graph 
    identified by the ASN scope"""

    pr = plotter.Plotter(db=dbList)
    pr.hegemonyEvolutionLocalGraph(scope, expid=expid)

    if name is None:
        plt.title("Transits towards AS%s\n\n" % scope)
    else:
        if name!="":
            plt.title(name+"\n\n")

    plt.tight_layout()
    plt.savefig(outdir+"/AS%s_localHegemony.pdf" %  scope)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Folder containing the .sql file produced by ashash", type=str, required=True)
    parser.add_argument("-a", "--asn", help="ASN of the origin AS", type=int, required=True)
    parser.add_argument("-t", "--title", help="Title for the figure", type=str, default=None)
    parser.add_argument("-e", "--expid", help="Experiment ID, only needed if your run ashash multiple times", type=int, default=1)
    args = parser.parse_args()

    db = glob.glob(args.input+"/*.sql")
    localGraphTransitEvolution(args.asn, args.title, dbList=db, outdir=args.input, expid=args.expid)
