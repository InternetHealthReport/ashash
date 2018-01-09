import matplotlib as mpl
mpl.use('Agg')
import plotter
import glob
import scipy.stats as sps
from matplotlib import pylab as plt


def alphaSensitivity(db=None, outdir="./"):
    """ Plot results with different values of alpha """

    pr = plotter.Plotter(db=db)
    cursor = pr.cursor[0]
    refAlpha = 0
    refExpid = None

    # Get expid and alpha values from "experiment" table
    expRaw = cursor.execute("SELECT id, args FROM experiment ")
    exp = {}
    for expid, args in expRaw:
        alpha = [float(x.rpartition("=")[2]) for x in args.split(",") if x.startswith(" alpha=")]
        exp[expid] = alpha[0]
        if alpha[0]>refAlpha:
            refExpid = expid

    asHegemonyRef = dict(cursor.execute("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope=0 AND expid=?",(refExpid,)))
    # Remove AS with a score == 0.0
    toremove = [asn for asn, score in asHegemonyRef.iteritems() if score==0.0]
    if not toremove is None:
        for asn in toremove:
            del asHegemonyRef[asn]
    minVal = min(asHegemonyRef.values())

    y = []
    for expid, alpha in exp.iteritems():
        asHegemony = dict(cursor.execute("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope=0 AND expid=?", (expid,)))

	# Remove AS with a score == 0.0
        toremove = [asn for asn, score in asHegemony.iteritems() if score==0.0]
        if not toremove is None:
            for asn in toremove:
                del asHegemony[asn]

        # Set the same number of ASN for both distributions
        missingAS = set(asHegemonyRef.keys()).difference(asHegemony.keys())
        if not missingAS is None:
            for asn in missingAS:
                asHegemony[asn] = minVal

        # Compute the KL-divergence
        dist = [asHegemony[asn] for asn in asHegemonyRef.keys()]
        kldiv = sps.entropy(dist, asHegemonyRef.values())
        y.append(kldiv)


    plt.figure()
    plt.plot(exp.values(), y)
    plt.xlabel("$\\alpha$")
    plt.ylabel("KL divergence")
    plt.tight_layout()
    plt.savefig("results/alphaSensitivity/alphaSensitivity.pdf")



if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-i", "--input", help="Folder containing the .sql file produced by ashash", type=str, required=True)
    # parser.add_argument("-t", "--title", help="Title for the figure", type=str, default=None)
    # args = parser.parse_args()

    db = glob.glob("results/alphaSensitivity/*.sql")
    alphaSensitivity(db=db)
