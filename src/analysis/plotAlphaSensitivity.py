import plotter


def alphaSensitivity(db=None, outdir="./"):
    """ Plot results with different values of alpha """

    pr = plotter.Plotter(db=db)
    exp = pr.cursor("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope=0 AND expid=1")
    data = pr.cursor("SELECT asn, hege FROM hegemony WHERE ts=0 AND scope=0 AND expid=1")
    pr.hegemonyEvolutionLocalGraph(scope, expid=expid)

    # plt.title(name+"\n\n")

    plt.tight_layout()
    plt.savefig(outdir+"/alphaSensitivity.pdf")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Folder containing the .sql file produced by ashash", type=str, required=True)
    parser.add_argument("-a", "--asn", help="ASN of the origin AS", type=int, required=True)
    parser.add_argument("-t", "--title", help="Title for the figure", type=str, default=None)
    parser.add_argument("-e", "--expid", help="Experiment ID, only needed if your run ashash multiple times", type=int, default=1)
    args = parser.parse_args()

    db = glob.glob("alphaSensitivity/*.sql")
    localGraphTransitEvolution(db=db, outdir=args.input)
