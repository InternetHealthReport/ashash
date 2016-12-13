import sys
ye = sys.argv[1] 
output = sys.argv[2]

for mo in range(6,13):
    print "/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/{ye}.{mo:02d}/RIBS/rib.{ye}{mo:02d}01.0000.bz2 '/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/{ye}.{mo:02d}/UPDATES/updates.{ye}{mo:02d}*' ../results/{out}_{ye}.{mo:02d}/".format(ye=ye, mo=mo, out=output)



