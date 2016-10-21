ye = 2015

for mo in range(1,13):
    print "/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/{ye}.{mo:02d}/RIBS/rib.{ye}{mo:02d}01.0000.bz2 '/data/routeviews/archive.routeviews.org/route-views.linx/bgpdata/{ye}.{mo:02d}/UPDATES/updates.{ye}{mo:02d}*' ../results/{ye}.{mo:02d}/".format(ye=ye, mo=mo)



