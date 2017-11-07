import psycopg2
import logging
from collections import defaultdict
import json
from sshtunnel import SSHTunnelForwarder
import apsw
import glob
from datetime import datetime
from multiprocessing import JoinableQueue as mpQueue
from multiprocessing import Process

#TODO make the timebin value function of what is passed in ts

class saverPostgresql(object):

    """Dumps only hegemony results to a Postgresql database. """

    def __init__(self, starttime, af, saverQueue, host="romain.iijlab.net", dbname="ihr"):
       

        self.saverQueue = saverQueue
        self.expid = None
        self.prevts = -1
        self.asNames = defaultdict(str, json.load(open("data/asNames.json")))
        self.starttime = starttime
        self.af = af

        local_port = 5432
        if host != "localhost" and host!="127.0.0.1":
            self.server = SSHTunnelForwarder(
                'romain.iijlab.net',
                ssh_username="romain",
                ssh_private_key="/home/romain/.ssh/id_rsa",
                remote_bind_address=('127.0.0.1', 5432),
                set_keepalive=60) 

            self.server.start()
            logging.debug("SSH tunnel opened")
            local_port = str(self.server.local_bind_port)

        conn_string = "host='127.0.0.1' port='%s' dbname='%s'" % (local_port, dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.cursor.execute("SELECT number FROM ihr_asn WHERE ashash=TRUE")
        self.asns = set(self.cursor.fetchall())
        logging.debug("%s ASNS already registered in the database" % len(self.asns))

        self.run()

    def run(self):

        while True:
            elem = self.saverQueue.get()
            if isinstance(elem, str) and elem.endswith(";"):
                self.cursor.execute(elem)
            else:
                self.save(elem)
            self.saverQueue.task_done()


    def save(self, elem):
        t, data = elem

        if t == "hegemony":
            ts, scope, hege = data

            if self.prevts != ts:
                self.prevts = ts
                logging.debug("start recording hegemony")

            if int(scope) not in self.asns:
                self.asns.add(int(scope))
                self.cursor.execute("""do $$
                    begin 
                        insert into ihr_asn(number, name, tartiflette, disco, ashash) values(%s, %s, FALSE, FALSE, TRUE);
                    exception when unique_violation then
                        update ihr_asn set ashash = TRUE where number = %s;
                    end $$;""", (scope, self.asNames["AS"+str(scope)], scope))

            for asn in hege.keys():
                if  isinstance(asn, int) or not asn.startswith("{"):
                    if int(asn) not in self.asns :
                        self.asns.add(int(asn))
                        self.cursor.execute("INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) select %s, %s, FALSE, FALSE, FALSE WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s); ", (asn, self.asNames["AS"+str(asn)], asn))
            
            self.cursor.executemany("INSERT INTO ihr_hegemony(timebin, originasn_id, asn_id, hege, af) VALUES (%s, %s, %s, %s, %s)", [(self.starttime, scope, k, v, self.af) for k,v in hege.iteritems() if (isinstance(k,int) or not k.startswith("{")) and v!=0 ] )

            # elif t == "graphchange":
                # self.cursor.execute("INSERT INTO graphchange(ts, scope, asn, nbvote, diffhege, expid) VALUES (?, ?, ?, ?, ?, ?)", data+[self.expid])

            # elif t == "anomalouspath":
                # self.cursor.execute("INSERT INTO anomalouspath(ts, path, origas, anoasn, hegepath, score, expid) VALUES (?, ?, ?, ?, ?, ?, ?)", data+[self.expid])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    af = 6
    directory="newResults%s/" % af
    
    slf = "results_2017-03-15 00:00:00.sql"
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    
    # Get data from the sqlite db
    conn = apsw.Connection(directory+slf)
    cursor = conn.cursor()
    cursor.execute("SELECT scope, ts, asn, hege FROM hegemony ORDER BY scope")

    for scope, ts, asn, hege in cursor.fetchall():
        data[scope][ts][asn] = hege

    # Push data to PostgreSQL
    dt = slf.partition("_")[2]
    dt = dt.partition(" ")[0]
    ye, mo, da = dt.split("-")
    starttime = datetime(int(ye), int(mo), int(da))

    saverQueue = mpQueue(1000)
    ss = Process(target=saverPostgresql, args=(starttime, af, saverQueue), name="saverPostgresql")
    ss.start()

    saverQueue.put("BEGIN TRANSACTION;")
    for scope, allts in data.iteritems():
        for ts, hege in allts.iteritems():
            saverQueue.put(("hegemony", (ts, scope, hege)) )
    saverQueue.put("COMMIT;")

    logging.debug("Finished")
    saverQueue.join()
    ss.terminate()


