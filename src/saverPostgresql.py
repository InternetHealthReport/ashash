import sys
import psycopg2
import psycopg2.extras
from pgcopy import CopyManager
# from io import BytesIO
import logging
from collections import defaultdict
import json
import apsw
import glob
from datetime import datetime
from multiprocessing import JoinableQueue as mpQueue
from multiprocessing import Process
from cStringIO import StringIO


class saverPostgresql(object):

    """Dumps only hegemony results to a Postgresql database. """

    def __init__(self, starttime, af, saverQueue, host="localhost", dbname="ihr"):
       

        self.saverQueue = saverQueue
        self.expid = None
        self.prevts = 0 
        self.asNames = defaultdict(str, json.load(open("data/asNames.json")))
        self.currenttime = starttime
        self.af = af
        self.dataHege = [] 
        self.cpmgr = None

        local_port = 5432
        if host != "localhost" and host!="127.0.0.1":
            from sshtunnel import SSHTunnelForwarder
            self.server = SSHTunnelForwarder(
                host,
                ssh_username="romain",
                ssh_private_key="/home/romain/.ssh/id_rsa",
                remote_bind_address=('127.0.0.1', 5432),
                set_keepalive=60) 

            self.server.start()
            logging.debug("SSH tunnel opened")
            local_port = str(self.server.local_bind_port)

            conn_string = "host='127.0.0.1' port='%s' dbname='%s'" % (local_port, dbname)
        else:
            conn_string = "dbname='%s'" % dbname

        self.conn = psycopg2.connect(conn_string)
        columns=("timebin", "originasn_id", "asn_id", "hege", "af")
        self.cpmgr = CopyManager(self.conn, 'ihr_hegemony', columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.cursor.execute("SELECT number FROM ihr_asn WHERE ashash=TRUE")
        self.asns = set([x[0] for x in self.cursor.fetchall()])
        logging.debug("%s ASNS already registered in the database" % len(self.asns))

        self.run()

    def run(self):

        while True:
            elem = self.saverQueue.get()
            if isinstance(elem, str) and elem.endswith(";"):
                if elem.startswith("COMMIT"):
                    logging.warn("psql: start commit")
                    self.commit()
                    logging.warn("psql: end commit")
            else:
                self.save(elem)
            self.saverQueue.task_done()


    def save(self, elem):
        t, data = elem

        if t == "hegemony":
            ts, scope, hege = data

            # Update the current bin timestamp
            if self.prevts != ts:
                self.prevts = ts
                self.currenttime = datetime.utcfromtimestamp(ts)
                logging.debug("start recording hegemony")

            # Update seen ASNs
            if int(scope) not in self.asns:
                self.asns.add(int(scope))
                logging.warn("psql: add new scope %s" % scope)
                self.cursor.execute("INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) select %s, %s, FALSE, FALSE, TRUE WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", (scope, self.asNames["AS"+str(scope)], scope))
                self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(scope),))
            insertQuery = "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) select %s, %s, FALSE, FALSE, TRUE WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)"
            param = [(asn, self.asNames["AS"+str(asn)], asn) for asn in hege.keys() if  (isinstance(asn, int) or not asn.startswith("{") ) and int(asn) not in self.asns]
            #toremove?
            for asn, _, _ in param:
                self.asns.add(int(asn))
                self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(asn),))
            if len(param)>0:
                psycopg2.extras.execute_batch(self.cursor, insertQuery, param, page_size=100)
            
            # Hegemony values to copy in the database
            self.dataHege.extend([(self.currenttime, int(scope), int(k), float(v), int(self.af)) for k,v in hege.iteritems() if (isinstance(k,int) or not k.startswith("{")) and v!=0 ])

    def commit(self):
        logging.warn("psql: start copy")
        self.cpmgr.copy(self.dataHege, StringIO)
        self.conn.commit()
        logging.warn("psql: end copy")
        # Populate the table for AS hegemony cone
        logging.warn("psql: adding hegemony cone")
        self.cursor.execute("INSERT INTO ihr_hegemonycone (timebin, conesize, af, asn_id) SELECT timebin, count(distinct originasn_id), af, asn_id FROM ihr_hegemony WHERE timebin=%s and asn_id!=originasn_id and originasn_id!=0 GROUP BY timebin, af, asn_id;", (self.currenttime,))
        self.conn.commit()
        self.dataHege = []
        logging.warn("psql: end hegemony cone")


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s af" % sys.argv[0])
        sys.exit()

    logging.basicConfig(level=logging.DEBUG)

    af = int(sys.argv[1])
    directory="newResults%s/" % af
    
    for slf in glob.glob(directory+"*.sql"):
        logging.debug("File: %s" % slf)
        # slf = "results_2017-03-15 00:00:00.sql"
        data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        
        # Get data from the sqlite db
        conn = apsw.Connection(slf)
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


