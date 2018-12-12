# import sqlite3
import apsw
import logging

class saverSQLite(object):

    """Dumps variables to a SQLite database. """

    def __init__(self, filename, saverQueue, saverChain, keepNullHege=False):
       
        self.filename = filename
        self.conn = apsw.Connection(filename)
        self.cursor = self.conn.cursor()
        self.saverQueue = saverQueue
        self.saverChain = saverChain
        self.expid = None
        self.prevts = -1
        self.keepNullHege = keepNullHege

        self.run()

    def run(self):

        self.createdb()
        
        while True:
            elem = self.saverQueue.get()
            if self.saverChain is not None:
                self.saverChain.put(elem)
            if isinstance(elem, str) and elem.endswith(";"):
                self.cursor.execute(elem)
            else:
                self.save(elem)
            self.saverQueue.task_done()


    def createdb(self):
        # Table storing experiements parameters
        self.cursor.execute("CREATE TABLE IF NOT EXISTS experiment (id integer primary key, date text, cmd text, args text)")

        # Table storing computed hegemony scores 
        self.cursor.execute("CREATE TABLE IF NOT EXISTS hegemony (ts integer, scope integer, asn integer, hege real, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON hegemony (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_scope ON hegemony (scope)")

        # Table storing anomalous graph changes
        self.cursor.execute("CREATE TABLE IF NOT EXISTS graphchange (ts integer, scope integer, asn integer, nbvote integer, diffhege real, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON graphchange (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_scope ON graphchange (scope)")

        # Table storing anomalous paths
        self.cursor.execute("CREATE TABLE IF NOT EXISTS anomalouspath (ts integer, path text, origas integer, anoasn integer, hegepath text, score real, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ts ON anomalouspath (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_origas ON anomalouspath (origas)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_anoasn ON anomalouspath (anoasn)")

    def save(self, elem):
        t, data = elem

        if t == "experiment":
            self.cursor.execute("INSERT INTO experiment(date, cmd, args) VALUES (?, ?, ?)", (str(data[0]), data[1], data[2]))
            self.expid = self.conn.last_insert_rowid()
            if self.expid != 1:
                logging.warning("Database exists: results will be stored with experiment ID (expid) = %s" % self.expid)

        if self.expid is None:
            logging.error("No experiment inserted for this data")
            return

        if self.expid is None:
            logging.error("No experiment inserted for this data")

        elif t == "hegemony":
            ts, scope, hege = data

            if self.prevts != ts:
                self.prevts = ts
                logging.debug("start recording hegemony")
            
            self.cursor.executemany("INSERT INTO hegemony(ts, scope, asn, hege, expid) VALUES (?, ?, ?, ?, ?)", [(ts, scope, k, v, self.expid) for k,v in hege.iteritems() if v!=0 or self.keepNullHege ] )
                    # zip([ts]*len(hege), [scope]*len(hege), hege.keys(), hege.values(), [self.expid]*len(hege)) )

        elif t == "graphchange":
            self.cursor.execute("INSERT INTO graphchange(ts, scope, asn, nbvote, diffhege, expid) VALUES (?, ?, ?, ?, ?, ?)", data+[self.expid])

        elif t == "anomalouspath":
            self.cursor.execute("INSERT INTO anomalouspath(ts, path, origas, anoasn, hegepath, score, expid) VALUES (?, ?, ?, ?, ?, ?, ?)", data+[self.expid])
