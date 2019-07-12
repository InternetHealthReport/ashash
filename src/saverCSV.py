import logging
import sys

class saverCSV(object):

    """Write results to a csv file (or stdout if filename is None)"""

    def __init__(self, filename, saverQueue, saverChain, keepNullHege=False):

        self.filename = filename
        if filename is None:
            self.filepointer = sys.stdout
        else:
            self.filepointer = open(self.filename, "w")

        self.saverQueue = saverQueue
        self.saverChain = saverChain
        self.prevts = -1
        self.keepNullHege = keepNullHege

        self.run()

    def run(self):

        self.filepointer.write("#timestamp, originasn, asn, hegemony\n")
        while True:
            elem = self.saverQueue.get()
            if self.saverChain is not None:
                self.saverChain.put(elem)


            if isinstance(elem, str) and elem.endswith(";"):
                pass
            else:
                self.save(elem)
                self.filepointer.flush()

            self.saverQueue.task_done()


    def save(self, elem):
        t, data = elem

        if t == "hegemony":
            ts, scope, hege = data

            if self.prevts != ts:
                self.prevts = ts
                logging.debug("start recording hegemony")

            for val in [(ts, scope, k, v) for k,v in hege.iteritems() if v!=0 or self.keepNullHege ]:
                self.filepointer.write("%, %, %, %\n" % val)


