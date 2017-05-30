import threading
import logging
import Queue
import numpy as np
import itertools

from  more_itertools import unique_justseen


class pathMonitor(threading.Thread):

    def __init__(self, hegemonyQueue, announceQueue ):
        threading.Thread.__init__(self)

        self.setName("Path Monitor")
        self.hegemonyQueue = hegemonyQueue
        self.announceQueue = announceQueue
        self.hegemony = None
        self.daemon = True


    def run(self):
        while True:
            try:
                _, _, self.hegemony = self.hegemonyQueue.get_nowait() 
                logging.debug("PM got new hegemony results")
            except Queue.Empty:
                pass

            msg = self.announceQueue.get()
            if not self.hegemony is None:
                self.detectValley(msg)

            self.announceQueue.task_done()


    def detectValley(self,msg):
    #TODO: clean this function
    #TODO: check if the hegemony scores are not too old..
        zTd, zDt, zS, zOrig, zAS, zPfx, path, zPro, zOr, z0, z1, z2, z3, z4, z5 = msg
        path = list(unique_justseen(path))

        try :
            # hegeAll = map(lambda x: round(asAggProb[x],2), path[1:-1])
            hegeAll = map(lambda x: self.hegemony[x], path[1:])
            hege = list(unique_justseen(hegeAll))
        except Exception:
            # print path
            # print "New AS"
            #TODO: handle exceptions better
            return

        hegeDiff = np.diff(hege)
        signChange = len(list(itertools.groupby(hegeDiff, lambda x: x >= 0)))

        if signChange > 2 : # and not "27064" in path and  not "27065" in path and not "27066" in path:
            # print "(pathMonitor) anomaly: %s (%s)" % (path[1:],hege)
            #TODO report anomaly

            # Find suspicious transit AS
            prev = hegeAll[0]
            goingDown = False
            for i, d in enumerate(hegeAll[1:]):
                if goingDown and d>prev:
                    #TODO report anomaly
                    pass
                    # print "(pathMonitor) anomalous transit: %s" % path[i+1]
                    # anomalousTransit[path[i+1]]+=1 #+1 because we ignore the peer AS in hegeAll

                if d >= prev:
                    goingDown = False
                else:
                    goingDown = True

                prev=d


