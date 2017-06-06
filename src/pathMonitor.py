import threading
import logging
import Queue
import numpy as np
import itertools

from  more_itertools import unique_justseen


class pathMonitor(threading.Thread):

    def __init__(self, hegemonyQueue, announceQueue, saverQueue=None):
        threading.Thread.__init__(self)

        self.setName("Path Monitor")
        self.hegemonyQueue = hegemonyQueue
        self.announceQueue = announceQueue
        self.hegemony = dict()
        self.daemon = True
        self.saverQueue = saverQueue


    def run(self):
        while True:
            try:
                _, scope, hege = self.hegemonyQueue.get_nowait() 
                logging.debug("PM got new hegemony results")
                self.hegemony[scope] = hege 
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
        origas = path[-1]

        if not origas in self.hegemony:
            return
        hege = self.hegemony[origas]

        hegeAll = map(lambda x: round(hege[x],2), path[1:])
        # hegeAll = map(lambda x: hege[x], path[1:])
        hege = list(unique_justseen(hegeAll))

        hegeDiff = np.diff(hege)
        signChange = len(list(itertools.groupby(hegeDiff, lambda x: x >= 0)))

        if signChange > 1 : # and not "27064" in path and  not "27065" in path and not "27066" in path:
            # print "(pathMonitor) anomaly: %s (%s)" % (path[1:],hege)

            # Find suspicious transit AS
            prev = hegeAll[0]
            goingDown = False
            for i, d in enumerate(hegeAll[1:]):
                if goingDown and d>prev:
                    #TODO compute anomalous score
                    self.saverQueue.put( ("anomalouspath", [zTd, msg, origas, path[i+1], hege, d]) )

                    # print "(pathMonitor) anomalous transit: %s" % path[i+1]
                    # anomalousTransit[path[i+1]]+=1 #+1 because we ignore the peer AS in hegeAll

                if d >= prev:
                    goingDown = False
                else:
                    goingDown = True

                prev=d

