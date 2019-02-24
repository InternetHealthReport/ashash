import csv


class BGPRecord():
    def __init__(self, row):
        ts, peerIP, peerAS, prefix, aspath, community = row[1], row[3], row[4], row[5], row[6], row[11] 
        self.ts = int(ts)
        self.peer_address = peerIP.strip()
        self.peer_asn = peerAS.strip()
        self.fields = {"prefix": prefix.strip(), "as-path": aspath.strip()}
        # TODO reformat communities if we actually use it  
    

class txtReader():
    def __init__(self, csvFile):
        self.csvFile = csvFile
        self.reader = csv.reader(open(self.csvFile, "r"), delimiter="|")
        self.running = False

        # Things that are referenced in BGPstream
        self.time = 0
        self.status = "valid"


    def stream(self):
        self.running = True
        return map(BGPRecord, self.reader.next())
