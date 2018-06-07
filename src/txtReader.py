import csv


class BGPRecord():
    def __init__(self, ts, peerIP, peerAS, prefix, aspath, community):
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


    def get_next_elem(self):
        self.running = True
        try:
            row = self.reader.next()
            return BGPRecord(row[1], row[3], row[4], row[5], row[6], row[11] )

        except StopIteration:
            return None


     
