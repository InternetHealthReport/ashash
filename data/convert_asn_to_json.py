import json
import sys

asn2name = {}
with open(sys.argv[1], "r", encoding="ISO-8859-1") as fi:

    for line in fi:
        asn, _, name = line.partition(" ")
        asn2name["AS"+asn] = name[:-1]

json.dump(asn2name, open("asNames.json","w"))
