from lxml import etree
import lxml
import lxml.html


def parsehtml(filename = "./20160601_caida_as_rank.html"):
    
    s = open(filename,"r").readlines()
    s = "".join(s)

    html = lxml.html.fromstring(s) #.find("body/table")
    body = html.find("body")
    table = body.find_class("as-table")[0]
    rows = table.findall("tr")

    for i in range(3, len(rows)-1):
        val = rows[i].findall("td")
        rank = int(val[0].text_content())
        asn = int(val[1].text_content())
        
        print "%s,%s" % (rank, asn)


if __name__ == "__main__":
    parsehtml()
