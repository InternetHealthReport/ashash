from kafka import KafkaProducer
import msgpack
import logging


class saverKafka(object):
    """Dumps variables to a Kafka cluster."""

    def __init__(self, bootstrapServers, af, saverQueue, saverChain, keepNullHege=False):     
        print('starting Kafka saver')

        self.saverQueue = saverQueue
        self.saverChain = saverChain
        self.af = af
        self.prevts = -1
        self.keepNullHege = keepNullHege

        self.producer = KafkaProducer(bootstrap_servers=bootstrapServers, acks=0,
            key_serializer=lambda k: k.to_bytes(8, byteorder='big'),
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            batch_size=65536, linger_ms=4000, compression_type='snappy')

        self.run()

    def run(self):
        while True:
            elem = self.saverQueue.get()
            if self.saverChain is not None:
                self.saverChain.put(elem)
            if isinstance(elem, str) and elem.endswith(";"):
                #Unhandled
                print("Cannot save item: ",elem)
            else:
                self.save(elem)
            self.saverQueue.task_done()

    def save(self, elem):
        t, data = elem

        if t == "hegemony":
            #for each item in hege, send it.
            ts, scope, hege = data

            if self.prevts != ts:
                self.prevts = ts
                logging.debug("start recording hegemony")

            for k,v in hege.iteritems():
                if v!=0 or self.keepNullHege:
                    hegeObj = {}
                    hegeObj["ts"] = ts
                    hegeObj["scope"] = scope
                    hegeObj["asn"] = k
                    hegeObj["hege"] = v

                    self.producer.send("ihr_hegemony_values_ipv{}".format(self.af), 
                            key=scope, value=hegeObj)
            
        elif t == "graphchange":
            graphChangeObj = {}
            graphChangeObj["ts"] = data[0]
            graphChangeObj["scope"] = data[1]
            graphChangeObj["asn"] = data[2]
            graphChangeObj["nbvote"] = data[3]
            graphChangeObj["diffhege"] = data[4]

            self.producer.send("ihr_hegemony_graphchange", 
                    key=data[1], value=graphChangeObj)

        elif t == "anomalouspath":
            anomalousPathObj = {}
            anomalousPathObj["ts"] = data[0]
            anomalousPathObj["path"] = data[1]
            anomalousPathObj["origas"] = data[2]
            anomalousPathObj["anoasn"] = data[3]
            anomalousPathObj["hegepath"] = data[4]
            anomalousPathObj["score"] = data[5]

            self.producer.send("ihr_hegemony_anomalouspath",anomalousPathObj)

