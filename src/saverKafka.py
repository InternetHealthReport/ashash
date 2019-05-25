from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from kafka.structs import TopicPartition
import json
import logging

class saverKafka(object):
    """Dumps variables to a Kafka cluster."""

    def __init__(self, bootstrapServers, saverQueue, saverChain, keepNullHege=False):     
        self.saverQueue = saverQueue
        self.saverChain = saverChain
        self.expid = None
        self.prevts = -1
        self.keepNullHege = keepNullHege

        self.producer = KafkaProducer(bootstrap_servers=bootstrapServers, acks=0,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=65536,linger_ms=4000,compression_type='gzip')

        self.expConsumer = KafkaConsumer(auto_offset_reset="latest",bootstrap_servers=bootstrapServers,consumer_timeout_ms=1000,value_deserializer=lambda m: json.loads(m.decode('ascii')))
        self.topicPartition = TopicPartition("experiment",0)
        self.expConsumer.assign([self.topicPartition])

        self.run()

    def getLastExperimentId(self):
        for message in self.expConsumer:
            msgAsDict = message.value
            expid = msgAsDict["id"]
            print("Experiment Id in topic: ",expid)
            return expid

        print("No experiments in topic!")
        return None

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

        if t == "experiment":
            #get last id in experiments table
            lastExpId = self.getLastExperimentId()

            #if id is None, id = 1 else, increment id by 1
            if lastExpId is None:
                self.expid = 1
            else:
                self.expid = lastExpId + 1

            #send the new experiment with new id
            experimentObj = {}
            experimentObj["id"] = self.expid
            experimentObj["date"] = str(data[0])
            experimentObj["cmd"] = data[1]
            experimentObj["args"] = data[2]

            future = self.producer.send("experiment",experimentObj)

            try:
                record_metadata = future.get(timeout=10)
                if self.expid != 1:
                    logging.warning("Database exists: results will be stored with experiment ID (expid) = %s" % self.expid)

            except KafkaError:
                logging.error("No experiment inserted for this data")
                return

        elif t == "hegemony":
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
                    hegeObj["experimentId"] = self.expid

                    self.producer.send("hegemony",hegeObj)
            
        elif t == "graphchange":
            graphChangeObj = {}
            graphChangeObj["ts"] = data[0]
            graphChangeObj["scope"] = data[1]
            graphChangeObj["asn"] = data[2]
            graphChangeObj["nbvote"] = data[3]
            graphChangeObj["diffhege"] = data[4]
            graphChangeObj["experimentId"] = self.expid

            self.producer.send("graphchange",graphChangeObj)

        elif t == "anomalouspath":
            anomalousPathObj = {}
            anomalousPathObj["ts"] = data[0]
            anomalousPathObj["path"] = data[1]
            anomalousPathObj["origas"] = data[2]
            anomalousPathObj["anoasn"] = data[3]
            anomalousPathObj["hegepath"] = data[4]
            anomalousPathObj["score"] = data[5]
            anomalousPathObj["experimentId"] = self.expid

            self.producer.send("anomalouspath",anomalousPathObj)

