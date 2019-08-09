from kafka import KafkaConsumer
import msgpack
import logging
from kafka.structs import TopicPartition


class DataReader():
    '''Read BGP data from Kafka cluster. 
    
    Reads 1 day of data, from topics ihr_collectorName_collectionType'''

    def __init__(self, collectorName, startTS, liveMode, collectionType, af=4,
                 includedPeers=[], includedPrefix=[]):
        self.collector = collectorName
        self.startTS = startTS
        self.liveMode = liveMode
        self.collectionType = collectionType
        self.af = af
        self.includedPeers = includedPeers
        self.includedPrefix = includedPrefix

        self.topicName = '_'.join(['ihr', collectorName, collectionType])

        self.consumer = KafkaConsumer(
            bootstrap_servers=['kafka1:9092', 'kafka2:9092'],
            # consumer_timeout_ms=1000, 
            # auto_offset_reset="earliest",
            value_deserializer=lambda v: msgpack.unpackb(v, raw=False))

        self.topicPartition = TopicPartition(self.topicName, 0)
        # 24 hours in milliseconds
        self.windowSize = 86400*1000
        self.observers = []

    def attach(self,observer):
        if observer not in self.observers:
            self.observers.append(observer)

    def performUpdate(self,data):
        for observer in self.observers:
            if self.collectionType == "rib":
                observer.updateCountsRIB(data)
            else:
                observer.updateCountsUpdates(data)

    def start(self):
        # seek the timestamp in consumer
        if self.collectionType == "rib":
            timestampToSeek = (self.startTS - 3600)*1000
        else:
            timestampToSeek = self.startTS * 1000

        if self.collectionType == "rib":
            timestampToBreakAt = (self.startTS + 3600)*1000
        else:
            timestampToBreakAt = timestampToSeek + self.windowSize

        logging.warning(self.collectionType, " ,Time Start: ", timestampToSeek, 
                        "Time End: ", timestampToBreakAt)

        offsets = self.consumer.offsets_for_times({self.topicPartition:timestampToSeek})
        theOffset = offsets[self.topicPartition].offset

        if theOffset is None:
            return

        self.consumer.assign([self.topicPartition])
        self.consumer.seek(self.topicPartition,theOffset)

        for message in self.consumer:
            #Convert message to JSON before handler function call
            messageTimestamp = message.timestamp

            if messageTimestamp > timestampToBreakAt:
                break

            self.performUpdate(message.value)
            
