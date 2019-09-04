# from kafka import KafkaConsumer
from confluent_kafka import Consumer, TopicPartition, KafkaError
import confluent_kafka 
import msgpack
import logging

class DataReader():
    '''Read BGP data from Kafka cluster. 
    
    Reads 1 day of data, from topics ihr_collectorName_collectionType'''

    def __init__(self, collectorName, startTS, liveMode, collectionType, af=4,
                 includedPeers=[], includedPrefix=[]):

        logging.warning('starting Kafka reader')

        self.collector = collectorName
        self.startTS = startTS
        self.liveMode = liveMode
        self.collectionType = collectionType
        self.af = af
        self.includedPeers = includedPeers
        self.includedPrefix = includedPrefix

        # 24 hours in milliseconds
        self.windowSize = 86400*1000
        self.observer = None 

        self.topic = '_'.join(['ihr', 'bgp', collectorName, collectionType])

        self.timestampToSeek = self.startTS * 1000
        if self.collectionType == "ribs":
            self.timestampToSeek = (self.startTS - 3600)*1000

        self.timestampToBreakAt = self.timestampToSeek + self.windowSize
        if self.collectionType == "ribs":
            self.timestampToBreakAt = (self.startTS + 3600)*1000

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_ashegemony_reader0',
            'max.poll.interval.ms': 900*1000,
        })

        self.consumer.subscribe([self.topic], on_assign=self.on_assign)

    def attach(self,observer):
        self.observer = observer

    def on_assign(self, consumer, partitions):
        logging.warning("{}, start: {}, end: {}".format(
            self.topic, self.timestampToSeek, self.timestampToBreakAt))

        for p in partitions:
            p.offset = self.timestampToSeek

        offsets = consumer.offsets_for_times(partitions)
        consumer.assign(offsets)

    def start(self):
        dataHandler = None
        timeout = 600
        if self.collectionType == "ribs":
            dataHandler = self.observer.updateCountsRIB
        else:
            dataHandler = self.observer.updateCountsUpdates

        logging.info('enter in consumer loop')
        while True:
            msg = self.consumer.poll(timeout)
            # Start reading data, decrease the timeout
            timeout = 60

            if msg is None:
                logging.warn('Timeout')
                break

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            ts = msg.timestamp()
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] >= self.timestampToBreakAt:
                logging.info('Read all data')
                break

            val = msgpack.unpackb(msg.value(), raw=False)
            dataHandler(val)
