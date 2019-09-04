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

        self.topic = '_'.join(['ihr', 'bgp', collectorName, collectionType])

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_ashegemony_reader0',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': 900*1000,
        })

        self.consumer.subscribe([self.topic])

        # 24 hours in milliseconds
        self.windowSize = 86400*1000
        self.observer = None 

    def attach(self,observer):
        self.observer = observer

    def start(self):
        # seek the timestamp in consumer
        if self.collectionType == "ribs":
            timestampToSeek = (self.startTS - 3600)*1000
        else:
            timestampToSeek = self.startTS * 1000

        if self.collectionType == "ribs":
            timestampToBreakAt = (self.startTS + 3600)*1000
        else:
            timestampToBreakAt = timestampToSeek + self.windowSize

        logging.warning("{}, start: {}, end: {}".format(
            self.topic, timestampToSeek, timestampToBreakAt))

        logging.debug("look for offset: {}".format(timestampToSeek))
        # Set offsets according to start time
        topic_info = self.consumer.list_topics(self.topic)
        partitions = [TopicPartition(self.topic, partition_id, timestampToSeek) 
                for partition_id in  topic_info.topics[self.topic].partitions.keys()]
        logging.warning("partitions: {}".format(partitions))

        offsets = self.consumer.offsets_for_times(partitions)
        self.consumer.poll()
        for offset in offsets:
            logging.info('set offset: {}'.format(offset))
            self.consumer.seek(offset)

        dataHandler = None
        timeout = 600
        if self.collectionType == "ribs":
            dataHandler = self.observer.updateCountsRIB
            timeout = 60
        else:
            dataHandler = self.observer.updateCountsUpdates

        logging.debug('enter in consumer loop')
        while True:
            msg = self.consumer.poll(timeout)

            if msg is None:
                logging.debug('Timeout')
                break

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            ts = msg.timestamp()
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] >= timestampToBreakAt:
                logging.debug('Read all data')
                break

            val = msgpack.unpackb(msg.value(), raw=False)
            dataHandler(val)
