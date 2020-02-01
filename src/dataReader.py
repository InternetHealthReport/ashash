# from kafka import KafkaConsumer
from confluent_kafka import Consumer, TopicPartition, KafkaError
import confluent_kafka 
import msgpack
import logging

class DataReader():
    '''Read BGP data from Kafka cluster. 
    
    Reads from topics ihr_bgp_collectorName_collectionType'''

    def __init__(self, collectorsName, collectionType, af, 
            startTS, endTS, windowSize, dataCallback):
        '''Initialize kafka consumer with the offset corresponding to the given 
        timestamp'''

        logging.warning('starting Kafka reader')

        self.collectors = collectorsName
        self.startTS = startTS
        self.endTS = endTS
        self.collectionType = collectionType
        self.af = af
        if collectionType == 'ribs':
            self.timeout = 600
        else:
            self.timeout = None

        self.windowSize = windowSize * 1000
        self.dataCallback = dataCallback 
        self.queuedMessages = []

        self.topics = ['_'.join(['ihr', 'bgp', collector, collectionType])
                for collector in self.collectors]

        self.timestampToSeek = self.startTS * 1000
        self.currentTimebin = int(self.startTS/windowSize)*windowSize * 1000
        self.timestampToBreakAt = self.endTS * 1000
        self.partitionPaused = set()
        self.partitionStopped = 0
        self.partitionTotal = 0

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_ashegemony_reader_'+self.collectionType,
            'max.poll.interval.ms': 900*1000,
            'enable.auto.commit': 'false',
        })

        self.consumer.subscribe(self.topics, on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        '''Position the consumer to the offset corresponding to the given start
        timestamp.'''

        # Intialize total number of assigned partitions
        self.partitionTotal = len(partitions)
        
        # Seek offset for given start timestamp
        for p in partitions:
            p.offset = self.timestampToSeek
        offsets = consumer.offsets_for_times(partitions)
        consumer.assign(offsets)
        logging.warning("{}, start: {}, end: {}, {} partitions".format(
            self.topics, self.timestampToSeek, self.timestampToBreakAt, self.partitionTotal))

    def start(self):
        '''Consume data for all collectors by chunk of length windowSize'''

        logging.info('enter in consumer loop')
        nb_messages = 0
        while True:
            msg = self.consumer.poll(self.timeout)

            if msg is None:
                logging.warn('Timeout! (poll done with {}s)'.format(self.timeout))
                break

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            ts = msg.timestamp()
            val = msgpack.unpackb(msg.value(), raw=False)
            nb_messages += 1
 
            if (len(val['elements']) == 0 or 
                    (ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] < self.timestampToSeek)) :
                continue
            
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] >= self.timestampToBreakAt:
                logging.warning('Stop partition {} for {}.'.format(msg.partition(), msg.topic()))
                self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                self.partitionStopped += 1
                if self.partitionStopped < self.partitionTotal:
                    continue
                else:
                    break
                
            # We got all data for this partition, pause it
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] >= self.currentTimebin + self.windowSize:
                logging.warning('Pause partition {} for {} ts={}.({}, {}, {} msg processed)'.format(
                    msg.partition(), msg.topic(), ts[1], self.currentTimebin, self.currentTimebin+self.windowSize, nb_messages))
                tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
                self.consumer.pause([tp])
                self.partitionPaused.add(tp) 
                if len(self.partitionPaused) < self.partitionTotal:
                    self.queuedMessages.append(val)
                    continue
                else:
                    logging.warning('Resume partitions {} ({}, {}).'.format(
                        self.consumer.assignment(), self.currentTimebin, self.currentTimebin+self.windowSize))
                    # Send queued messages and resume consumer
                    self.currentTimebin += self.windowSize
                    for qval in self.queuedMessages:
                        self.dataCallback(qval)
                    logging.warning('pushed queued messages')

                    # Initialisation for a new time bin
                    tps = list(self.partitionPaused)
                    self.consumer.commit(offsets=tps)
                    self.queuedMessages = []
                    self.consumer.resume(tps)
                    self.partitionPaused = set()
                    self.partitionStopped = 0

            self.dataCallback(val)

        self.consumer.close()


