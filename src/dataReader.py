from kafka import KafkaConsumer
import json

from datetime import datetime

import logging 

from kafka.structs import TopicPartition, OffsetAndTimestamp

class DataReader():
    def __init__(self,collectorName,startTS,dataHandler,liveMode,collectionType,af=4,includedPeers=[],includedPrefix=[]):
        self.collector = collectorName
        self.startTS = startTS
        self.dataHandler = dataHandler
        self.liveMode = liveMode
        self.collectionType = collectionType
        self.af = af
        self.includedPeers = includedPeers
        self.includedPrefix = includedPrefix

        self.topicName = collectorName + collectionType 

        if liveMode:
            self.topicName += "Live"
        else:
            self.topicName += "Historic"

        self.consumer = KafkaConsumer(auto_offset_reset="earliest",bootstrap_servers=['localhost:9092'],consumer_timeout_ms=1000,value_deserializer=lambda m: json.loads(m.decode('ascii')))
        self.topicPartition = TopicPartition(self.topicName,0)

        self.windowSize = 21600*1000 #milliseconds  #6 hours

    def start(self):
        #seek the timestamp in consumer
        timestampToSeek = self.startTS*1000

        timestampToBreakAt = timestampToSeek + self.windowSize

        print("Time Start: ",timestampToSeek,"Time End: ",timestampToBreakAt)

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

            """
            msgAsString = message.value.decode("utf-8")

            msgAsDict = json.loads(msgAsString)"""

            msgAsDict = message.value

            self.dataHandler(msgAsDict)
            