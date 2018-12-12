# -*- coding: utf-8 -*-

from pykafka import KafkaClient
import time
from pykafka.cli.kafka_tools import fetch_consumer_lag
from config import KAFKA_CONFIG

class KafkaService:
    def __init__(self):
        client = KafkaClient(hosts=KAFKA_CONFIG['hosts'],
                         broker_version='0.10.0')

        self.topic = client.topics[KAFKA_CONFIG['topic']] 
        self.balanced_consumer = self.topic.get_balanced_consumer(
                    consumer_group=KAFKA_CONFIG['consumer_group'],
                    auto_commit_enable=False,
                    fetch_message_max_bytes=5242880,
                    queued_max_messages=500,
                    zookeeper_connect=KAFKA_CONFIG['zookeeper_hosts'],
                    auto_start = True)

    @staticmethod
    def __cmp_offset(fcl1, fcl2):
        for keys, offsets in fcl1.items():
            if offsets[1] > fcl2[keys][0]:
                return True
        return False

    def __consume(self):
        i = 0
        t = time.time()
        for message in self.balanced_consumer:
            if message is not None:
                i +=1
            if i >= 1000:
                print('pykafka time: %s' % ((time.time()-t)))
                break
        self.balanced_consumer.commit_offsets()

    def monitor(self):
        try:
            fcl = fetch_consumer_lag(self.balanced_consumer,
                                     self.topic,
                                     KAFKA_CONFIG['consumer_group'])
            print(fcl)
            if fcl is not None:
                self.__consume()
        except Exception as exception:
            print(exception)


if __name__ == '__main__':
    ks = KafkaService()
    bool=True
    while bool:
        ks.monitor()
    print('fin')
        
