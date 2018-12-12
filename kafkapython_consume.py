# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import time
from config import KAFKA_CONFIG

class KafkaService:
    def __init__(self):
        self.balanced_consumer = KafkaConsumer(
                    KAFKA_CONFIG['topic'].decode("utf-8"),
                    group_id=KAFKA_CONFIG['consumer_group'].decode("utf-8"),
                    enable_auto_commit = False,
                    bootstrap_servers = KAFKA_CONFIG['hosts'])
        
    def __consume(self):
        i =0
        t = time.time()
        for message in self.balanced_consumer:
            if message is not None:
                i +=1
                # print (i,message.offset)
            if i >= 100:
                print('kafka-python time: %s' % ((time.time()-t)))
                self.balanced_consumer.seek_to_beginning()
                break
        self.balanced_consumer.commit() 
    
    def monitor(self):
        fcl = self.balanced_consumer.poll()
        if fcl != [] or fcl is not None:
            self.__consume()

if __name__ == '__main__':
    ks = KafkaService()
    for j in range(5):
        ks.monitor()
        
