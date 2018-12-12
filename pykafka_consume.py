from pykafka import KafkaClient
from pykafka.cli.kafka_tools import fetch_consumer_lag

class KafkaService:
  def __init__(self):
    client = KafkaClient(hosts=KAFKA_CONFIG['hosts'],
                                   use_greenlets=True,
                                   broker_version='0.10.0')
    self.topic = client.topics[KAFKA_CONFIG['topic']]

    balanced_consumer = self.__get_balance_consumer()
  

  def __get_balance_consumer(self):
      return self.topic.get_balanced_consumer(consumer_group=KAFKA_CONFIG['consumer_group'],
                                              auto_commit_enable=False,
                                              fetch_message_max_bytes=15728640,
                                              zookeeper_connection_timeout_ms=3600000,
                                              offsets_commit_max_retries=10,
                                              fetch_wait_max_ms=1,
                                              consumer_timeout_ms=-1,
                                              zookeeper_connect=KAFKA_CONFIG['zookeeper_hosts'],
                                              auto_start=False,
                                              queued_max_messages=KAFKA_CONFIG['queued_max_messages'],
                                              reset_offset_on_start=self.reset_offset_on_start,
                                              )
    def __read_consummer(self):
        fcl = fetch_consumer_lag(self.balanced_consumer,
                                 self.topic,
                                 KAFKA_CONFIG['consumer_group'])
        print('{{partition_id: (latest_offset, consumer_offset)}} \n'
                '{} \n'
                .format(str(fcl)))

    def __consume(self):
        consume_value = 0
        self.balanced_consumer.start()
        for message in self.balanced_consumer:
            if message:
                time.sleep(1)
                consume_value += 1
                dict_message = json.loads(message.value.decode('utf-8')) //接收到为json格式，转化成dict
                __traite_message(dict_message)
                if consume_value >= KAFKA_CONFIG['consume_value']:
                    self.balanced_consumer.commit_offsets()
                    self.__read_consummer()
                    consume_value = 0
         self.balanced_consumer.stop()
