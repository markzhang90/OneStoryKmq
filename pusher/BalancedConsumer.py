from pykafka import KafkaClient


class BalancedConsumer:

    host = None
    client = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(BalancedConsumer, cls).__new__(cls)
        return cls._instance

    def __init__(self, host="127.0.0.1:9092", zookeeper_connect='127.0.0.1:2181'):

        self.host = host
        self.client = KafkaClient(hosts=self.host)
        self.zookeeper_connect = zookeeper_connect

    def runConsumer(self, topic_code, consumer_group='group1'):

        if topic_code is None:
            return False

        if not isinstance(topic_code, bytes):
            topic_code = topic_code.encode()

        if not isinstance(consumer_group, bytes):
            consumer_group = consumer_group.encode()

        topicsingle = self.client.topics[topic_code]
        balanced_consumer = topicsingle.get_balanced_consumer(
            consumer_group=consumer_group,
            auto_commit_enable=True,
            zookeeper_connect=self.zookeeper_connect
        )

        for message in balanced_consumer:
            if message is not None:
                print(message.offset, message.value.decode())

bc = BalancedConsumer("127.0.0.1:9092")
bc.runConsumer('testnew')