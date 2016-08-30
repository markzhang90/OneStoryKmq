from pykafka import KafkaClient

client = KafkaClient(hosts="127.0.0.1:9092")
topic_code = 'noooo'
topicsingle = client.topics[topic_code.encode()]
balanced_consumer = topicsingle.get_balanced_consumer(
    consumer_group='mark-aaa'.encode(),
    auto_commit_enable=True,
    zookeeper_connect='127.0.0.1:2181'
)

for message in balanced_consumer:
    if message is not None:
        print(message.offset, message.value.decode())
