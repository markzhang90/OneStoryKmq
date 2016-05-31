from pykafka import KafkaClient

client = KafkaClient(hosts="127.0.0.1:9092")
topic_code = 'test'
topic = client.topics[topic_code.encode()]
consumer = topic.get_simple_consumer()

for message in consumer:
    if message is not None:
        print(message.offset, message.value.decode())
