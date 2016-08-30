from pykafka import KafkaClient
from queue import Queue


client = KafkaClient(hosts="127.0.0.1:9092")
topic_code = 'noooo'
topic = client.topics[topic_code.encode()]


# with topic.get_sync_producer() as producer:
#     producer.produce('test yeeeeee cooool'.encode())
#     producer.produce('test nooool'.encode())

with topic.get_producer(delivery_reports=True) as producer:
    count = 0
    while count < 40:
        count += 1
        producer.produce(('test' + str(count)).encode(), partition_key='{}'.format(count).encode())
        if count % 10 == 0:  # adjust this or bring lots of RAM ;)
            while True:
                try:
                    msg, exc = producer.get_delivery_report(block=False)
                    if exc is not None:
                        print('Failed to deliver msg {}: {}'.format(
                            msg.partition_key, repr(exc)))
                    else:
                        print('Successfully delivered msg {}'.format(
                            msg.partition_key))
                except Exception as e:
                    print(e)
                    break

