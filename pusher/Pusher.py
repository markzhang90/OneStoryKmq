from pykafka import KafkaClient
from queue import Queue
import json


class Pusher:

    host = None
    client = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Pusher, cls).__new__(cls)
        return cls._instance

    def __init__(self, host="127.0.0.1:9092"):

        self.host = host
        self.client = KafkaClient(hosts=self.host)

    def produce_msg(self, message='', mytopic='noooo', partition_key = ''):

        if not isinstance(mytopic, bytes):
            mytopic = mytopic.encode()

        if not isinstance(partition_key, bytes):
            partition_key = partition_key.encode()

        if not isinstance(message, bytes):
            message = message.encode()

        topic = self.client.topics[mytopic]

        with topic.get_producer(delivery_reports=True) as producer:
            producer.produce(message, partition_key)
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


test = Pusher()
send_data = dict()
send_data['url'] = '127.0.0.1/sendnewfile'
send_data['data'] = {'work':'good', 'home':'nice'}
jsonify = json.dumps(send_data)
print(jsonify)
test.produce_msg(jsonify, 'testnew')