import pika
from .entity import ClsEntity


class ClsResultMQ(ClsEntity):
    def __init__(self, host=None, queue=None, routing_key=None, exchange=None):
        super(ClsResultMQ, self).__init__()
        self._queue = queue
        self._host = host
        self._routing_key = routing_key
        self._exchange = exchange
        self._connection = None

    def _connect(self):
        if self._connection:
            self._connection.close()
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host))
        channel = self._connection.channel()
        channel.queue_declare(queue=self._queue)
        return channel

    def _disconnect(self):
        self._connection.close()
        self._connection = None

    def send_message(self, body):
        channel = self._connect()
        channel.basic_publish(exchange=self._exchange, routing_key=self._routing_key, body=body)
        self._disconnect()
