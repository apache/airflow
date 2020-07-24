import pika
from .entity import ClsEntity
import threading
from airflow.utils.logger import generate_logger

_logger = generate_logger(__name__)


class ClsResultMQ(ClsEntity):
    _instance_lock = threading.Lock()
    _connection: pika.BlockingConnection = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsResultMQ, "_instance"):
            with ClsResultMQ._instance_lock:
                if not hasattr(ClsResultMQ, "_instance"):
                    ClsResultMQ._instance = object.__new__(cls)
        return ClsResultMQ._instance

    def __init__(self, host=None, port=None, exchange=None, username='guest', password='guest'):
        super(ClsResultMQ, self).__init__()
        if not self.is_config_changed(host, port, exchange):
            return
        self._disconnect()
        self._host = host
        self._port = port
        self._exchange = exchange
        self._username = username
        self._password = password

    def is_config_changed(self, host=None, port=None, exchange=None, username='guest',
                          password='guest'):
        try:
            if self._host != host:
                return True
            if self._port != port:
                return True
            if self._exchange != exchange:
                return True
            if self._username != username:
                return True
            if self._password != password:
                return True
            return False
        except Exception as e:
            return True

    def _connect(self):
        if self._connection:
            return
        credentials = pika.PlainCredentials(self._username, self._password)
        _logger.info('{}:{}, {},{}'.format(self._host, self._port, self._username, self._password))
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials=credentials)
        )

    def get_channel(self, queue, **kwargs) -> pika.adapters.blocking_connection.BlockingChannel:
        self._connect()
        channel = self._connection.channel()
        channel.confirm_delivery()
        channel.queue_declare(queue, **kwargs)
        return channel

    def _disconnect(self):
        if not self._connection:
            self._connection = None
            return
        self._connection.close()
        self._connection = None

    def send_message(self, body, queue, **kwargs):
        if queue is None:
            raise Exception('mq queue 未指定')
        channel = self.get_channel(queue, **kwargs)
        channel.basic_publish(exchange=self._exchange, routing_key=queue, body=body)
