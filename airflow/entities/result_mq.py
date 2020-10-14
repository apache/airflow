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

    def __init__(self, **kwargs):
        super(ClsResultMQ, self).__init__()
        if not self.is_config_changed(**kwargs):
            return
        self._disconnect()
        self._kwargs = kwargs

    def is_config_changed(self, **kwargs):
        try:
            if not self._kwargs:
                return False
            for key in kwargs.keys():
                if self._kwargs.get(key, None) != kwargs.get(key):
                    return True
            return False
        except Exception as e:
            return True

    def _connect(self):
        if self._connection:
            return
        username = self._kwargs.get('username', 'guest')
        password = self._kwargs.get('password', 'guest')
        host = self._kwargs.get('host', None)
        port = self._kwargs.get('port', None)
        credentials = pika.PlainCredentials(username, password)
        _logger.info('{}:{}, {},{}'.format(host, port, username, password))
        connection_config = {
            'host': host,
            'port': port,
            'credentials': credentials,
            'virtual_host': self._kwargs.get('vhost')
        }
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(**connection_config)
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
        exchange = self._kwargs.get('exchange', None)
        channel.basic_publish(exchange=exchange, routing_key=queue, body=body)
