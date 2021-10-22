from airflow.exceptions import AirflowNotFoundException
import pika
from plugins.entities.entity import ClsEntity
import threading
from plugins.utils.logger import generate_logger

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
        super(ClsResultMQ, self).__init__(**kwargs)
        if not self.is_config_changed(**kwargs):
            return
        self._disconnect()
        self._kwargs = kwargs

    @staticmethod
    def get_result_mq_args(key='qcos_rabbitmq'):
        from airflow.models.connection import Connection
        mq = Connection.get_connection_from_secrets(key)
        if mq is None:
            _logger.error('连接"{}"未配置'.format(key))
            return {
                "host": None,
                "port": None,
                "username": None,
                "password": None
            }
        data = {
            "host": mq.host,
            "port": mq.port,
            "username": mq.login,
            "password": mq.get_password(),
            "vhost": mq.get_extra().get('vhost', '/')
        }
        return data

    def is_config_changed(self, **kwargs):
        try:
            if not self._kwargs:
                return False
            for key in kwargs.keys():
                if self._kwargs.get(key, None) != kwargs.get(key):
                    return True
            return False
        except Exception:
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
            'virtual_host': self._kwargs.get('vhost', '/')
        }
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(**connection_config)
        )

    def get_channel(
        self,
        queue,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
        arguments=None,
        exchange=None,
        exchange_type='fanout',
        routing_key='#',
        exchange_durable=None,
        **kwargs
    ) -> pika.adapters.blocking_connection.BlockingChannel:
        _logger.debug('get extra args: {}'.format(repr(kwargs)))
        if queue in self.channels.keys():
            return self.channels.get(queue)
        self._connect()
        channel = self._connection.channel()
        channel.confirm_delivery()
        channel.queue_declare(
            queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments
        )
        if exchange and exchange_type:
            channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=exchange_durable)
            channel.queue_bind(exchange=exchange,
                               queue=queue,
                               routing_key=routing_key)  # 匹配python.后所有单词
        self.channels[queue] = channel  # 将channel加入到字典对象中
        return channel

    def _disconnect(self):
        if not self._connection:
            self._connection = None
            return
        self._connection.close()
        self._connection = None

    def run(self, queue, **kwargs):
        channel = self.get_channel(queue=queue, **kwargs)
        channel.start_consuming()

    def doSubscribe(self, queue, message_handler, **kwargs):
        if not queue:
            raise AirflowNotFoundException(u'订阅的队列未指定')
        channel = self.get_channel(queue=queue, **kwargs)
        channel.basic_consume(queue, on_message_callback=message_handler, auto_ack=kwargs.get('auto_ack', True))

    def doUnsubscribe(self, queue, **kwargs):
        channel = self.get_channel(queue=queue, **kwargs)
        channel.stop_consuming()

    def send_message(self, body, queue, **kwargs):
        if queue is None:
            raise AirflowNotFoundException(u'mq queue 未指定')
        channel = self.get_channel(queue, **kwargs)
        exchange = self._kwargs.get('exchange', '')
        channel.basic_publish(exchange=exchange, routing_key=queue, body=body)
