from airflow.hooks import BaseHook
from kafka import KafkaConsumer


class KafkaConsumerHook(BaseHook):

    default_host = 'localhost'
    default_port = 9092

    def __init__(self, conn_id):
        super(KafkaConsumerHook, self).__init__(None)
        self.conn = self.get_connection(conn_id)
        self.consumer = None

    def get_conn(self):
        if not self.consumer:
            conf = self.conn.extra_dejson
            host = self.conn.host or self.default_host
            port = self.conn.port or self.default_port

            server = '{host}:{port}'.format(**locals())
            self.consumer = KafkaConsumer(
                bootstrap_servers=server, **conf)

        return self.consumer

    def get_message(self):
        consumer = self.get_conn()
        return next(consumer)