from airflow.hooks import BaseHook
from kafka import KafkaConsumer


class KafkaConsumerHook(BaseHook):

    default_host = 'localhost'
    default_port = 9092

    def __init__(self, conn_id, topic):
        super(KafkaConsumerHook, self).__init__(None)
        self.conn = self.get_connection(conn_id)
        self.server = None
        self.consumer = None
        self.topic = topic

    def get_conn(self):
        if not self.consumer:
            conf = self.conn.extra_dejson
            host = self.conn.host or self.default_host
            port = self.conn.port or self.default_port

            # Disable auto commit as the hook will commit right
            # after polling.
            conf['enable_auto_commit'] = False

            self.server = '{host}:{port}'.format(**locals())
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.server, **conf)

        return self.consumer

    def get_messages(self):
        """
        Get all the messages haven't been consumed, it doesn't
        block by default, then commit the offset.

        :return:
            A list of messages
        """
        consumer = self.get_conn()

        # `poll` returns a dict where keys are the partitions
        # and values are the corresponding messages.
        messages = consumer.poll()

        consumer.commit()
        return messages

    def __repr__(self):
        """
        Pretty the hook with the connection info
        """
        connected = self.consumer is not None
        return '<KafkaConsumerHook ' \
               'connected?=%s server=%s topic=%s>' % \
               (connected, self.server, self.topic)
