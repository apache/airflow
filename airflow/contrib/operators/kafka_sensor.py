import logging

from airflow.contrib.hooks.kafka_hook import KafkaConsumerHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults


class KafkaSensor(BaseSensorOperator):
    """
    Consumes the Kafka message with the specific topic
    """

    @apply_defaults
    def __init__(self, conn_id, topic, *args, **kwargs):
        """
        Initialize the sensor, the connection establish
        is put off to it's first time usage.

        :param conn_id:
            the kafka broker connection whom this sensor
            subscripts against.
        :param topic:
            the subscribed topic
        """
        self.topic = topic
        self.hook = KafkaConsumerHook(conn_id, topic)
        super(KafkaSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info(
            'Poking topic: %s, using hook: %s',
            self.topic, self.hook)

        message = self.hook.get_message()

        logging.info(
            'Got message during poking: %s', message)

        return message or False
