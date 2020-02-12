# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from src.kafka_hook import KafkaConsumerHook
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

        messages = self.hook.get_messages()

        logging.info(
            'Got messages during poking: %s', messages)

        return messages or False
