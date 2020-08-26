#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from cached_property import cached_property

from airflow.providers.apache.kafka.hooks.kafka_consumer_hook import KafkaConsumerHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class KafkaSensor(BaseSensorOperator):
    """
    Consumes the Kafka message with the specific topic
    """
    DEFAULT_HOST = 'kafka1'
    DEFAULT_PORT = 9092
    templated_fields = ('topic',
                        'host',
                        'port',
                        )

    @apply_defaults
    def __init__(self, topic, host=DEFAULT_HOST, port=DEFAULT_PORT, *args, **kwargs):
        """
            Initialize the sensor, the connection establish
            is put off to it's first time usage.

        :param topic:
        :param host:
        :param port:
        :param args:
        :param kwargs:
        """
        self.topic = topic
        self.host = host
        self.port = port
        super(KafkaSensor, self).__init__(*args, **kwargs)

    @cached_property
    def hook(self):
        """
        Returns a Kafka Consumer Hook
        """
        return KafkaConsumerHook(self.topic, self.host, self.port)

    def poke(self, context):
        """
            Checks to see if messages exist on this topic/partition.

        :param context:
        :return:
        """
        self.log.info('Poking topic: %s, using hook: %s', str(self.topic), str(self.hook))

        messages = self.hook.get_messages()

        if messages:
            self.log.info('Got messages during poking: %s', str(messages))
            return messages
        else:
            return False
