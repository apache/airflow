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

import logging

from cached_property import cached_property
from airflow.utils.decorators import apply_defaults
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.providers.apache.kafka.hooks.kafka_consumer_hook import KafkaConsumerHook


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
        :param conn_id:
            the kafka broker connection whom this sensor
            subscripts against.
        :param topic:
            the subscribed topic
        """
        self.topic = topic
        self.host = host
        self.port = port
        super(KafkaSensor, self).__init__(*args, **kwargs)

    @cached_property
    def hook(self):
        return KafkaConsumerHook(self.topic, self.host, self.port)

    def poke(self, context):
        logging.info(
            'Poking topic: %s, using hook: %s',
            self.topic, self.hook)

        messages = self.hook.get_messages()

        if messages is not {}:
            logging.info(
                'Got messages during poking: %s', messages)
            return messages
        else:
            return False
