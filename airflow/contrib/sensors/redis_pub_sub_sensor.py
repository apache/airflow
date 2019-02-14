# -*- coding: utf-8 -*-
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

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class RedisPubSubSensor(BaseSensorOperator):
    """
    Redis sensor for reading a message from pub sub channels
    """
    template_fields = ('channels',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self, channels, hook, *args, **kwargs):
        """
        Create a new RedisPubSubSensor

        :param channels: The channels to be subscribed to (templated)
        :type channels: str or list of str
        :param hook: the redis hook
        :type hook: RedisHook
        """

        super(RedisPubSubSensor, self).__init__(*args, **kwargs)
        self.channels = channels
        self.hook = hook
        self.ps = hook.get_conn().pubsub()
        self.ps.subscribe(self.channels)

    def poke(self, context):
        """
        Check for message on subscribed channels and write to xcom the message with key ``message``

        An example of message ``{'type': 'message', 'pattern': None, 'channel': b'test', 'data': b'hello'}``

        :param context: the context object
        :type context: dict
        :return: ``True`` if message (with type 'message') is available or ``False`` if not
        :rtype: bool
        """
        self.log.info('RedisPubSubSensor checking for message on channels: %s', self.channels)

        message = self.ps.get_message()
        self.log.info('Message %s from channel %s', message, self.channels)

        # Process only message types
        if message and message['type'] == 'message':
            self.log.info('Message from channel %s is %s',
                          message['channel'].decode('utf-8'), message['data'].decode('utf-8'))
            context['ti'].xcom_push(key='message', value=message)
            self.ps.unsubscribe(self.channels)
            return True

        return False
