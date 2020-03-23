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

"""
Rabbitmq module
"""
from librabbitmq import Connection
from airflow.hooks.base_hook import BaseHook


class RabbitmqHook(BaseHook):
    """
    Wrapper for connection to interact with Rabbitmq Message Middleware
    """

    def __init__(self, rabbitmq_conn_id='rabbitmq_default'):
        """
        Prepares hook to connect to a Rabbitmq database.

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Rabbitmq.
        """
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.rabbitmq = None
        self.host = None
        self.port = 5672
        self.password = None
        self.userId = None
        self.vhost = None
        self.heartbeat = 0  # 默认关闭心跳， 单位为秒

    def get_conn(self):
        """
        Returns a Rabbitmq connection.
        """
        conn = self.get_connection(self.rabbitmq_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.password = 'guest' if str(conn.password).lower() in ['none', 'false', ''] else conn.password
        self.userId = conn.extra_dejson.get('user_id', 'guest')
        self.vhost = conn.extra_dejson.get('vhost', '/')
        heartbeat = conn.extra_dejson.get('heartbeat', 0)
        if isinstance(heartbeat, str) or isinstance(heartbeat, bytes):
            heartbeat = int(heartbeat)
        self.heartbeat = heartbeat

        if not self.rabbitmq:
            self.log.debug(
                'Initializing Rabbitmq object for conn_id "%s" on %s:%s vhost:%s',
                self.rabbitmq_conn_id, self.host, self.port, self.vhost
            )
            self.rabbitmq = Connection(
                host=self.host,
                port=self.port,
                password=self.password,
                heartbeat=self.heartbeat,
                userid=self.userId)

        return self.rabbitmq
