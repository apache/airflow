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
from airflow.contrib.hooks.redis_cluster_hook import RedisClusterHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class RedisClusterKeySensor(BaseSensorOperator):
    """
    Checks for the existence of a key in a Redis Cluster
    """
    template_fields = ('key',)
    ui_color = '#d8766c'

    @apply_defaults
    def __init__(self, key, redis_cluster_conn_id, *args, **kwargs):
        """
        Creates a new RedisClusterKeySensor

        :param key: the key to monitor its existence.
        :type key: str
        :param redis_cluster_conn_id: the name of the connection that has the
            parameters we need to connect to Redis Cluster.
        :type redis_cluster_conn_id: str
        """
        super(RedisClusterKeySensor, self).__init__(*args, **kwargs)
        self.redis_cluster_conn_id = redis_cluster_conn_id
        self.key = key

    def poke(self, context):
        self.log.info('Sensor checks for existence of key: %s', self.key)
        return RedisClusterHook(self.redis_cluster_conn_id).get_conn().exists(self.key)
