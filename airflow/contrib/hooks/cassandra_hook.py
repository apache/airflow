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

from cassandra.cluster import Cluster
from cassandra.policies import (RoundRobinPolicy, DCAwareRoundRobinPolicy,
                                TokenAwarePolicy, HostFilterPolicy,
                                WhiteListRoundRobinPolicy)
from cassandra.auth import PlainTextAuthProvider

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class CassandraHook(BaseHook, LoggingMixin):
    """
    Hook used to interact with Cassandra

    Contact_points can be specified as a comma-separated string in the 'hosts'
    field of the connection. Port can be specified in the port field of the
    connection. Load_alancing_policy, ssl_options, cql_version can be specified
    in the extra field of the connection.

    For details of the Cluster config, see cassandra.cluster for more details.
    """
    def __init__(self, cassandra_conn_id='cassandra_default'):
        conn = self.get_connection(cassandra_conn_id)

        conn_config = {}
        if conn.host:
            conn_config['contact_points'] = conn.host.split(',')

        if conn.port:
            conn_config['port'] = int(conn.port)

        if conn.login:
            conn_config['auth_provider'] = PlainTextAuthProvider(
                username=conn.login, password=conn.password)

        lb_policy = self.get_policy(conn.extra_dejson.get('load_balancing_policy', None))
        if lb_policy:
            conn_config['load_balancing_policy'] = lb_policy

        cql_version = conn.extra_dejson.get('cql_version', None)
        if cql_version:
            conn_config['cql_version'] = cql_version

        ssl_options = conn.extra_dejson.get('ssl_options', None)
        if ssl_options:
            conn_config['ssl_options'] = ssl_options

        self.cluster = Cluster(**conn_config)
        self.keyspace = conn.schema

    def get_conn(self):
        """
        Returns a cassandra connection object
        """
        return self.cluster.connect(self.keyspace)

    def get_cluster(self):
        return self.cluster

    @classmethod
    def get_policy(cls, policy_name):
        policies = {
            'RoundRobinPolicy': RoundRobinPolicy,
            'DCAwareRoundRobinPolicy': DCAwareRoundRobinPolicy,
            'TokenAwarePolicy': TokenAwarePolicy,
            'HostFilterPolicy': HostFilterPolicy,
            'WhiteListRoundRobinPolicy': WhiteListRoundRobinPolicy,
        }
        return policies.get(policy_name)
