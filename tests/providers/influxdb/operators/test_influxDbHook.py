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

import unittest
from unittest import mock

from airflow.models import Connection
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook


class TestInfluxDbHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.influxdb_hook = InfluxDBHook()
        extra = {}
        extra['token'] = '123456789'
        extra['org_name'] = 'test'

        self.connection = Connection(schema='http', host='localhost', extra=extra)

    def test_get_conn(self):
        self.influxdb_hook.get_connection = mock.Mock()
        self.influxdb_hook.get_connection.return_value = self.connection

        self.influxdb_hook.get_client = mock.Mock()
        self.influxdb_hook.get_conn()

        assert self.influxdb_hook.org_name == 'test'
        assert self.influxdb_hook.uri == 'http://localhost:7687'

        assert self.influxdb_hook.get_connection.return_value.schema == 'http'
        assert self.influxdb_hook.get_connection.return_value.host == 'localhost'

        assert self.influxdb_hook.get_client is not None
