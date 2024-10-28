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
from __future__ import annotations

from unittest import mock

from airflow.models import Connection
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook


class TestInfluxDbHook:
    def setup_method(self):
        self.influxdb_hook = InfluxDBHook()
        extra = {}
        extra["token"] = "123456789"
        extra["org"] = "test"
        extra["timeout"] = 10000

        self.connection = Connection(schema="http", host="localhost", extra=extra)

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_get_conn(self, influx_db_client):
        self.influxdb_hook.get_connection = mock.Mock()
        self.influxdb_hook.get_connection.return_value = self.connection

        self.influxdb_hook.get_conn()

        assert self.influxdb_hook.uri == "http://localhost:7687"

        assert self.influxdb_hook.get_connection.return_value.schema == "http"
        assert self.influxdb_hook.get_connection.return_value.host == "localhost"
        influx_db_client.assert_called_once_with(
            url="http://localhost:7687", token="123456789", org="test", timeout=10000
        )

        assert self.influxdb_hook.get_client is not None

    def test_query(self):
        self.influxdb_hook.get_conn = mock.Mock()

        influxdb_query = 'SELECT "duration" FROM "pyexample"'
        self.influxdb_hook.query(influxdb_query)

        self.influxdb_hook.get_conn.assert_called()

    def test_query_to_df(self):
        self.influxdb_hook.get_conn = mock.Mock()

        self.influxdb_hook.write = mock.Mock()
        influxdb_query = 'SELECT "duration" FROM "pyexample"'
        self.influxdb_hook.query_to_df(influxdb_query)

        self.influxdb_hook.get_conn.assert_called()

    def test_write(self):
        self.influxdb_hook.get_connection = mock.Mock()
        self.influxdb_hook.get_connection.return_value = self.connection

        self.influxdb_hook.get_conn = mock.Mock()
        self.influxdb_hook.client = mock.Mock()
        self.influxdb_hook.client.write_api = mock.Mock()

        self.influxdb_hook.write(
            "test_bucket", "test_point", "location", "Prague", "temperature", 25.3, True
        )

        self.influxdb_hook.client.write_api.assert_called()

    def test_find_bucket_by_id(self):
        self.influxdb_hook.get_connection = mock.Mock()
        self.influxdb_hook.get_connection.return_value = self.connection

        self.influxdb_hook.client = mock.Mock()
        self.influxdb_hook.client.buckets_api = mock.Mock()

        self.influxdb_hook.find_bucket_id_by_name("test")

        self.influxdb_hook.client.buckets_api.assert_called()
