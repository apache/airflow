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

import pytest

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
        # Every hook method resolves the Airflow connection before touching the
        # client, so stub it once rather than in each test.
        self.influxdb_hook.get_connection = mock.Mock(return_value=self.connection)

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_get_conn(self, influx_db_client):
        self.influxdb_hook.get_conn()

        assert self.influxdb_hook.uri == "http://localhost:8086"

        assert self.influxdb_hook.get_connection.return_value.schema == "http"
        assert self.influxdb_hook.get_connection.return_value.host == "localhost"
        influx_db_client.assert_called_once_with(
            url="http://localhost:8086", token="123456789", org="test", timeout=10000
        )

        assert self.influxdb_hook.get_client is not None

    @pytest.mark.parametrize(
        ("schema", "port", "expected_uri"),
        [
            pytest.param("http", None, "http://localhost:8086", id="http-defaults-to-8086"),
            pytest.param("https", None, "https://localhost:443", id="https-defaults-to-443"),
            pytest.param(None, None, "https://localhost:443", id="no-scheme-defaults-to-https-443"),
            pytest.param("http", 9999, "http://localhost:9999", id="explicit-port-wins"),
        ],
    )
    def test_get_uri(self, schema, port, expected_uri):
        conn = Connection(schema=schema, host="localhost", port=port)

        assert self.influxdb_hook.get_uri(conn) == expected_uri

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_write_opens_connection(self, influx_db_client):
        self.influxdb_hook.write("bucket", "point", "location", "Prague", "temperature", 25.3, True)

        write_api = influx_db_client.return_value.write_api.return_value
        write_api.write.assert_called_once()

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_create_organization_opens_connection(self, influx_db_client):
        self.influxdb_hook.create_organization("my-org")

        organizations_api = influx_db_client.return_value.organizations_api.return_value
        organizations_api.create_organization.assert_called_once_with(name="my-org")

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_delete_organization_opens_connection(self, influx_db_client):
        self.influxdb_hook.delete_organization("org-id")

        organizations_api = influx_db_client.return_value.organizations_api.return_value
        organizations_api.delete_organization.assert_called_once_with(org_id="org-id")

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_create_bucket_forwards_retention_rules(self, influx_db_client):
        retention_rules = mock.sentinel.retention_rules

        self.influxdb_hook.create_bucket("bucket", "description", "org-id", retention_rules)

        buckets_api = influx_db_client.return_value.buckets_api.return_value
        buckets_api.create_bucket.assert_called_once_with(
            bucket_name="bucket",
            description="description",
            org_id="org-id",
            retention_rules=retention_rules,
        )

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_find_bucket_id_by_name_opens_connection(self, influx_db_client):
        buckets_api = influx_db_client.return_value.buckets_api.return_value
        buckets_api.find_bucket_by_name.return_value = mock.Mock(id="bucket-id")

        assert self.influxdb_hook.find_bucket_id_by_name("bucket") == "bucket-id"

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_delete_bucket_opens_connection(self, influx_db_client):
        buckets_api = influx_db_client.return_value.buckets_api.return_value
        buckets_api.find_bucket_by_name.return_value = mock.Mock(id="bucket-id")

        self.influxdb_hook.delete_bucket("bucket")

        buckets_api.delete_bucket.assert_called_once_with("bucket-id")

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_query(self, influx_db_client):
        influxdb_query = 'SELECT "duration" FROM "pyexample"'

        self.influxdb_hook.query(influxdb_query)

        query_api = influx_db_client.return_value.query_api.return_value
        query_api.query.assert_called_once_with(influxdb_query)

    @mock.patch("airflow.providers.influxdb.hooks.influxdb.InfluxDBClient")
    def test_query_to_df(self, influx_db_client):
        influxdb_query = 'SELECT "duration" FROM "pyexample"'

        self.influxdb_hook.query_to_df(influxdb_query)

        query_api = influx_db_client.return_value.query_api.return_value
        query_api.query_data_frame.assert_called_once_with(influxdb_query)
