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
from airflow.providers.influxdb3.hooks.influxdb3 import InfluxDB3Hook


class TestInfluxDB3Hook:
    def setup_method(self):
        self.influxdb3_hook = InfluxDB3Hook()
        extra = {}
        extra["token"] = "123456789"
        extra["database"] = "test_db"
        extra["org"] = "test_org"

        self.connection = Connection(schema="https", host="localhost", port=8086, extra=extra)

    @mock.patch("airflow.providers.influxdb3.hooks.influxdb3.InfluxDBClient3")
    def test_get_conn(self, influx_db_client_3):
        """Test connection to InfluxDB 3.x."""
        self.influxdb3_hook.get_connection = mock.Mock()
        self.influxdb3_hook.get_connection.return_value = self.connection
        self.connection.extra_dejson = self.connection.extra_dejson

        self.influxdb3_hook.get_conn()

        assert self.influxdb3_hook.uri == "https://localhost:8086"

        assert self.influxdb3_hook.get_connection.return_value.schema == "https"
        assert self.influxdb3_hook.get_connection.return_value.host == "localhost"
        influx_db_client_3.assert_called_once_with(
            host="https://localhost:8086", token="123456789", database="test_db", org="test_org"
        )

        assert self.influxdb3_hook.get_client is not None

    def test_get_conn_missing_database(self):
        """Test that InfluxDB 3.x requires database parameter."""
        extra = {}
        extra["token"] = "123456789"

        connection = Connection(schema="https", host="localhost", extra=extra)
        influxdb3_hook = InfluxDB3Hook()

        influxdb3_hook.get_connection = mock.Mock()
        influxdb3_hook.get_connection.return_value = connection
        connection.extra_dejson = extra

        with pytest.raises(ValueError, match="database is required"):
            influxdb3_hook.get_conn()

    def test_query(self):
        """Test query with InfluxDB 3.x."""
        import pandas as pd

        self.influxdb3_hook.get_conn = mock.Mock()
        self.influxdb3_hook.client = mock.Mock()
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        self.influxdb3_hook.client.query = mock.Mock(return_value=mock_df)

        influxdb_query = 'SELECT "duration" FROM "pyexample"'
        result = self.influxdb3_hook.query(influxdb_query)

        self.influxdb3_hook.get_conn.assert_called()
        self.influxdb3_hook.client.query.assert_called_once_with(
            query=influxdb_query, language="sql", mode="pandas"
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_write(self):
        """Test write with InfluxDB 3.x."""
        self.influxdb3_hook.get_connection = mock.Mock()
        self.influxdb3_hook.get_connection.return_value = self.connection
        self.influxdb3_hook.get_conn = mock.Mock()
        self.influxdb3_hook.client = mock.Mock()
        self.influxdb3_hook.client.write = mock.Mock()

        self.influxdb3_hook.write(
            measurement="test_measurement",
            tags={"location": "Prague"},
            fields={"temperature": 25.3},
        )

        self.influxdb3_hook.client.write.assert_called_once()
        # Verify the point was created correctly
        call_args = self.influxdb3_hook.client.write.call_args
        assert call_args is not None
        assert "record" in call_args.kwargs

    def test_write_no_fields(self):
        """Test that write requires at least one field."""
        self.influxdb3_hook.get_connection = mock.Mock()
        self.influxdb3_hook.get_connection.return_value = self.connection
        self.influxdb3_hook.get_conn = mock.Mock()
        self.influxdb3_hook.client = mock.Mock()

        with pytest.raises(ValueError, match="At least one field is required"):
            self.influxdb3_hook.write(measurement="test", tags={"tag": "value"}, fields=None)

    @mock.patch("airflow.providers.influxdb3.hooks.influxdb3.INFLUXDB_CLIENT_3_AVAILABLE", False)
    def test_get_client_missing_library(self):
        """Test that ImportError is raised when influxdb3-python is not installed."""
        self.influxdb3_hook.get_connection = mock.Mock()
        self.influxdb3_hook.get_connection.return_value = self.connection
        self.connection.extra_dejson = self.connection.extra_dejson

        with pytest.raises(ImportError, match="influxdb3-python is required"):
            self.influxdb3_hook.get_conn()
