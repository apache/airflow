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

from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator


class TestInfluxDBOperator:
    @mock.patch("airflow.providers.influxdb.operators.influxdb.InfluxDBHook")
    def test_influxdb_operator_test(self, mock_hook):
        sql = """from(bucket:"test") |> range(start: -10m)"""
        op = InfluxDBOperator(
            task_id="basic_influxdb", sql=sql, influxdb_conn_id="influxdb_default"
        )
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(conn_id="influxdb_default")
        mock_hook.return_value.query.assert_called_once_with(sql)
