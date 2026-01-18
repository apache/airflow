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

from airflow.providers.influxdb3.operators.influxdb3 import InfluxDB3Operator


class TestInfluxDB3Operator:
    def setup_method(self):
        self.operator = InfluxDB3Operator(
            task_id="test_task",
            sql='SELECT "duration" FROM "pyexample"',
            influxdb3_conn_id="influxdb3_default",
        )

    def test_init(self):
        """Test operator initialization."""
        assert self.operator.sql == 'SELECT "duration" FROM "pyexample"'
        assert self.operator.influxdb3_conn_id == "influxdb3_default"
        assert "sql" in self.operator.template_fields

    @mock.patch("airflow.providers.influxdb3.operators.influxdb3.InfluxDB3Hook")
    def test_execute(self, mock_hook_class):
        """Test operator execution."""
        import json

        import pandas as pd

        mock_hook = mock.Mock()
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        mock_hook.query.return_value = mock_df
        mock_hook_class.return_value = mock_hook

        result = self.operator.execute(context={})

        mock_hook_class.assert_called_once_with(conn_id="influxdb3_default")
        mock_hook.query.assert_called_once_with('SELECT "duration" FROM "pyexample"')
        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], dict)
        assert "col1" in result[0] and "col2" in result[0]
