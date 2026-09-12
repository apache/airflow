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

from airflow.providers.influxdb.triggers.influxdb3 import InfluxDB3QueryTrigger
from airflow.triggers.base import TriggerEvent

SQL = 'SELECT "duration" FROM "pyexample"'


class TestInfluxDB3QueryTrigger:
    def test_serialization(self):
        """Trigger round-trips its constructor arguments."""
        trigger = InfluxDB3QueryTrigger(sql=SQL, influxdb3_conn_id="influxdb3_default")
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.influxdb.triggers.influxdb3.InfluxDB3QueryTrigger"
        assert kwargs == {"sql": SQL, "influxdb3_conn_id": "influxdb3_default"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_success(self, mock_hook_class):
        """A completed query emits a single success event carrying JSON-serializable records."""
        pd = pytest.importorskip("pandas")

        dataframe = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        records = [{"col1": 1, "col2": 3}, {"col1": 2, "col2": 4}]
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(return_value=dataframe)

        trigger = InfluxDB3QueryTrigger(sql=SQL)
        events = [event async for event in trigger.run()]

        mock_hook_class.assert_called_once_with(conn_id="influxdb3_default")
        mock_hook.query_async.assert_awaited_once_with(SQL)
        assert events == [TriggerEvent({"status": "success", "records": records})]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_failure(self, mock_hook_class):
        """A failing query is reported as an event, not raised out of the triggerer."""
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(side_effect=ValueError("boom"))

        trigger = InfluxDB3QueryTrigger(sql=SQL)
        events = [event async for event in trigger.run()]

        assert events == [TriggerEvent({"status": "error", "message": "boom"})]
