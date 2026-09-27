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

import asyncio
from unittest import mock

import pandas as pd
import pytest

from airflow.providers.influxdb.triggers.influxdb3 import InfluxDB3QueryTrigger, InfluxDB3SensorTrigger
from airflow.triggers.base import TriggerEvent

SQL = 'SELECT "duration" FROM "pyexample"'
CONN_ID = "test_influxdb3_conn"


class TestInfluxDB3QueryTrigger:
    def test_serialization(self):
        """Trigger serializes its constructor arguments."""
        trigger = InfluxDB3QueryTrigger(sql=SQL, influxdb3_conn_id=CONN_ID)
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.influxdb.triggers.influxdb3.InfluxDB3QueryTrigger"
        assert kwargs == {"sql": SQL, "influxdb3_conn_id": CONN_ID}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_success(self, mock_hook_class):
        """A completed query emits a single success event carrying JSON-serializable records."""
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

        mock_hook_class.assert_called_once_with(conn_id="influxdb3_default")
        mock_hook.query_async.assert_awaited_once_with(SQL)
        assert events == [TriggerEvent({"status": "error", "message": "boom"})]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_propagates_cancellation(self, mock_hook_class):
        """Trigger cancellation propagates instead of being converted into an error event."""
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(side_effect=asyncio.CancelledError())

        trigger = InfluxDB3QueryTrigger(sql=SQL)

        with pytest.raises(asyncio.CancelledError):
            await anext(trigger.run())


class TestInfluxDB3SensorTrigger:
    def test_serialization(self):
        """Trigger serializes its constructor arguments."""
        trigger = InfluxDB3SensorTrigger(
            sql=SQL,
            influxdb3_conn_id=CONN_ID,
            poll_interval=30,
            fail_on_empty=True,
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.influxdb.triggers.influxdb3.InfluxDB3SensorTrigger"
        assert kwargs == {
            "sql": SQL,
            "influxdb3_conn_id": CONN_ID,
            "poll_interval": 30,
            "fail_on_empty": True,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_succeeds_without_sleep_when_condition_is_met(self, mock_hook_class, mock_sleep):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(return_value=pd.DataFrame({"literal": [1]}))

        events = [event async for event in InfluxDB3SensorTrigger(sql=SQL).run()]

        mock_hook_class.assert_called_once_with(conn_id="influxdb3_default")
        mock_hook.query_async.assert_awaited_once_with(SQL)
        mock_sleep.assert_not_awaited()
        assert events == [TriggerEvent({"status": "success"})]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_polls_until_condition_is_met(self, mock_hook_class, mock_sleep):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(
            side_effect=[
                pd.DataFrame({"literal": []}),
                pd.DataFrame({"count": [0]}),
                pd.DataFrame({"literal": [1]}),
            ]
        )

        events = [event async for event in InfluxDB3SensorTrigger(sql=SQL, poll_interval=30).run()]

        assert mock_hook.query_async.await_count == 3
        assert mock_sleep.await_args_list == [mock.call(30), mock.call(30)]
        assert events == [TriggerEvent({"status": "success"})]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_fail_on_empty(self, mock_hook_class, mock_sleep):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(return_value=pd.DataFrame({"literal": []}))

        events = [event async for event in InfluxDB3SensorTrigger(sql=SQL, fail_on_empty=True).run()]

        mock_hook.query_async.assert_awaited_once_with(SQL)
        mock_sleep.assert_not_awaited()
        assert events == [
            TriggerEvent({"status": "fail", "message": "No rows returned, raising as per fail_on_empty flag"})
        ]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_failure(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(side_effect=ValueError("boom"))

        events = [event async for event in InfluxDB3SensorTrigger(sql=SQL).run()]

        mock_hook_class.assert_called_once_with(conn_id="influxdb3_default")
        mock_hook.query_async.assert_awaited_once_with(SQL)
        assert events == [TriggerEvent({"status": "error", "message": "boom"})]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_failure_after_unsuccessful_poll(self, mock_hook_class, mock_sleep):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(
            side_effect=[pd.DataFrame({"literal": []}), ValueError("boom")]
        )

        events = [event async for event in InfluxDB3SensorTrigger(sql=SQL, poll_interval=30).run()]

        assert mock_hook.query_async.await_count == 2
        mock_sleep.assert_awaited_once_with(30)
        assert events == [TriggerEvent({"status": "error", "message": "boom"})]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_propagates_cancellation_during_query(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(side_effect=asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await anext(InfluxDB3SensorTrigger(sql=SQL).run())

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.influxdb.triggers.influxdb3.asyncio.sleep",
        new_callable=mock.AsyncMock,
        side_effect=asyncio.CancelledError(),
    )
    @mock.patch("airflow.providers.influxdb.triggers.influxdb3.InfluxDB3Hook", autospec=True)
    async def test_run_propagates_cancellation_during_sleep(self, mock_hook_class, mock_sleep):
        mock_hook = mock_hook_class.return_value
        mock_hook.query_async = mock.AsyncMock(return_value=pd.DataFrame({"literal": []}))

        with pytest.raises(asyncio.CancelledError):
            await anext(InfluxDB3SensorTrigger(sql=SQL, poll_interval=30).run())

        mock_hook.query_async.assert_awaited_once_with(SQL)
        mock_sleep.assert_awaited_once_with(30)
