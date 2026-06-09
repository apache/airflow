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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.apache.spark.triggers.spark_submit import SparkDriverTrigger
from airflow.triggers.base import TriggerEvent


class TestSparkDriverTrigger:
    """Tests for SparkDriverTrigger."""

    def _make_trigger(self, driver_id="driver-001", master_urls=None, poll_interval=1):
        return SparkDriverTrigger(
            driver_id=driver_id,
            master_urls=master_urls or ["http://spark-master:6066"],
            poll_interval=poll_interval,
        )

    # ── serialize ──────────────────────────────────────────────────────

    def test_serialize_roundtrip(self):
        """serialize() must return the correct classpath and all constructor args."""
        trigger = self._make_trigger(
            driver_id="driver-abc",
            master_urls=["http://m1:6066", "http://m2:6066"],
            poll_interval=5,
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.apache.spark.triggers.spark_submit.SparkDriverTrigger"
        assert kwargs == {
            "driver_id": "driver-abc",
            "master_urls": ["http://m1:6066", "http://m2:6066"],
            "poll_interval": 5,
        }

    def test_serialize_can_reconstruct_trigger(self):
        """A trigger reconstructed from serialize() is identical to the original."""
        original = self._make_trigger(driver_id="driver-xyz", poll_interval=30)
        _, kwargs = original.serialize()
        reconstructed = SparkDriverTrigger(**kwargs)

        assert reconstructed.driver_id == original.driver_id
        assert reconstructed.master_urls == original.master_urls
        assert reconstructed.poll_interval == original.poll_interval

    # ── run — terminal states ──────────────────────────────────────────

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["FINISHED", "FAILED", "KILLED", "ERROR"])
    async def test_run_yields_event_on_terminal_state(self, state):
        """run() must yield a TriggerEvent when driver reaches a terminal state."""
        trigger = self._make_trigger()
        trigger._poll_driver_status = mock.AsyncMock(return_value=state)

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert isinstance(events[0], TriggerEvent)
        assert events[0].payload["driver_id"] == "driver-001"
        assert events[0].payload["driver_state"] == state.upper()

    @pytest.mark.asyncio
    async def test_run_success_on_finished(self):
        """FINISHED state must produce status=success."""
        trigger = self._make_trigger()
        trigger._poll_driver_status = mock.AsyncMock(return_value="FINISHED")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["FAILED", "KILLED", "ERROR"])
    async def test_run_error_on_non_finished_terminal(self, state):
        """Non-FINISHED terminal states must produce status=error."""
        trigger = self._make_trigger()
        trigger._poll_driver_status = mock.AsyncMock(return_value=state)

        events = []
        async for event in trigger.run():
            events.append(event)

        assert events[0].payload["status"] == "error"

    # ── run — active states loop ───────────────────────────────────────

    @pytest.mark.asyncio
    async def test_run_polls_until_terminal(self):
        """run() must keep polling while driver is in an active state."""
        trigger = self._make_trigger(poll_interval=0)
        trigger._poll_driver_status = mock.AsyncMock(
            side_effect=["RUNNING", "RUNNING", "FINISHED"]
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert trigger._poll_driver_status.call_count == 3
        assert events[0].payload["status"] == "success"

    # ── run — all masters unreachable ──────────────────────────────────

    @pytest.mark.asyncio
    async def test_run_yields_error_when_all_masters_unreachable(self):
        """run() must yield an error event when _poll_driver_status returns None."""
        trigger = self._make_trigger()
        trigger._poll_driver_status = mock.AsyncMock(return_value=None)

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert "unreachable" in events[0].payload["message"].lower()

    # ── _poll_driver_status ────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_poll_returns_driver_state(self):
        """_poll_driver_status must return driverState from a successful REST response."""
        trigger = self._make_trigger()
        mock_resp = mock.AsyncMock()
        mock_resp.raise_for_status = mock.MagicMock()
        mock_resp.json = mock.AsyncMock(return_value={"success": True, "driverState": "RUNNING"})
        mock_resp.__aenter__ = mock.AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = mock.AsyncMock(return_value=False)

        mock_session = mock.AsyncMock()
        mock_session.get.return_value = mock_resp
        mock_session.__aenter__ = mock.AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = mock.AsyncMock(return_value=False)

        with mock.patch("aiohttp.ClientSession", return_value=mock_session):
            result = await trigger._poll_driver_status()

        assert result == "RUNNING"

    @pytest.mark.asyncio
    async def test_poll_returns_unknown_on_success_false(self):
        """When success=false, _poll_driver_status must return UNKNOWN (HA failover)."""
        trigger = self._make_trigger()
        mock_resp = mock.AsyncMock()
        mock_resp.raise_for_status = mock.MagicMock()
        mock_resp.json = mock.AsyncMock(return_value={"success": False, "message": "not found"})
        mock_resp.__aenter__ = mock.AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = mock.AsyncMock(return_value=False)

        mock_session = mock.AsyncMock()
        mock_session.get.return_value = mock_resp
        mock_session.__aenter__ = mock.AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = mock.AsyncMock(return_value=False)

        with mock.patch("aiohttp.ClientSession", return_value=mock_session):
            result = await trigger._poll_driver_status()

        assert result == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_poll_tries_next_master_on_exception(self):
        """_poll_driver_status must try the next master URL when one raises."""
        trigger = self._make_trigger(master_urls=["http://m1:6066", "http://m2:6066"])

        call_count = 0

        async def fake_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "m1" in url:
                raise ConnectionError("m1 down")
            mock_resp = mock.AsyncMock()
            mock_resp.raise_for_status = mock.MagicMock()
            mock_resp.json = mock.AsyncMock(return_value={"success": True, "driverState": "RUNNING"})
            mock_resp.__aenter__ = mock.AsyncMock(return_value=mock_resp)
            mock_resp.__aexit__ = mock.AsyncMock(return_value=False)
            return mock_resp

        mock_session = mock.AsyncMock()
        mock_session.get.side_effect = fake_get
        mock_session.__aenter__ = mock.AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = mock.AsyncMock(return_value=False)

        with mock.patch("aiohttp.ClientSession", return_value=mock_session):
            result = await trigger._poll_driver_status()

        assert result == "RUNNING"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_poll_returns_none_when_all_masters_fail(self):
        """_poll_driver_status must return None when all master URLs raise."""
        trigger = self._make_trigger(master_urls=["http://m1:6066", "http://m2:6066"])

        mock_session = mock.AsyncMock()
        mock_session.get.side_effect = ConnectionError("all down")
        mock_session.__aenter__ = mock.AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = mock.AsyncMock(return_value=False)

        with mock.patch("aiohttp.ClientSession", return_value=mock_session):
            result = await trigger._poll_driver_status()

        assert result is None