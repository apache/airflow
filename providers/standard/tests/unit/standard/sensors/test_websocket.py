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
from websockets.sync.client import ClientConnection

from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import AirflowSensorTimeout, TaskDeferred
from airflow.providers.standard.sensors.websocket import WebSocketSensor
from airflow.providers.standard.triggers.websocket import WebSocketTrigger

from tests_common.test_utils.version_compat import timezone

URL = "ws://example.com/socket"
DEFAULT_DATE = timezone.datetime(2015, 1, 1)


class TestWebSocketSensor:
    @classmethod
    def setup_class(cls):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        cls.dag = DAG("test_websocket_sensor", schedule=None, default_args=args)

    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_returns_true_on_message(self, mock_connect):
        mock_websocket = mock.MagicMock(spec=ClientConnection)
        mock_websocket.recv.return_value = "pong"
        mock_connect.return_value.__enter__.return_value = mock_websocket

        sensor = WebSocketSensor(task_id="poke_true", url=URL, message_to_send="ping", dag=self.dag)
        assert sensor.poke(context={}) is True
        mock_websocket.send.assert_called_once_with("ping")

    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_returns_false_on_timeout(self, mock_connect):
        mock_websocket = mock.MagicMock(spec=ClientConnection)
        mock_websocket.recv.side_effect = TimeoutError()
        mock_connect.return_value.__enter__.return_value = mock_websocket

        sensor = WebSocketSensor(task_id="poke_false", url=URL, dag=self.dag)
        assert sensor.poke(context={}) is False

    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_waits_for_the_overall_timeout_not_poke_interval(self, mock_connect):
        """recv() must be bounded by the sensor's overall timeout, not poke_interval —
        otherwise a single poke could block past the sensor's declared timeout before
        that timeout is ever checked."""
        mock_websocket = mock.MagicMock(spec=ClientConnection)
        mock_websocket.recv.return_value = "pong"
        mock_connect.return_value.__enter__.return_value = mock_websocket

        sensor = WebSocketSensor(
            task_id="poke_timeout_arg", url=URL, timeout=45, poke_interval=5, dag=self.dag
        )
        assert sensor.poke(context={}) is True

        mock_connect.assert_called_once_with(URL, additional_headers=None, open_timeout=45)
        (_, kwargs) = mock_websocket.recv.call_args
        assert kwargs["timeout"] == pytest.approx(45, abs=1)

    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_returns_false_when_handshake_times_out(self, mock_connect):
        """A connect() timeout (slow handshake) must be treated the same as a recv()
        timeout — including respecting soft_fail — not propagate as a raw TimeoutError
        that bypasses the sensor's normal timeout handling."""
        mock_connect.side_effect = TimeoutError("timed out while waiting for handshake response")

        sensor = WebSocketSensor(task_id="handshake_timeout", url=URL, dag=self.dag)
        assert sensor.poke(context={}) is False

    @mock.patch("airflow.providers.standard.sensors.websocket.time.monotonic")
    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_recv_gets_remaining_time_after_slow_handshake(self, mock_connect, mock_monotonic):
        """If the handshake itself consumes part of the timeout budget, recv() must only
        get what's left, not the full timeout again — otherwise total wait time could
        exceed the sensor's declared timeout."""
        mock_websocket = mock.MagicMock(spec=ClientConnection)
        mock_websocket.recv.return_value = "pong"
        mock_connect.return_value.__enter__.return_value = mock_websocket
        # deadline computed at t=0 with timeout=10; connect() "takes" 4s, leaving 6s for recv().
        mock_monotonic.side_effect = [0, 4]

        sensor = WebSocketSensor(task_id="slow_handshake", url=URL, timeout=10, dag=self.dag)
        assert sensor.poke(context={}) is True
        mock_websocket.recv.assert_called_once_with(timeout=6)

    def test_reschedule_mode_not_allowed(self):
        with pytest.raises(ValueError, match="Cannot set mode to 'reschedule'. Only 'poke' is acceptable"):
            WebSocketSensor(task_id="reschedule", url=URL, mode="reschedule", dag=self.dag)

    def test_task_defer_does_not_poke_first(self):
        """The deferrable path must defer immediately: poke() consumes the connection,
        so polling before deferring would send message_to_send and lose the reply the
        trigger is supposed to wait for."""
        sensor = WebSocketSensor(task_id="defer", url=URL, deferrable=True, dag=self.dag)

        with mock.patch.object(WebSocketSensor, "poke", autospec=True) as mock_poke:
            with pytest.raises(TaskDeferred) as exc:
                sensor.execute({})

        mock_poke.assert_not_called()
        assert isinstance(exc.value.trigger, WebSocketTrigger)
        assert exc.value.trigger.url == URL

    def test_execute_sync_calls_poke_exactly_once(self):
        """Since poke() already blocks for the full sensor timeout, execute() must never
        call it a second time — a second call would open a new connection and re-send
        message_to_send."""
        sensor = WebSocketSensor(task_id="sync_timeout", url=URL, timeout=0, dag=self.dag)

        with mock.patch.object(WebSocketSensor, "poke", autospec=True, return_value=False) as mock_poke:
            with pytest.raises(AirflowSensorTimeout):
                sensor.execute({})

        mock_poke.assert_called_once()

    def test_template_fields_are_rendered(self):
        """url, header, and message_to_send commonly need runtime values (run_id, an
        idempotency key, an auth token), so all three must be templated."""
        sensor = WebSocketSensor(
            task_id="templated",
            url="wss://example.com/{{ run_id }}",
            message_to_send='{"run_id": "{{ run_id }}"}',
            header={"Authorization": "Bearer {{ run_id }}"},
            dag=self.dag,
        )
        sensor.render_template_fields({"run_id": "manual__2024-01-01"})

        assert sensor.url == "wss://example.com/manual__2024-01-01"
        assert sensor.message_to_send == '{"run_id": "manual__2024-01-01"}'
        assert sensor.header == {"Authorization": "Bearer manual__2024-01-01"}
