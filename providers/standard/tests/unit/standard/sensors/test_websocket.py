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
        mock_connect.return_value = mock_websocket

        sensor = WebSocketSensor(task_id="poke_true", url=URL, message_to_send="ping", dag=self.dag)
        assert sensor.poke(context={}) is True
        mock_websocket.send.assert_called_once_with("ping")
        mock_websocket.close.assert_called_once()
        assert sensor._connection is None

    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_returns_false_on_timeout(self, mock_connect):
        mock_websocket = mock.MagicMock(spec=ClientConnection)
        mock_websocket.recv.side_effect = TimeoutError()
        mock_connect.return_value = mock_websocket

        sensor = WebSocketSensor(task_id="poke_false", url=URL, dag=self.dag)
        assert sensor.poke(context={}) is False
        mock_websocket.close.assert_not_called()

    @mock.patch("airflow.providers.standard.sensors.websocket.connect", autospec=True)
    def test_poke_reuses_connection_and_sends_message_once(self, mock_connect):
        """A second poke() after a timeout must not reopen the connection or re-send
        message_to_send — WebSocket reads are consumptive, so resending would restart
        the remote job the sensor is waiting on."""
        mock_websocket = mock.MagicMock(spec=ClientConnection)
        mock_websocket.recv.side_effect = [TimeoutError(), "pong"]
        mock_connect.return_value = mock_websocket

        sensor = WebSocketSensor(task_id="reuse", url=URL, message_to_send="ping", dag=self.dag)
        assert sensor.poke(context={}) is False
        assert sensor.poke(context={}) is True

        mock_connect.assert_called_once()
        mock_websocket.send.assert_called_once_with("ping")

    def test_execute_closes_connection_left_open_on_timeout(self):
        """If the sensor loop times out, execute() must close whatever connection poke()
        left open rather than leaking it."""
        sensor = WebSocketSensor(task_id="timeout", url=URL, timeout=0, dag=self.dag)
        fake_connection = mock.MagicMock(spec=ClientConnection)

        def fake_poke(context):
            sensor._connection = fake_connection
            return False

        with mock.patch.object(WebSocketSensor, "poke", side_effect=fake_poke):
            with pytest.raises(AirflowSensorTimeout):
                sensor.execute({})

        fake_connection.close.assert_called_once()
        assert sensor._connection is None

    def test_reschedule_mode_not_allowed(self):
        with pytest.raises(ValueError, match="Cannot set mode to 'reschedule'. Only 'poke' is acceptable"):
            WebSocketSensor(task_id="reschedule", url=URL, mode="reschedule", dag=self.dag)

    def test_task_defer_does_not_poke_first(self):
        """The deferrable path must defer immediately: poke() consumes the connection,
        so polling before deferring would send message_to_send and lose the reply the
        trigger is supposed to wait for."""
        sensor = WebSocketSensor(task_id="defer", url=URL, deferrable=True, dag=self.dag)

        with mock.patch.object(WebSocketSensor, "poke") as mock_poke:
            with pytest.raises(TaskDeferred) as exc:
                sensor.execute({})

        mock_poke.assert_not_called()
        assert isinstance(exc.value.trigger, WebSocketTrigger)
        assert exc.value.trigger.url == URL

    def test_execute_sync_returns_after_one_poke(self):
        """The non-deferrable path must return once the sensor loop succeeds, not poke
        (and consume) a second WebSocket message."""
        sensor = WebSocketSensor(task_id="sync", url=URL, timeout=0, dag=self.dag)

        with mock.patch.object(WebSocketSensor, "poke", return_value=True) as mock_poke:
            sensor.execute({})

        mock_poke.assert_called_once()
