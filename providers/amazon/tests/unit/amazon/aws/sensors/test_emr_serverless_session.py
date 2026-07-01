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

from unittest.mock import MagicMock

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessSessionSensor
from airflow.providers.amazon.aws.triggers.emr import EmrServerlessSessionTrigger


class TestEmrServerlessSessionSensor:
    def setup_method(self):
        self.app_id = "app-123"
        self.session_id = "sess-abc"
        self.sensor = EmrServerlessSessionSensor(
            task_id="test_emr_serverless_session_sensor",
            application_id=self.app_id,
            session_id=self.session_id,
            aws_conn_id="aws_default",
        )

    def set_session_state(self, state: str):
        self.mock_hook = MagicMock()
        self.mock_hook.get_session_state.return_value = state
        self.sensor.hook = self.mock_hook

    def assert_get_session_state_called_once(self):
        self.mock_hook.get_session_state.assert_called_once_with(self.app_id, self.session_id)

    @pytest.mark.parametrize(
        ("state", "expected_result"),
        [
            ("SUBMITTED", False),
            ("STARTING", False),
            ("STARTED", True),
            ("IDLE", True),
        ],
    )
    def test_poke_returns_expected_result_for_states(self, state, expected_result):
        self.set_session_state(state)
        assert self.sensor.poke(None) == expected_result
        self.assert_get_session_state_called_once()

    @pytest.mark.parametrize("state", ["FAILED", "TERMINATING", "TERMINATED"])
    def test_poke_raises_for_failure_states(self, state):
        self.set_session_state(state)
        with pytest.raises(RuntimeError, match=f"failure state: {state}"):
            self.sensor.poke(None)
        self.assert_get_session_state_called_once()

    def test_deferrable_defers(self):
        sensor = EmrServerlessSessionSensor(
            task_id="deferred_session_sensor",
            application_id=self.app_id,
            session_id=self.session_id,
            aws_conn_id="aws_default",
            deferrable=True,
        )
        mock_hook = MagicMock()
        mock_hook.get_session_state.return_value = "STARTING"
        sensor.hook = mock_hook

        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute(None)

        assert isinstance(deferred.value.trigger, EmrServerlessSessionTrigger)

    def test_execute_complete_failure_raises(self):
        with pytest.raises(RuntimeError):
            self.sensor.execute_complete({}, {"status": "failure"})
