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

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.looker import JobStatus
from airflow.providers.google.cloud.sensors.looker import LookerCheckPdtBuildSensor

SENSOR_PATH = "airflow.providers.google.cloud.sensors.looker.{}"

TASK_ID = "task-id"
LOOKER_CONN_ID = "test-conn"

TEST_JOB_ID = "123"


class TestLookerCheckPdtBuildSensor:
    @mock.patch(SENSOR_PATH.format("LookerHook"))
    def test_done(self, mock_hook):
        mock_hook.return_value.pdt_build_status.return_value = {"status": JobStatus.DONE.value}

        # run task in mock context
        sensor = LookerCheckPdtBuildSensor(
            task_id=TASK_ID,
            looker_conn_id=LOOKER_CONN_ID,
            materialization_id=TEST_JOB_ID,
        )
        ret = sensor.poke(context={})

        # assert hook.pdt_build_status called once
        mock_hook.return_value.pdt_build_status.assert_called_once_with(materialization_id=TEST_JOB_ID)

        # assert we got a response
        assert ret

    @mock.patch(SENSOR_PATH.format("LookerHook"))
    def test_error(self, mock_hook):
        mock_hook.return_value.pdt_build_status.return_value = {
            "status": JobStatus.ERROR.value,
            "message": "test",
        }

        # run task in mock context
        sensor = LookerCheckPdtBuildSensor(
            task_id=TASK_ID,
            looker_conn_id=LOOKER_CONN_ID,
            materialization_id=TEST_JOB_ID,
        )

        with pytest.raises(AirflowException, match="PDT materialization job failed"):
            sensor.poke(context={})

        # assert hook.pdt_build_status called once
        mock_hook.return_value.pdt_build_status.assert_called_once_with(materialization_id=TEST_JOB_ID)

    @mock.patch(SENSOR_PATH.format("LookerHook"))
    def test_wait(self, mock_hook):
        mock_hook.return_value.pdt_build_status.return_value = {"status": JobStatus.RUNNING.value}

        # run task in mock context
        sensor = LookerCheckPdtBuildSensor(
            task_id=TASK_ID,
            looker_conn_id=LOOKER_CONN_ID,
            materialization_id=TEST_JOB_ID,
        )
        ret = sensor.poke(context={})

        # assert hook.pdt_build_status called once
        mock_hook.return_value.pdt_build_status.assert_called_once_with(materialization_id=TEST_JOB_ID)

        # assert we got NO response
        assert not ret

    @mock.patch(SENSOR_PATH.format("LookerHook"))
    def test_cancelled(self, mock_hook):
        mock_hook.return_value.pdt_build_status.return_value = {"status": JobStatus.CANCELLED.value}

        # run task in mock context
        sensor = LookerCheckPdtBuildSensor(
            task_id=TASK_ID,
            looker_conn_id=LOOKER_CONN_ID,
            materialization_id=TEST_JOB_ID,
        )

        with pytest.raises(AirflowException, match="PDT materialization job was cancelled"):
            sensor.poke(context={})

        # assert hook.pdt_build_status called once
        mock_hook.return_value.pdt_build_status.assert_called_once_with(materialization_id=TEST_JOB_ID)

    def test_empty_materialization_id(self):
        # run task in mock context
        sensor = LookerCheckPdtBuildSensor(
            task_id=TASK_ID, looker_conn_id=LOOKER_CONN_ID, materialization_id=""
        )

        with pytest.raises(AirflowException, match="^Invalid `materialization_id`.$"):
            sensor.poke(context={})
