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

from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor
from airflow.providers.common.compat.sdk import AirflowException


@pytest.fixture
def mocked_get_task_description():
    with mock.patch.object(DmsHook, "get_task_description", autospec=True) as m:
        yield m


class TestDmsTaskCompletedSensor:
    def setup_method(self):
        self.default_op_kwargs = {
            "task_id": "test_dms_sensor",
            "aws_conn_id": None,
            "replication_task_arn": "task_arn",
        }

    def test_init(self):
        self.default_op_kwargs.pop("aws_conn_id", None)

        sensor = DmsTaskCompletedSensor(
            **self.default_op_kwargs,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="ca-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert sensor.hook.client_type == "dms"
        assert sensor.hook.resource_type is None
        assert sensor.hook.aws_conn_id == "fake-conn-id"
        assert sensor.hook._region_name == "ca-west-1"
        assert sensor.hook._verify is True
        assert sensor.hook._config is not None
        assert sensor.hook._config.read_timeout == 42

        sensor = DmsTaskCompletedSensor(task_id="create_task", replication_task_arn="task_arn")
        assert sensor.hook.aws_conn_id == "aws_default"
        assert sensor.hook._region_name is None
        assert sensor.hook._verify is None
        assert sensor.hook._config is None

    @pytest.mark.parametrize("status", ["stopped"])
    def test_poke_completed(self, mocked_get_task_description, status):
        mocked_get_task_description.return_value = {"Status": status}
        assert DmsTaskCompletedSensor(**self.default_op_kwargs).poke({})

    @pytest.mark.parametrize("status", ["running", "starting"])
    def test_poke_not_completed(self, mocked_get_task_description, status):
        mocked_get_task_description.return_value = {"Status": status}
        assert not DmsTaskCompletedSensor(**self.default_op_kwargs).poke({})

    @pytest.mark.parametrize(
        "status",
        [
            "creating",
            "deleting",
            "failed",
            "failed-move",
            "modifying",
            "moving",
            "ready",
            "testing",
        ],
    )
    def test_poke_terminated_status(self, mocked_get_task_description, status):
        mocked_get_task_description.return_value = {"Status": status}
        with pytest.raises(AirflowException) as ctx:
            DmsTaskCompletedSensor(**self.default_op_kwargs).poke({})
        assert str(ctx.value) == f"Unexpected status: {status}"

    def test_poke_terminated_status_includes_reason(self, mocked_get_task_description):
        mocked_get_task_description.return_value = {
            "Status": "failed",
            "StopReason": "Stop Reason FATAL_ERROR",
            "LastFailureMessage": "Last Error",
        }
        with pytest.raises(AirflowException) as ctx:
            DmsTaskCompletedSensor(**self.default_op_kwargs).poke({})
        assert (
            str(ctx.value)
            == "Unexpected status: failed (StopReason: Stop Reason FATAL_ERROR, LastFailureMessage: Last Error)"
        )

    def test_poke_none_status(self, mocked_get_task_description):
        mocked_get_task_description.return_value = None
        with pytest.raises(AirflowException, match="task with ARN .* not found"):
            DmsTaskCompletedSensor(**self.default_op_kwargs).poke({})
