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

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.sensors.ssm import SsmRunCommandCompletedSensor

COMMAND_ID = "123e4567-e89b-12d3-a456-426614174000"


@pytest.fixture
def mock_ssm_list_invocations():
    def _setup(mock_conn: mock.MagicMock, state: str):
        mock_conn.list_command_invocations.return_value = {
            "CommandInvocations": [
                {"CommandId": COMMAND_ID, "InstanceId": "i-1234567890abcdef0", "Status": state}
            ]
        }

    return _setup


class TestSsmRunCommandCompletedSensor:
    SENSOR = SsmRunCommandCompletedSensor

    def setup_method(self):
        self.default_op_kwarg = dict(
            task_id="test_ssm_run_command_sensor",
            command_id=COMMAND_ID,
            poke_interval=5,
            max_retries=1,
        )

        self.sensor = self.SENSOR(**self.default_op_kwarg)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwarg)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
            **self.default_op_kwarg,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(SsmHook, "conn")
    def test_poke_success_states(self, mock_conn, state, mock_ssm_list_invocations):
        mock_ssm_list_invocations(mock_conn, state)
        self.sensor.hook.conn = mock_conn
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(SsmHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state, mock_ssm_list_invocations):
        mock_ssm_list_invocations(mock_conn, state)
        self.sensor.hook.conn = mock_conn
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(SsmHook, "conn")
    def test_poke_failure_states(self, mock_conn, state, mock_ssm_list_invocations):
        mock_ssm_list_invocations(mock_conn, state)
        with pytest.raises(RuntimeError, match=self.SENSOR.FAILURE_MESSAGE):
            self.sensor.poke({})
