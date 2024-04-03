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

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.sensors.bedrock import BedrockCustomizeModelCompletedSensor


@pytest.fixture
def mock_get_job_state():
    with mock.patch.object(BedrockHook, "get_customize_model_job_state") as mock_state:
        yield mock_state


class TestBedrockCustomizeModelCompletedSensor:
    def setup_method(self):
        self.job_name = "test_job_name"

        self.default_op_kwargs = dict(
            task_id="test_bedrock_customize_model_sensor",
            job_name=self.job_name,
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = BedrockCustomizeModelCompletedSensor(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = BedrockCustomizeModelCompletedSensor(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = BedrockCustomizeModelCompletedSensor(
            **self.default_op_kwargs,
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

    @pytest.mark.parametrize("state", list(BedrockCustomizeModelCompletedSensor.SUCCESS_STATES))
    def test_poke_success_states(self, state, mock_get_job_state):
        mock_get_job_state.side_effect = [state]
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", list(BedrockCustomizeModelCompletedSensor.INTERMEDIATE_STATES))
    def test_poke_intermediate_states(self, state, mock_get_job_state):
        mock_get_job_state.side_effect = [state]
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize(
        "soft_fail, expected_exception",
        [
            pytest.param(False, AirflowException, id="not-soft-fail"),
            pytest.param(True, AirflowSkipException, id="soft-fail"),
        ],
    )
    @pytest.mark.parametrize("state", list(BedrockCustomizeModelCompletedSensor.FAILURE_STATES))
    def test_poke_failure_states(self, state, soft_fail, expected_exception, mock_get_job_state):
        mock_get_job_state.side_effect = [state]
        sensor = BedrockCustomizeModelCompletedSensor(
            **self.default_op_kwargs, aws_conn_id=None, soft_fail=soft_fail
        )

        with pytest.raises(expected_exception, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})
