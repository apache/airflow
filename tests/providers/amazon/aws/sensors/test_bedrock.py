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
from airflow.providers.amazon.aws.sensors.bedrock import (
    BedrockCustomizeModelCompletedSensor,
    BedrockProvisionModelThroughputCompletedSensor,
)


class TestBedrockCustomizeModelCompletedSensor:
    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_customize_model_sensor",
            job_name="job_name",
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
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.get_model_customization_job.return_value = {"status": state}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", list(BedrockCustomizeModelCompletedSensor.INTERMEDIATE_STATES))
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.get_model_customization_job.return_value = {"status": state}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize(
        "soft_fail, expected_exception",
        [
            pytest.param(False, AirflowException, id="not-soft-fail"),
            pytest.param(True, AirflowSkipException, id="soft-fail"),
        ],
    )
    @pytest.mark.parametrize("state", list(BedrockCustomizeModelCompletedSensor.FAILURE_STATES))
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_failure_states(self, mock_conn, state, soft_fail, expected_exception):
        mock_conn.get_model_customization_job.return_value = {"status": state}
        sensor = BedrockCustomizeModelCompletedSensor(
            **self.default_op_kwargs, aws_conn_id=None, soft_fail=soft_fail
        )
        with pytest.raises(expected_exception, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})


class TestBedrockProvisionModelThroughputCompletedSensor:
    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_provision_model_sensor",
            model_id="provisioned_model_arn",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = BedrockProvisionModelThroughputCompletedSensor(
            **self.default_op_kwargs, aws_conn_id=None
        )

    def test_base_aws_op_attributes(self):
        op = BedrockProvisionModelThroughputCompletedSensor(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = BedrockProvisionModelThroughputCompletedSensor(
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

    @pytest.mark.parametrize("state", list(BedrockProvisionModelThroughputCompletedSensor.SUCCESS_STATES))
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.get_provisioned_model_throughput.return_value = {"status": state}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize(
        "state", list(BedrockProvisionModelThroughputCompletedSensor.INTERMEDIATE_STATES)
    )
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.get_provisioned_model_throughput.return_value = {"status": state}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize(
        "soft_fail, expected_exception",
        [
            pytest.param(False, AirflowException, id="not-soft-fail"),
            pytest.param(True, AirflowSkipException, id="soft-fail"),
        ],
    )
    @pytest.mark.parametrize("state", list(BedrockProvisionModelThroughputCompletedSensor.FAILURE_STATES))
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_failure_states(self, mock_conn, state, soft_fail, expected_exception):
        mock_conn.get_provisioned_model_throughput.return_value = {"status": state}
        sensor = BedrockProvisionModelThroughputCompletedSensor(
            **self.default_op_kwargs, aws_conn_id=None, soft_fail=soft_fail
        )

        with pytest.raises(expected_exception, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})
