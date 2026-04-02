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

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook, BedrockHook
from airflow.providers.amazon.aws.sensors.bedrock import (
    BedrockBatchInferenceSensor,
    BedrockCustomizeModelCompletedSensor,
    BedrockIngestionJobSensor,
    BedrockKnowledgeBaseActiveSensor,
    BedrockProvisionModelThroughputCompletedSensor,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred


class TestBedrockCustomizeModelCompletedSensor:
    SENSOR = BedrockCustomizeModelCompletedSensor

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_customize_model_sensor",
            job_name="job_name",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.get_model_customization_job.return_value = {"status": state}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.get_model_customization_job.return_value = {"status": state}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.get_model_customization_job.return_value = {"status": state}
        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)
        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})


class TestBedrockProvisionModelThroughputCompletedSensor:
    SENSOR = BedrockProvisionModelThroughputCompletedSensor

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_provision_model_sensor",
            model_id="provisioned_model_arn",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.get_provisioned_model_throughput.return_value = {"status": state}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.get_provisioned_model_throughput.return_value = {"status": state}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(BedrockHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.get_provisioned_model_throughput.return_value = {"status": state}
        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})


class TestBedrockKnowledgeBaseActiveSensor:
    SENSOR = BedrockKnowledgeBaseActiveSensor

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_knowledge_base_active_sensor",
            knowledge_base_id="knowledge_base_id",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(BedrockAgentHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.get_knowledge_base.return_value = {"knowledgeBase": {"status": state}}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(BedrockAgentHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.get_knowledge_base.return_value = {"knowledgeBase": {"status": state}}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(BedrockAgentHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.get_knowledge_base.return_value = {"knowledgeBase": {"status": state}}
        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)
        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})


class TestBedrockIngestionJobSensor:
    SENSOR = BedrockIngestionJobSensor

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_knowledge_base_active_sensor",
            knowledge_base_id="knowledge_base_id",
            data_source_id="data_source_id",
            ingestion_job_id="ingestion_job_id",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(BedrockAgentHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.get_ingestion_job.return_value = {"ingestionJob": {"status": state}}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(BedrockAgentHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.get_ingestion_job.return_value = {"ingestionJob": {"status": state}}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(BedrockAgentHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.get_ingestion_job.return_value = {"ingestionJob": {"status": state}}
        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)
        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})


class TestBedrockBatchInferenceSensor:
    SENSOR = BedrockBatchInferenceSensor

    @pytest.fixture(
        params=[
            BedrockBatchInferenceSensor.SuccessState.COMPLETED,
            BedrockBatchInferenceSensor.SuccessState.SCHEDULED,
        ]
    )
    def success_state(self, request):
        return request.param

    @pytest.fixture(params=["deferrable", "not deferrable"])
    def is_deferrable(self, request):
        # I did it this way instead of passing True/False purely so the pytest names are more descriptive.
        return request.param == "deferrable"

    def setup_method(self, is_deferrable):
        self.default_op_kwargs = dict(
            task_id="test_bedrock_batch_inference_sensor",
            job_arn="job_arn",
            deferrable=is_deferrable,
        )

    def test_base_aws_op_attributes(self, success_state):
        op = self.SENSOR(**self.default_op_kwargs, success_state=success_state)

        if success_state == BedrockBatchInferenceSensor.SuccessState.COMPLETED:
            assert "Scheduled" in op.INTERMEDIATE_STATES
            assert "Scheduled" not in op.SUCCESS_STATES
        elif success_state == BedrockBatchInferenceSensor.SuccessState.SCHEDULED:
            assert "Scheduled" in op.SUCCESS_STATES
            assert "Scheduled" not in op.INTERMEDIATE_STATES
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @mock.patch.object(BedrockHook, "conn")
    def test_poke_success_states(self, mock_conn, success_state, is_deferrable):
        op = self.SENSOR(**self.default_op_kwargs, success_state=success_state)

        for state in op.SUCCESS_STATES:
            mock_conn.get_model_invocation_job.return_value = {"status": state}

            if is_deferrable:
                with pytest.raises(TaskDeferred):
                    op.execute({})
            else:
                assert op.poke({}) is True

    @mock.patch.object(BedrockHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, success_state, is_deferrable):
        op = self.SENSOR(**self.default_op_kwargs, success_state=success_state)

        for state in op.INTERMEDIATE_STATES:
            mock_conn.get_model_invocation_job.return_value = {"status": state}

            if is_deferrable:
                with pytest.raises(TaskDeferred):
                    op.execute({})
            else:
                assert op.poke({}) is False

    @mock.patch.object(BedrockHook, "conn")
    def test_poke_failure_states(self, mock_conn, success_state, is_deferrable):
        op = self.SENSOR(**self.default_op_kwargs, success_state=success_state)

        for state in op.FAILURE_STATES:
            mock_conn.get_model_invocation_job.return_value = {"status": state}

            if is_deferrable:
                with pytest.raises(AirflowException, match=op.FAILURE_MESSAGE):
                    op.poke({})
            else:
                with pytest.raises(TaskDeferred):
                    op.execute({})
