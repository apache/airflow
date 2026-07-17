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
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerPipelineTrigger, SageMakerTrigger
from airflow.triggers.base import TriggerEvent

JOB_NAME = "job_name"
JOB_TYPE = "training"
AWS_CONN_ID = "aws_sagemaker_conn"
WAITER_DELAY = 30
WAITER_MAX_ATTEMPTS = 60
REGION_NAME = "us-west-2"
PIPELINE_ARN = "arn:aws:sagemaker:us-west-2:123456789012:pipeline/my-pipeline/execution/abc"


class TestSagemakerTrigger:
    def test_sagemaker_trigger_serialize(self):
        sagemaker_trigger = SageMakerTrigger(
            job_name=JOB_NAME,
            job_type=JOB_TYPE,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
        )
        class_path, args = sagemaker_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.sagemaker.SageMakerTrigger"
        assert args["job_name"] == JOB_NAME
        assert args["job_type"] == JOB_TYPE
        assert args["waiter_delay"] == WAITER_DELAY
        assert args["waiter_max_attempts"] == WAITER_MAX_ATTEMPTS
        assert args["aws_conn_id"] == AWS_CONN_ID
        assert args["region_name"] == REGION_NAME

    @pytest.mark.parametrize(
        ("deprecated_kwarg", "canonical_attr", "value"),
        [
            ("poke_interval", "waiter_delay", 17),
            ("max_attempts", "attempts", 21),
        ],
    )
    def test_sagemaker_trigger_deprecated_params(self, deprecated_kwarg, canonical_attr, value):
        with pytest.warns(AirflowProviderDeprecationWarning, match=deprecated_kwarg):
            trigger = SageMakerTrigger(
                job_name=JOB_NAME,
                job_type=JOB_TYPE,
                aws_conn_id=AWS_CONN_ID,
                **{deprecated_kwarg: value},
            )
        assert getattr(trigger, canonical_attr) == value

    def test_sagemaker_trigger_hook_uses_generic_params(self):
        sagemaker_trigger = SageMakerTrigger(
            job_name=JOB_NAME,
            job_type=JOB_TYPE,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            verify=False,
            botocore_config={"read_timeout": 10},
        )
        hook = sagemaker_trigger.hook()
        assert isinstance(hook, SageMakerHook)
        assert hook.aws_conn_id == AWS_CONN_ID
        assert hook._region_name == REGION_NAME
        assert hook._verify is False
        assert hook._config.read_timeout == 10

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "job_type",
        [
            "training",
            "transform",
            "processing",
            "tuning",
            "endpoint",
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_async_conn")
    async def test_sagemaker_trigger_run_all_job_types(self, mock_async_conn, mock_get_waiter, job_type):
        mock_async_conn.return_value.__aenter__.return_value = mock.MagicMock()

        mock_get_waiter().wait = AsyncMock()

        sagemaker_trigger = SageMakerTrigger(
            job_name=JOB_NAME,
            job_type=job_type,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )

        generator = sagemaker_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "job_name": JOB_NAME})


class TestSagemakerPipelineTrigger:
    def test_serialize(self):
        trigger = SageMakerPipelineTrigger(
            waiter_type=SageMakerPipelineTrigger.Type.COMPLETE,
            pipeline_execution_arn=PIPELINE_ARN,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )
        class_path, args = trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.sagemaker.SageMakerPipelineTrigger"
        assert args["waiter_type"] == SageMakerPipelineTrigger.Type.COMPLETE.value
        assert args["pipeline_execution_arn"] == PIPELINE_ARN
        assert args["waiter_delay"] == WAITER_DELAY
        assert args["waiter_max_attempts"] == WAITER_MAX_ATTEMPTS
        assert args["aws_conn_id"] == AWS_CONN_ID

    def test_deserialize_accepts_int_waiter_type(self):
        # On deserialization the waiter_type is passed back as the stored int value.
        trigger = SageMakerPipelineTrigger(
            waiter_type=SageMakerPipelineTrigger.Type.STOPPED.value,
            pipeline_execution_arn=PIPELINE_ARN,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )
        assert trigger.waiter_type == SageMakerPipelineTrigger.Type.STOPPED
        assert trigger.waiter_name == "PipelineExecutionStopped"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()

        trigger = SageMakerPipelineTrigger(
            waiter_type=SageMakerPipelineTrigger.Type.COMPLETE,
            pipeline_execution_arn=PIPELINE_ARN,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )

        response = await trigger.run().asend(None)

        assert response == TriggerEvent({"status": "success", "value": PIPELINE_ARN})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.sagemaker.asyncio.sleep")
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_async_conn")
    async def test_run_logs_steps_then_succeeds(self, mock_async_conn, mock_get_waiter, mock_sleep):
        conn = mock.MagicMock()
        conn.list_pipeline_execution_steps = AsyncMock(
            return_value={
                "PipelineExecutionSteps": [
                    {"StepName": "step-1", "StepStatus": "Executing"},
                    {"StepName": "step-2", "StepStatus": "Succeeded"},
                ]
            }
        )
        mock_async_conn.return_value.__aenter__.return_value = conn
        mock_sleep.return_value = None

        non_terminal_error = WaiterError(
            name="PipelineExecutionComplete",
            reason="not done yet",
            last_response={"PipelineExecutionStatus": "Executing"},
        )
        mock_get_waiter().wait = AsyncMock(side_effect=[non_terminal_error, None])

        trigger = SageMakerPipelineTrigger(
            waiter_type=SageMakerPipelineTrigger.Type.COMPLETE,
            pipeline_execution_arn=PIPELINE_ARN,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )

        response = await trigger.run().asend(None)

        assert response == TriggerEvent({"status": "success", "value": PIPELINE_ARN})
        conn.list_pipeline_execution_steps.assert_awaited_once_with(PipelineExecutionArn=PIPELINE_ARN)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.get_async_conn")
    async def test_run_raises_on_terminal_failure(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = mock.MagicMock()
        terminal_error = WaiterError(
            name="PipelineExecutionComplete",
            reason="terminal failure",
            last_response={"PipelineExecutionStatus": "Failed"},
        )
        mock_get_waiter().wait = AsyncMock(side_effect=terminal_error)

        trigger = SageMakerPipelineTrigger(
            waiter_type=SageMakerPipelineTrigger.Type.COMPLETE,
            pipeline_execution_arn=PIPELINE_ARN,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )

        with pytest.raises(WaiterError, match="terminal failure"):
            await trigger.run().asend(None)
