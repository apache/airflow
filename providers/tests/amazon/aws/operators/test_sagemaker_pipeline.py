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

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerStartPipelineOperator,
    SageMakerStopPipelineOperator,
)
from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerPipelineTrigger

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from unittest.mock import MagicMock


class TestSageMakerStartPipelineOperator:
    @mock.patch.object(SageMakerHook, "start_pipeline")
    @mock.patch.object(SageMakerHook, "check_status")
    def test_execute(self, check_status, start_pipeline):
        op = SageMakerStartPipelineOperator(
            task_id="test_sagemaker_operator",
            pipeline_name="my_pipeline",
            display_name="test_disp_name",
            pipeline_params={"is_a_test": "yes"},
            wait_for_completion=True,
            check_interval=12,
            verbose=False,
        )

        op.execute({})

        start_pipeline.assert_called_once_with(
            pipeline_name="my_pipeline",
            display_name="test_disp_name",
            pipeline_params={"is_a_test": "yes"},
        )
        check_status.assert_called_once()

    @mock.patch.object(SageMakerHook, "start_pipeline")
    def test_defer(self, start_mock):
        op = SageMakerStartPipelineOperator(
            task_id="test_sagemaker_operator",
            pipeline_name="my_pipeline",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as defer:
            op.execute({})

        assert isinstance(defer.value.trigger, SageMakerPipelineTrigger)
        assert defer.value.trigger.waiter_type == SageMakerPipelineTrigger.Type.COMPLETE

    def test_template_fields(self):
        operator = SageMakerStartPipelineOperator(
            task_id="test_sagemaker_operator",
            pipeline_name="my_pipeline",
        )
        validate_template_fields(operator)


class TestSageMakerStopPipelineOperator:
    @mock.patch.object(SageMakerHook, "stop_pipeline")
    def test_execute(self, stop_pipeline):
        op = SageMakerStopPipelineOperator(
            task_id="test_sagemaker_operator", pipeline_exec_arn="pipeline_arn"
        )

        op.execute({})

        stop_pipeline.assert_called_once_with(
            pipeline_exec_arn="pipeline_arn",
            fail_if_not_running=False,
        )

    @mock.patch.object(SageMakerHook, "stop_pipeline")
    def test_defer(self, stop_mock: MagicMock):
        stop_mock.return_value = "Stopping"
        op = SageMakerStopPipelineOperator(
            task_id="test_sagemaker_operator",
            pipeline_exec_arn="my_pipeline_arn",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as defer:
            op.execute({})

        assert isinstance(defer.value.trigger, SageMakerPipelineTrigger)
        assert defer.value.trigger.waiter_type == SageMakerPipelineTrigger.Type.STOPPED

    def test_template_fields(self):
        operator = SageMakerStopPipelineOperator(
            task_id="test_sagemaker_operator",
            pipeline_exec_arn="my_pipeline_arn",
            deferrable=True,
        )
        validate_template_fields(operator)
