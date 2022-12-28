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

from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerStartPipelineOperator,
    SageMakerStopPipelineOperator,
)


class TestSageMakerStartPipelineOperator:
    @mock.patch.object(SageMakerHook, "start_pipeline")
    def test_execute(self, start_pipeline):
        op = SageMakerStartPipelineOperator(
            task_id="test_sagemaker_operator",
            pipeline_name="my_pipeline",
            display_name="test_disp_name",
            pipeline_params={"is_a_test": "yes"},
            wait_for_completion=True,
            check_interval=12,
            verbose=False,
        )

        op.execute(None)

        start_pipeline.assert_called_once_with(
            pipeline_name="my_pipeline",
            display_name="test_disp_name",
            pipeline_params={"is_a_test": "yes"},
            wait_for_completion=True,
            check_interval=12,
            verbose=False,
        )


class TestSageMakerStopPipelineOperator:
    @mock.patch.object(SageMakerHook, "stop_pipeline")
    def test_execute(self, stop_pipeline):
        op = SageMakerStopPipelineOperator(
            task_id="test_sagemaker_operator", pipeline_exec_arn="pipeline_arn"
        )

        op.execute(None)

        stop_pipeline.assert_called_once_with(
            pipeline_exec_arn="pipeline_arn",
            wait_for_completion=False,
            check_interval=30,
            fail_if_not_running=False,
            verbose=True,
        )
