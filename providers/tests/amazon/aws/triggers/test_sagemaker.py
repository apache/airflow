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

from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerTrigger
from airflow.triggers.base import TriggerEvent

JOB_NAME = "job_name"
JOB_TYPE = "job_type"
AWS_CONN_ID = "aws_sagemaker_conn"
POKE_INTERVAL = 30
MAX_ATTEMPTS = 60


class TestSagemakerTrigger:
    def test_sagemaker_trigger_serialize(self):
        sagemaker_trigger = SageMakerTrigger(
            job_name=JOB_NAME,
            job_type=JOB_TYPE,
            poke_interval=POKE_INTERVAL,
            max_attempts=MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )
        class_path, args = sagemaker_trigger.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.sagemaker.SageMakerTrigger"
        )
        assert args["job_name"] == JOB_NAME
        assert args["job_type"] == JOB_TYPE
        assert args["poke_interval"] == POKE_INTERVAL
        assert args["max_attempts"] == MAX_ATTEMPTS
        assert args["aws_conn_id"] == AWS_CONN_ID

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
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.async_conn")
    async def test_sagemaker_trigger_run_all_job_types(
        self, mock_async_conn, mock_get_waiter, job_type
    ):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()

        mock_get_waiter().wait = AsyncMock()

        sagemaker_trigger = SageMakerTrigger(
            job_name=JOB_NAME,
            job_type=job_type,
            poke_interval=POKE_INTERVAL,
            max_attempts=MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
        )

        generator = sagemaker_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "message": "Job completed.", "job_name": JOB_NAME}
        )
