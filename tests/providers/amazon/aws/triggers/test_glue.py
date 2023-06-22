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

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.triggers.glue import GlueJobCompleteTrigger


class TestGlueJobTrigger:
    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "async_get_job_state")
    async def test_wait_job(self, get_state_mock: mock.MagicMock):
        GlueJobHook.JOB_POLL_INTERVAL = 0.1
        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="JobRunId",
            verbose=False,
            aws_conn_id="aws_conn_id",
        )
        get_state_mock.side_effect = [
            "RUNNING",
            "RUNNING",
            "SUCCEEDED",
        ]

        generator = trigger.run()
        event = await generator.asend(None)

        assert get_state_mock.call_count == 3
        assert event.payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "async_get_job_state")
    async def test_wait_job_failed(self, get_state_mock: mock.MagicMock):
        GlueJobHook.JOB_POLL_INTERVAL = 0.1
        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="JobRunId",
            verbose=False,
            aws_conn_id="aws_conn_id",
        )
        get_state_mock.side_effect = [
            "RUNNING",
            "RUNNING",
            "FAILED",
        ]

        with pytest.raises(AirflowException):
            await trigger.run().asend(None)

        assert get_state_mock.call_count == 3
