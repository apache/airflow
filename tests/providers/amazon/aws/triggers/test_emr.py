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

from unittest.mock import PropertyMock

import pytest

from airflow.providers.amazon.aws.triggers.emr import EmrStepSensorTrigger
from tests.providers.amazon.aws.utils.compat import AsyncMock, async_mock

JOB__FLOW_ID = "job-1234"
STEP_ID = "s-1234"
TARGET_STATE = ["TERMINATED"]
AWS_CONN_ID = "aws_emr_conn"
POLL_INTERVAL = 60
MAX_ATTEMPTS = 5


class TestEmrStepSensorTrigger:
    def test_emr_job_step_sensor_trigger_serialize(self):
        emr_trigger = EmrStepSensorTrigger(
            job_flow_id=JOB__FLOW_ID,
            step_id=STEP_ID,
            target_states=TARGET_STATE,
            aws_conn_id=AWS_CONN_ID,
            poll_interval=POLL_INTERVAL,
            max_attempts=MAX_ATTEMPTS,
        )
        class_path, args = emr_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger"
        assert args["job_flow_id"] == JOB__FLOW_ID
        assert args["step_id"] == STEP_ID
        assert args["target_states"] == TARGET_STATE
        assert args["aws_conn_id"] == AWS_CONN_ID
        assert args["poll_interval"] == POLL_INTERVAL
        assert args["max_attempts"] == MAX_ATTEMPTS

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook.conn", new_callable=PropertyMock
    )
    @async_mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.get_waiter")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.async_conn")
    async def test_emr_step_sensor_trigger_run(self, mock_async_conn, mock_get_waiter, mock_conn):
        mock = async_mock.MagicMock()
        mock_async_conn.__aenter__.return_value = mock

        mock_get_waiter().wait = AsyncMock()

        emr_trigger = EmrStepSensorTrigger(
            job_flow_id=JOB__FLOW_ID,
            target_states=TARGET_STATE,
            step_id=STEP_ID,
            aws_conn_id=AWS_CONN_ID,
            poll_interval=POLL_INTERVAL,
            max_attempts=MAX_ATTEMPTS,
        )

        generator = emr_trigger.run()
        await generator.asend(None)

        assert mock_conn.return_value.describe_step.called
