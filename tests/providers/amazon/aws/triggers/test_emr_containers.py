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

import pytest

from airflow.providers.amazon.aws.triggers.emr import EmrContainerOperatorTrigger
from airflow.triggers.base import TriggerEvent
from tests.providers.amazon.aws.utils.compat import AsyncMock, async_mock

VIRTUAL_CLUSTER_ID = "vzwemreks"
JOB_ID = "job-1234"
AWS_CONN_ID = "aws_emr_conn"
POLL_INTERVAL = 60
MAX_ATTEMPTS = 5


class TestEmrContainerSensorTrigger:
    def test_emr_container_operator_trigger_serialize(self):
        emr_trigger = EmrContainerOperatorTrigger(
            virtual_cluster_id=VIRTUAL_CLUSTER_ID,
            job_id=JOB_ID,
            aws_conn_id=AWS_CONN_ID,
            poll_interval=POLL_INTERVAL,
            max_attempts=MAX_ATTEMPTS,
        )
        class_path, args = emr_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.emr.EmrContainerOperatorTrigger"
        assert args["virtual_cluster_id"] == VIRTUAL_CLUSTER_ID
        assert args["job_id"] == JOB_ID
        assert args["aws_conn_id"] == AWS_CONN_ID
        assert args["poll_interval"] == POLL_INTERVAL
        assert args["max_attempts"] == MAX_ATTEMPTS

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.get_waiter")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.async_conn")
    async def test_emr_container_trigger_run(self, mock_async_conn, mock_get_waiter):
        mock = async_mock.MagicMock()
        mock_async_conn.__aenter__.return_value = mock

        mock_get_waiter().wait = AsyncMock()

        emr_trigger = EmrContainerOperatorTrigger(
            virtual_cluster_id=VIRTUAL_CLUSTER_ID,
            job_id=JOB_ID,
            aws_conn_id=AWS_CONN_ID,
            poll_interval=POLL_INTERVAL,
            max_attempts=MAX_ATTEMPTS,
        )

        generator = emr_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Job completed."})
