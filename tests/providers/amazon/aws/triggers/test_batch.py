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

from airflow.providers.amazon.aws.triggers.batch import BatchOperatorTrigger
from airflow.triggers.base import TriggerEvent

BATCH_JOB_ID = "job_id"
POLL_INTERVAL = 5
MAX_ATTEMPT = 5
AWS_CONN_ID = "aws_batch_job_conn"
AWS_REGION = "us-east-2"


class TestBatchOperatorTrigger:
    def test_batch_operator_trigger_serialize(self):
        batch_trigger = BatchOperatorTrigger(
            job_id=BATCH_JOB_ID,
            poll_interval=POLL_INTERVAL,
            max_retries=MAX_ATTEMPT,
            aws_conn_id=AWS_CONN_ID,
            region_name=AWS_REGION,
        )
        class_path, args = batch_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.batch.BatchOperatorTrigger"
        assert args["job_id"] == BATCH_JOB_ID
        assert args["poll_interval"] == POLL_INTERVAL
        assert args["max_retries"] == MAX_ATTEMPT
        assert args["aws_conn_id"] == AWS_CONN_ID
        assert args["region_name"] == AWS_REGION

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook.async_conn")
    async def test_batch_job_trigger_run(self, mock_async_conn, mock_get_waiter):
        the_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = the_mock

        mock_get_waiter().wait = AsyncMock()

        batch_trigger = BatchOperatorTrigger(
            job_id=BATCH_JOB_ID,
            poll_interval=POLL_INTERVAL,
            max_retries=MAX_ATTEMPT,
            aws_conn_id=AWS_CONN_ID,
            region_name=AWS_REGION,
        )

        generator = batch_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "job_id": BATCH_JOB_ID})
