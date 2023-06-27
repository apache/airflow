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

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger


class TestAthenaTrigger:
    @pytest.mark.asyncio
    @mock.patch.object(AthenaHook, "get_waiter")
    @mock.patch.object(AthenaHook, "async_conn")  # LatestBoto step of CI fails without this
    async def test_run_with_error(self, conn_mock, waiter_mock):
        waiter_mock.side_effect = WaiterError("name", "reason", {})

        trigger = AthenaTrigger("query_id", 0, 5, None)

        with pytest.raises(WaiterError):
            generator = trigger.run()
            await generator.asend(None)

    @pytest.mark.asyncio
    @mock.patch.object(AthenaHook, "get_waiter")
    @mock.patch.object(AthenaHook, "async_conn")  # LatestBoto step of CI fails without this
    async def test_run_success(self, conn_mock, waiter_mock):
        waiter_mock().wait = AsyncMock()
        trigger = AthenaTrigger("my_query_id", 0, 5, None)

        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["value"] == "my_query_id"
