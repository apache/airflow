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

import asyncio
import sys

import pytest

from airflow.providers.apache.hive.triggers.hive_partition import HivePartitionTrigger
from airflow.triggers.base import TriggerEvent

if sys.version_info < (3, 8):
    from asynctest import mock
    from asynctest.mock import CoroutineMock as AsyncMock
else:
    from unittest import mock
    from unittest.mock import AsyncMock

TEST_TABLE = "test_table"
TEST_SCHEMA = "test_schema"
TEST_POLLING_INTERVAL = 5
TEST_PARTITION = "state='FL'"
TEST_METASTORE_CONN_ID = "test_conn_id"


def test_hive_partition_trigger_serialization():
    """
    Asserts that the HivePartitionTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.apache.hive.triggers.hive_partition.HivePartitionTrigger"
    assert kwargs == {
        "table": TEST_TABLE,
        "partition": TEST_PARTITION,
        "schema": TEST_SCHEMA,
        "polling_interval": TEST_POLLING_INTERVAL,
        "metastore_conn_id": TEST_METASTORE_CONN_ID,
    }


@pytest.mark.asyncio
@mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliAsyncHook.partition_exists")
@mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliAsyncHook.get_connection")
async def test_hive_partition_trigger_success(mock_get_connection, mock_partition_exists):
    """Tests that the HivePartitionTrigger is success case when a partition exists in the given table"""
    mock_partition_exists.return_value = "success"

    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "message": "success"}) == actual


@pytest.mark.asyncio
@mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliAsyncHook.partition_exists")
@mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliAsyncHook.get_connection")
async def test_hive_partition_trigger_pending(mock_get_connection, mock_partition_exists):
    """Test that HivePartitionTrigger is in loop if partition isn't found."""
    mock_partition_exists.return_value = "pending"

    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliAsyncHook.partition_exists")
@mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliAsyncHook.get_connection")
async def test_hive_partition_trigger_exception(mock_get_connection, mock_partition_exists):
    """Tests the HivePartitionTrigger does fire if there is an exception."""
    mock_partition_exists.side_effect = AsyncMock(side_effect=Exception("Test exception"))
    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
