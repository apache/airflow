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
from typing import Any, AsyncIterator

from airflow.providers.apache.hive.hooks.hive import HiveCliAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class HivePartitionTrigger(BaseTrigger):
    """
    A trigger that fires and it looks for a partition in the given table
    in the database or wait for the partition.

    :param table: the table where the partition is present.
    :param partition: The partition clause to wait for.
    :param schema: database which needs to be connected in hive.
    :param metastore_conn_id: connection string to connect to hive.
    :param polling_period_seconds: polling period in seconds to check for the partition.
    """

    def __init__(
        self,
        table: str,
        partition: str,
        polling_interval: float,
        metastore_conn_id: str,
        schema: str,
    ):
        super().__init__()
        self.table = table
        self.partition = partition
        self.polling_interval = polling_interval
        self.metastore_conn_id: str = metastore_conn_id
        self.schema = schema

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes HivePartitionTrigger arguments and classpath."""
        return (
            "airflow.providers.apache.hive.triggers.hive_partition.HivePartitionTrigger",
            {
                "table": self.table,
                "partition": self.partition,
                "polling_interval": self.polling_interval,
                "metastore_conn_id": self.metastore_conn_id,
                "schema": self.schema,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """Simple loop until the relevant table partition is present in the table or wait for it."""
        try:
            hook = self._get_async_hook()
            while True:
                res = await hook.partition_exists(
                    table=self.table,
                    schema=self.schema,
                    partition=self.partition,
                    polling_interval=self.polling_interval,
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                await asyncio.sleep(self.polling_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> HiveCliAsyncHook:
        return HiveCliAsyncHook(metastore_conn_id=self.metastore_conn_id)
