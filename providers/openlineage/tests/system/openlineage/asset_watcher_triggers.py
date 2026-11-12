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
"""
Asset watcher triggers of the system test Dags, in a module of their own so the triggerer can import them.

Separate from ``triggers.py`` because ``AssetTriggerEvent`` only exists on the Airflow versions that let watchers
report a partition, and ``triggers.py`` is imported by Dags that also run on older versions.
"""

from __future__ import annotations

import asyncio
from typing import Any

import anyio

from airflow.triggers.base import AssetTriggerEvent, BaseEventTrigger


class PartitionedFileDeleteTrigger(BaseEventTrigger):
    """Fire once a file exists, deleting it, with an asset event for one partition of the watched asset."""

    def __init__(self, filepath: str, partition_key: str, poke_interval: float = 1.0):
        super().__init__()
        self.filepath = filepath
        self.partition_key = partition_key
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "filepath": self.filepath,
                "partition_key": self.partition_key,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self):
        while True:
            filepath = anyio.Path(self.filepath)
            if await filepath.is_file():
                await filepath.unlink()
                yield AssetTriggerEvent(True, partition_key=self.partition_key)
                return
            await asyncio.sleep(self.poke_interval)
