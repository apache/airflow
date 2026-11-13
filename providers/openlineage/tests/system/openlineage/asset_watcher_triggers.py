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

``AssetTriggerEvent`` only exists on the Airflow versions that let watchers report a partition, so it is imported
where it is used and the module also works on the older versions with asset watchers.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import anyio

from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class FileDeleteWithPayloadTrigger(BaseEventTrigger):
    """
    Fire once a file exists, deleting it, with the file name as the payload.

    The payload ends up in the extra of the asset event, so the asset watcher events of two watchers of one asset
    can be told apart on every Airflow version, including those that don't pass the watcher to listeners.
    """

    def __init__(self, filepath: str, poke_interval: float = 1.0):
        super().__init__()
        self.filepath = filepath
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {"filepath": self.filepath, "poke_interval": self.poke_interval},
        )

    async def _wait_and_delete(self) -> None:
        filepath = anyio.Path(self.filepath)
        while True:
            if await filepath.is_file():
                await filepath.unlink()
                return
            await asyncio.sleep(self.poke_interval)

    async def run(self):
        await self._wait_and_delete()
        yield TriggerEvent({"file": os.path.basename(self.filepath)})


class PartitionedFileDeleteTrigger(FileDeleteWithPayloadTrigger):
    """Fire once a file exists, deleting it, with an asset event for one partition of the watched asset."""

    def __init__(self, filepath: str, partition_key: str, poke_interval: float = 1.0):
        super().__init__(filepath=filepath, poke_interval=poke_interval)
        self.partition_key = partition_key

    def serialize(self) -> tuple[str, dict[str, Any]]:
        classpath, kwargs = super().serialize()
        return classpath, {**kwargs, "partition_key": self.partition_key}

    async def run(self):
        from airflow.triggers.base import AssetTriggerEvent

        await self._wait_and_delete()
        yield AssetTriggerEvent({"file": os.path.basename(self.filepath)}, partition_key=self.partition_key)
