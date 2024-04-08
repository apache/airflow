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

from airflow.providers.microsoft.azure.hooks.wasb import WasbAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class WasbBlobSensorTrigger(BaseTrigger):
    """
    Check for existence of the given blob in the provided container.

    WasbBlobSensorTrigger is fired as deferred class with params to run the task in trigger worker.

    :param container_name: name of the container in which the blob should be searched for
    :param blob_name: name of the blob to check existence for
    :param wasb_conn_id: the connection identifier for connecting to Azure WASB
    :param poke_interval:  polling period in seconds to check for the status
    :param public_read: whether an anonymous public read access should be used. Default is False
    """

    def __init__(
        self,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
        poke_interval: float = 5.0,
    ):
        super().__init__()
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.poke_interval = poke_interval
        self.public_read = public_read

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize WasbBlobSensorTrigger arguments and classpath."""
        return (
            "airflow.providers.microsoft.azure.triggers.wasb.WasbBlobSensorTrigger",
            {
                "container_name": self.container_name,
                "blob_name": self.blob_name,
                "wasb_conn_id": self.wasb_conn_id,
                "poke_interval": self.poke_interval,
                "public_read": self.public_read,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Azure WASB and polls for existence of the given blob name."""
        blob_exists = False
        hook = WasbAsyncHook(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)
        try:
            async with await hook.get_async_conn():
                while not blob_exists:
                    blob_exists = await hook.check_for_blob_async(
                        container_name=self.container_name,
                        blob_name=self.blob_name,
                    )
                    if blob_exists:
                        message = f"Blob {self.blob_name} found in container {self.container_name}."
                        yield TriggerEvent({"status": "success", "message": message})
                        return
                    else:
                        message = (
                            f"Blob {self.blob_name} not available yet in container {self.container_name}."
                            f" Sleeping for {self.poke_interval} seconds"
                        )
                        self.log.info(message)
                        await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class WasbPrefixSensorTrigger(BaseTrigger):
    """
    Check for the existence of a blob with the given prefix in the provided container.

    WasbPrefixSensorTrigger is fired as a deferred class with params to run the task in trigger.

    :param container_name: name of the container in which the blob should be searched for
    :param prefix: prefix of the blob to check existence for
    :param include: specifies one or more additional datasets to include in the
            response. Options include: ``snapshots``, ``metadata``, ``uncommittedblobs``,
            ``copy``, ``deleted``
    :param delimiter: filters objects based on the delimiter (for e.g '.csv')
    :param wasb_conn_id: the connection identifier for connecting to Azure WASB
    :param check_options: Optional keyword arguments that
        `WasbAsyncHook.check_for_prefix_async()` takes.
    :param public_read: whether an anonymous public read access should be used. Default is False
    :param poke_interval:  polling period in seconds to check for the status
    """

    def __init__(
        self,
        container_name: str,
        prefix: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        public_read: bool = False,
        poke_interval: float = 5.0,
    ):
        if not check_options:
            check_options = {}
        super().__init__()
        self.container_name = container_name
        self.prefix = prefix
        self.wasb_conn_id = wasb_conn_id
        self.check_options = check_options
        self.poke_interval = poke_interval
        self.public_read = public_read

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize WasbPrefixSensorTrigger arguments and classpath."""
        return (
            "airflow.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger",
            {
                "container_name": self.container_name,
                "prefix": self.prefix,
                "wasb_conn_id": self.wasb_conn_id,
                "poke_interval": self.poke_interval,
                "check_options": self.check_options,
                "public_read": self.public_read,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Azure WASB and polls for existence of a blob with given prefix."""
        prefix_exists = False
        hook = WasbAsyncHook(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)
        try:
            async with await hook.get_async_conn():
                while not prefix_exists:
                    prefix_exists = await hook.check_for_prefix_async(
                        container_name=self.container_name, prefix=self.prefix, **self.check_options
                    )
                    if prefix_exists:
                        message = f"Prefix {self.prefix} found in container {self.container_name}."
                        yield TriggerEvent({"status": "success", "message": message})
                        return
                    else:
                        message = (
                            f"Prefix {self.prefix} not available yet in container {self.container_name}."
                            f" Sleeping for {self.poke_interval} seconds"
                        )
                        self.log.info(message)
                        await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
