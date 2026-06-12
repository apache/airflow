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

from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from airflow.triggers.base import BaseTrigger, TriggerEvent

_SPARK_TERMINAL_STATES = {"FINISHED", "FAILED", "KILLED", "ERROR"}


class SparkDriverTrigger(BaseTrigger):
    """
    Async trigger that polls the Spark standalone REST API until the driver
    reaches a terminal state. Used when SparkSubmitOperator runs with deferrable=True.

    :param driver_id: Spark driver submission ID returned by spark-submit --rest.
    :param master_urls: List of Spark master REST base URLs e.g. ["http://spark-master:6066"].
    :param poll_interval: Seconds between REST API polls. Defaults to 10.
    """

    def __init__(
        self,
        driver_id: str,
        master_urls: list[str],
        poll_interval: int = 10,
    ) -> None:
        super().__init__()
        self.driver_id = driver_id
        self.master_urls = master_urls
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.apache.spark.triggers.spark_submit.SparkDriverTrigger",
            {
                "driver_id": self.driver_id,
                "master_urls": self.master_urls,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll Spark REST API until driver reaches a terminal state."""
        while True:
            status = await self._poll_driver_status()
            if status is None:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "driver_id": self.driver_id,
                        "message": "All Spark masters unreachable",
                    }
                )
                return
            self.log.info("SparkDriverTrigger: driver=%s status=%s", self.driver_id, status)
            normalized_status = status.upper()
            if normalized_status in _SPARK_TERMINAL_STATES:
                success = normalized_status == "FINISHED"
                yield TriggerEvent(
                    {
                        "status": "success" if success else "error",
                        "driver_id": self.driver_id,
                        "driver_state": normalized_status,
                        "message": f"Driver {self.driver_id} reached state {normalized_status}",
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)

    async def _poll_driver_status(self) -> str | None:
        """Try each master URL; return driverState str or None if all fail."""
        for url in self.master_urls:
            status_url = f"{url.rstrip('/')}/v1/submissions/status/{self.driver_id}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(status_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        if not data.get("success"):
                            self.log.warning(
                                "Spark REST API returned success=false for %s: %s",
                                self.driver_id,
                                data.get("message", "unknown"),
                            )
                            continue
                        return data["driverState"]
            except aiohttp.ClientError as exc:
                self.log.warning("Could not reach Spark master at %s: %s", url, exc)
                continue
        return None
