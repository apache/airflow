#
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
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, timezone
from airflow.providers.standard.version_compat import AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_4_PLUS:
    raise AirflowOptionalProviderFeatureException("Asset partition sensor needs Airflow 3.4+.")

from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    import datetime


class AssetPartitionTrigger(BaseTrigger):
    """
    Trigger when an asset event exists for the given partition key.

    :param asset_name: name of the asset to wait for.
    :param asset_uri: URI of the asset to wait for.
    :param partition_key: partition key for the asset event to wait for.
    :param after: only match events whose timestamp is at or after this (timezone-aware)
        datetime. Leave unset to match any event with the partition key.
    :param poke_interval: polling interval in seconds.
    """

    def __init__(
        self,
        *,
        asset_name: str | None,
        asset_uri: str | None,
        partition_key: str,
        after: datetime.datetime | str | None = None,
        poke_interval: float = 5.0,
    ) -> None:
        super().__init__()
        self.asset_name = asset_name
        self.asset_uri = asset_uri
        self.partition_key = partition_key
        self.after = after
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize AssetPartitionTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.asset.AssetPartitionTrigger",
            {
                "asset_name": self.asset_name,
                "asset_uri": self.asset_uri,
                "partition_key": self.partition_key,
                "after": self.after,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll until the requested asset partition event exists."""
        from airflow.sdk.execution_time.comms import AssetEventsResult, ErrorResponse, GetAssetEventByAsset
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        after = timezone.parse(self.after) if isinstance(self.after, str) else self.after
        while True:
            response = await SUPERVISOR_COMMS.asend(
                GetAssetEventByAsset(
                    name=self.asset_name,
                    uri=self.asset_uri,
                    partition_key=self.partition_key,
                    after=after,
                    ascending=False,
                    limit=1,
                )
            )
            if isinstance(response, ErrorResponse):
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"{response.error.value}: {response.detail}",
                    }
                )
                return
            if TYPE_CHECKING:
                assert isinstance(response, AssetEventsResult)
            if response and response.asset_events:
                yield TriggerEvent({"status": "success"})
                return
            await asyncio.sleep(self.poke_interval)
