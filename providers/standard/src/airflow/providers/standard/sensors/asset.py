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

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseSensorOperator,
    conf,
    timezone,
)
from airflow.providers.standard.version_compat import AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_4_PLUS:
    raise AirflowOptionalProviderFeatureException("Asset partition sensor needs Airflow 3.4+.")

from airflow.providers.standard.exceptions import AssetPartitionTriggerEventError
from airflow.providers.standard.triggers.asset import AssetPartitionTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Asset, Context


class AssetPartitionSensor(BaseSensorOperator):
    """
    Wait for an asset event with the given partition key.

    :param asset: asset to wait for.
    :param partition_key: partition key for the asset event to wait for.
    :param after: only match events whose timestamp is at or after this (timezone-aware)
        datetime. When unset, any historical event with the partition key satisfies the
        wait. Bound it (e.g. ``after="{{ data_interval_start }}"``) when the partition key
        can be reused across events, so a stale event from an earlier run is not matched.
    :param deferrable: If waiting for completion, whether to defer the task until done.
    """

    template_fields: Sequence[str] = ("partition_key", "after")
    ui_color = "#e6f1f2"

    def __init__(
        self,
        *,
        asset: Asset,
        partition_key: str,
        after: datetime.datetime | str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.asset = asset
        self.partition_key = partition_key
        self.after = after
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        from airflow.sdk.exceptions import AirflowRuntimeError
        from airflow.sdk.execution_time.comms import AssetEventsResult, ErrorResponse, GetAssetEventByAsset
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        self.log.info("Poking for asset event: asset=%s, partition_key=%s", self.asset, self.partition_key)
        after = timezone.parse(self.after) if isinstance(self.after, str) else self.after
        response = SUPERVISOR_COMMS.send(
            GetAssetEventByAsset(
                name=self.asset.name,
                uri=self.asset.uri,
                partition_key=self.partition_key,
                after=after,
                ascending=False,
                limit=1,
            )
        )
        if isinstance(response, ErrorResponse):
            raise AirflowRuntimeError(response)
        if TYPE_CHECKING:
            assert isinstance(response, AssetEventsResult)
        return bool(response and response.asset_events)

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
            return

        if not self.poke(context=context):
            self.defer(
                timeout=datetime.timedelta(seconds=self.timeout),
                trigger=AssetPartitionTrigger(
                    asset_name=self.asset.name,
                    asset_uri=self.asset.uri,
                    partition_key=self.partition_key,
                    after=self.after,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event and event.get("status") == "success":
            self.log.info(
                "Asset partition event found: asset=%s, partition_key=%s",
                self.asset,
                self.partition_key,
            )
            return
        message = event.get("message") if event else "Trigger completed without an event"
        raise AssetPartitionTriggerEventError(message)
