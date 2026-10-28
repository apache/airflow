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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, Query
from sqlalchemy import exists, select as sql_select

from airflow.api_fastapi.common.db.triggers import (
    TriggerKind,
    TriggerStatus,
    build_trigger_dag_id_expression,
    build_trigger_kind_expression,
    build_trigger_status_expression,
)
from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.api_fastapi.common.parameters.filter import FilterOptionEnum, FilterParam, filter_param_factory
from airflow.jobs.job import Job
from airflow.models.asset import AssetWatcherModel
from airflow.models.trigger import Trigger

if TYPE_CHECKING:
    from sqlalchemy.sql import Select


class _TriggerStatusFilter(BaseParam[list[TriggerStatus]]):
    """Filter triggers on their derived status; built per request because "alive" depends on now."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select
        return select.where(build_trigger_status_expression().in_([status.value for status in self.value]))

    @classmethod
    def depends(
        cls,
        status: list[TriggerStatus] = Query(
            default_factory=list, description="Return only triggers with one of these statuses."
        ),
    ) -> _TriggerStatusFilter:
        return cls().set_value(status)


class _TriggerAssetIdFilter(BaseParam[int]):
    """Filter triggers on an asset one of their watchers watches."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.where(
            exists(
                sql_select(AssetWatcherModel.trigger_id).where(
                    AssetWatcherModel.trigger_id == Trigger.id, AssetWatcherModel.asset_id == self.value
                )
            )
        )

    @classmethod
    def depends(
        cls,
        asset_id: int | None = Query(
            default=None, description="Return only the triggers of this asset's watchers."
        ),
    ) -> _TriggerAssetIdFilter:
        return cls().set_value(asset_id)


QueryTriggerStatusFilter = Annotated[_TriggerStatusFilter, Depends(_TriggerStatusFilter.depends)]
QueryTriggerAssetIdFilter = Annotated[_TriggerAssetIdFilter, Depends(_TriggerAssetIdFilter.depends)]
QueryTriggerKindFilter = Annotated[
    FilterParam[list[TriggerKind]],
    Depends(
        filter_param_factory(
            build_trigger_kind_expression(),
            list[TriggerKind],
            FilterOptionEnum.IN,
            filter_name="kind",
            default_factory=list,
            description="Return only triggers of these kinds.",
        )
    ),
]
QueryTriggerDagIdFilter = Annotated[
    FilterParam[str | None],
    Depends(
        filter_param_factory(
            build_trigger_dag_id_expression(),
            str | None,
            filter_name="dag_id",
            description="Return only the triggers of this Dag's task instances and callbacks.",
        )
    ),
]
QueryTriggererHostnameFilter = Annotated[
    FilterParam[str | None],
    Depends(
        filter_param_factory(
            Job.hostname,
            str | None,
            filter_name="triggerer_hostname",
            description="Return only the triggers assigned to the triggerer on this host.",
        )
    ),
]
