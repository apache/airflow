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

import functools
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, Query
from sqlalchemy import select as sql_select, union_all

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.api_fastapi.common.parameters.filter import FilterOptionEnum, FilterParam, filter_param_factory
from airflow.api_fastapi.core_api.security import AuthManagerDep, GetUserDep, ReadableTriggersFilterDep
from airflow.jobs.job import Job
from airflow.models.asset import (
    AssetModel,
    AssetWatcherModel,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
)
from airflow.models.trigger import Trigger, TriggerKind, TriggerStatus

if TYPE_CHECKING:
    from collections.abc import Collection

    from sqlalchemy.sql import CompoundSelect, Select


def select_scheduled_dag_ids(*, asset_ids: Collection[int]) -> CompoundSelect:
    """
    Select ``(asset_id, dag_id)`` for each of the Dags scheduled on each of the assets.

    As an event of the asset queues them: scheduled on the asset itself, or on ``Asset.ref(name=...)`` /
    ``Asset.ref(uri=...)`` of it.
    """
    by_ref = [
        sql_select(AssetModel.id, ref.dag_id)
        .join(AssetModel, getattr(AssetModel, column) == getattr(ref, column))
        .where(AssetModel.id.in_(asset_ids))
        for ref, column in ((DagScheduleAssetNameReference, "name"), (DagScheduleAssetUriReference, "uri"))
    ]
    direct = sql_select(DagScheduleAssetReference.asset_id, DagScheduleAssetReference.dag_id).where(
        DagScheduleAssetReference.asset_id.in_(asset_ids)
    )
    return union_all(direct, *by_ref)


def select_scheduled_asset_ids(*, dag_id: str) -> CompoundSelect:
    """Select the assets a Dag is scheduled on: directly, or on ``Asset.ref(name=...)`` / ``Asset.ref(uri=...)``."""
    by_ref = [
        sql_select(AssetModel.id)
        .join(ref, getattr(AssetModel, column) == getattr(ref, column))
        .where(ref.dag_id == dag_id)
        for ref, column in ((DagScheduleAssetNameReference, "name"), (DagScheduleAssetUriReference, "uri"))
    ]
    direct = sql_select(DagScheduleAssetReference.asset_id).where(DagScheduleAssetReference.dag_id == dag_id)
    return union_all(direct, *by_ref)


def get_readable_dag_ids_once(
    readable: ReadableTriggersFilterDep, user: GetUserDep, auth_manager: AuthManagerDep
) -> Callable[[], set[str]]:
    """
    Look up the Dags the caller can read when first asked, and only once: some auth managers check every Dag.

    A caller who reads task instances or Dag runs has them in the request's trigger filter already.
    """
    known = readable.task_dag_ids | readable.callback_dag_ids
    return functools.cache(lambda: known or auth_manager.get_authorized_dag_ids(user=user, method="GET"))


ReadableDagIdsDep = Annotated[Callable[[], set[str]], Depends(get_readable_dag_ids_once)]


class _TriggerStatusFilter(BaseParam[list[TriggerStatus]]):
    """Filter triggers on their derived status; built per request because "alive" depends on now."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select
        return select.where(Trigger.status_expression().in_([status.value for status in self.value]))

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
            Trigger.id.in_(
                sql_select(AssetWatcherModel.trigger_id).where(AssetWatcherModel.asset_id == self.value)
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


class _TriggerScheduledDagIdFilter(BaseParam[str]):
    """Filter asset watchers' triggers on a Dag scheduled on a watched asset, as the watchers' scheduled_dag_ids."""

    def __init__(self, asset_ids: set[int] | None = None) -> None:
        super().__init__()
        self.asset_ids = asset_ids or set()

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.where(
            Trigger.id.in_(
                sql_select(AssetWatcherModel.trigger_id).where(AssetWatcherModel.asset_id.in_(self.asset_ids))
            )
        )

    @classmethod
    def depends(
        cls,
        readable: ReadableTriggersFilterDep,
        readable_dag_ids: ReadableDagIdsDep,
        session: SessionDep,
        scheduled_dag_id: str | None = Query(
            default=None,
            description="Return only the triggers of watchers on assets this Dag is scheduled on, directly or "
            "by an asset name or URI reference.",
        ),
    ) -> _TriggerScheduledDagIdFilter:
        asset_ids: set[int] = set()
        # Only a Dag and assets the caller can read, as scheduled_dag_ids names them: none for any other Dag.
        if scheduled_dag_id is not None and scheduled_dag_id in readable_dag_ids():
            scheduled = set(session.scalars(select_scheduled_asset_ids(dag_id=scheduled_dag_id)))
            asset_ids = scheduled & readable.asset_ids
        return cls(asset_ids).set_value(scheduled_dag_id)


class _TriggerTeamsFilter(BaseParam[list[str]]):
    """Filter triggers by the team they run for."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        return select.where(Trigger.team_name.in_(self.value))

    @classmethod
    def depends(
        cls,
        teams: list[str] = Query(
            default_factory=list, description="Return only the triggers of these teams."
        ),
    ) -> _TriggerTeamsFilter:
        return cls().set_value(teams)


QueryTriggerStatusFilter = Annotated[_TriggerStatusFilter, Depends(_TriggerStatusFilter.depends)]
QueryTriggerAssetIdFilter = Annotated[_TriggerAssetIdFilter, Depends(_TriggerAssetIdFilter.depends)]
QueryTriggerScheduledDagIdFilter = Annotated[
    _TriggerScheduledDagIdFilter, Depends(_TriggerScheduledDagIdFilter.depends)
]
QueryTriggerTeamsFilter = Annotated[_TriggerTeamsFilter, Depends(_TriggerTeamsFilter.depends)]
QueryTriggerKindFilter = Annotated[
    FilterParam[list[TriggerKind]],
    Depends(
        filter_param_factory(
            Trigger.kind_expression(),
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
            Trigger.dag_id,
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
