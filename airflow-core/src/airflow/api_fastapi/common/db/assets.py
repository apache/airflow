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

from collections.abc import Iterable
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, select
from sqlalchemy.orm import selectinload

from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.models.asset import (
    AssetEvent,
    AssetModel,
    AssetWatcherModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
    association_table,
)
from airflow.models.dagrun import DagRun

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.orm.strategy_options import _AbstractLoad
    from sqlalchemy.sql import Select


def generate_assets_with_last_event_query() -> Select:
    """Fetch Assets outer-joined to their latest AssetEvent id/timestamp."""
    latest_asset_event_timestamps = (
        select(AssetEvent.asset_id, func.max(AssetEvent.timestamp).label("last_timestamp"))
        .group_by(AssetEvent.asset_id)
        .subquery()
    )
    latest_asset_event_ids = (
        select(AssetEvent.asset_id, func.max(AssetEvent.id).label("last_asset_event_id"))
        .join(
            latest_asset_event_timestamps,
            and_(
                AssetEvent.asset_id == latest_asset_event_timestamps.c.asset_id,
                AssetEvent.timestamp == latest_asset_event_timestamps.c.last_timestamp,
            ),
        )
        .group_by(AssetEvent.asset_id)
        .subquery()
    )

    return (
        select(
            AssetModel,
            AssetEvent.id.label("last_asset_event_id"),
            AssetEvent.timestamp.label("last_asset_event_timestamp"),
        )
        .outerjoin(latest_asset_event_ids, AssetModel.id == latest_asset_event_ids.c.asset_id)
        .outerjoin(AssetEvent, AssetEvent.id == latest_asset_event_ids.c.last_asset_event_id)
        .options(
            *eager_load_asset_reference_teams(),
            selectinload(AssetModel.consuming_tasks),
            selectinload(AssetModel.aliases),
            selectinload(AssetModel.watchers).joinedload(AssetWatcherModel.trigger),
        )
    )


def resolve_triggering_event_refs(
    events: Iterable[AssetEvent], *, session: Session
) -> set[tuple[int, str, str]]:
    """
    Return ``(event_id, dag_id, run_id)`` tuples where the asset event actually triggered the run.

    A dag run consumes every asset event queued when it is created, but only the run's most recent
    consumed event triggers it; earlier consumed events are merely included.
    """
    run_ids = {run.id for event in events for run in event.created_dagruns}
    if not run_ids:
        return set()
    ranked_events = (
        select(
            association_table.c.event_id,
            DagRun.dag_id,
            DagRun.run_id,
            func.row_number()
            .over(
                partition_by=association_table.c.dag_run_id,
                order_by=(AssetEvent.timestamp.desc(), AssetEvent.id.desc()),
            )
            .label("rank"),
        )
        .join(AssetEvent, AssetEvent.id == association_table.c.event_id)
        .join(DagRun, DagRun.id == association_table.c.dag_run_id)
        .where(association_table.c.dag_run_id.in_(run_ids))
        .subquery()
    )
    return {
        (event_id, dag_id, run_id)
        for event_id, dag_id, run_id in session.execute(
            select(ranked_events.c.event_id, ranked_events.c.dag_id, ranked_events.c.run_id).where(
                ranked_events.c.rank == 1
            )
        )
    }


def eager_load_asset_reference_teams() -> tuple[_AbstractLoad, ...]:
    """
    Loader options for an ``AssetModel`` query whose response exposes reference ``team_name``.

    Covers the two references that serialize a team: scheduled Dags and producing tasks.
    The Dag each one points at is only traversed in multi-team mode, so single-team
    deployments load the references alone -- see :func:`eager_load_teams`.
    """
    return (
        selectinload(AssetModel.scheduled_dags).options(*eager_load_teams(DagScheduleAssetReference.dag)),
        selectinload(AssetModel.producing_tasks).options(*eager_load_teams(TaskOutletAssetReference.dag)),
    )
