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

from sqlalchemy import func, select
from sqlalchemy.orm import subqueryload

from airflow.models.asset import AssetEvent, AssetModel, AssetWatcherModel, association_table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select


def generate_assets_with_last_event_query() -> Select:
    """Fetch Assets outer-joined to their latest AssetEvent id/timestamp."""
    max_asset_event_id_query = (
        select(AssetEvent.asset_id, func.max(AssetEvent.id).label("max_asset_event_id"))
        .group_by(AssetEvent.asset_id)
        .subquery()
    )

    return (
        select(
            AssetModel,
            AssetEvent.id.label("last_asset_event_id"),
            AssetEvent.timestamp.label("last_asset_event_timestamp"),
        )
        .outerjoin(max_asset_event_id_query, AssetModel.id == max_asset_event_id_query.c.asset_id)
        .outerjoin(AssetEvent, AssetEvent.id == max_asset_event_id_query.c.max_asset_event_id)
        .options(
            subqueryload(AssetModel.scheduled_dags),
            subqueryload(AssetModel.producing_tasks),
            subqueryload(AssetModel.consuming_tasks),
            subqueryload(AssetModel.aliases),
            subqueryload(AssetModel.watchers).joinedload(AssetWatcherModel.trigger),
        )
    )


def resolve_triggering_event_ids(events: Iterable[AssetEvent], *, session: Session) -> dict[int, int]:
    """
    Map each dag run id to the id of the asset event that actually triggered it.

    A dag run consumes every asset event queued when it is created, but only the run's most
    recent consumed event triggers it; earlier consumed events are merely included. Rank each
    run's consumed events by timestamp (newest wins) so callers can tell the two apart.
    """
    run_ids = {run.id for event in events for run in event.created_dagruns}
    if not run_ids:
        return {}
    ranked_events = (
        select(
            association_table.c.dag_run_id,
            association_table.c.event_id,
            func.row_number()
            .over(
                partition_by=association_table.c.dag_run_id,
                order_by=(AssetEvent.timestamp.desc(), AssetEvent.id.desc()),
            )
            .label("rank"),
        )
        .join(AssetEvent, AssetEvent.id == association_table.c.event_id)
        .where(association_table.c.dag_run_id.in_(run_ids))
        .subquery()
    )
    return {
        dag_run_id: event_id
        for dag_run_id, event_id in session.execute(
            select(ranked_events.c.dag_run_id, ranked_events.c.event_id).where(ranked_events.c.rank == 1)
        )
    }
