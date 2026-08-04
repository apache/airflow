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

from typing import TYPE_CHECKING

from sqlalchemy import and_, func, select
from sqlalchemy.orm import subqueryload

from airflow.models.asset import AssetEvent, AssetModel, AssetWatcherModel

if TYPE_CHECKING:
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
            subqueryload(AssetModel.scheduled_dags),
            subqueryload(AssetModel.producing_tasks),
            subqueryload(AssetModel.consuming_tasks),
            subqueryload(AssetModel.aliases),
            subqueryload(AssetModel.watchers).joinedload(AssetWatcherModel.trigger),
        )
    )
