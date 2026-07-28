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

from airflow.models.asset import AssetEvent

if TYPE_CHECKING:
    from sqlalchemy.sql import Subquery


def generate_last_asset_event_query() -> Subquery:
    """Build a subquery yielding the ID and timestamp of the latest AssetEvent per asset."""
    last_asset_event_per_asset = (
        select(AssetEvent.asset_id, func.max(AssetEvent.timestamp).label("last_timestamp"))
        .group_by(AssetEvent.asset_id)
        .subquery()
    )

    return (
        select(
            AssetEvent.asset_id,  
            func.max(AssetEvent.id).label("last_asset_event_id"),  
            func.max(AssetEvent.timestamp).label("last_asset_event_timestamp"),
        )
        .join(
            last_asset_event_per_asset,
            and_(
                AssetEvent.asset_id == last_asset_event_per_asset.c.asset_id,
                AssetEvent.timestamp == last_asset_event_per_asset.c.last_timestamp,
            ),
        )
        .group_by(AssetEvent.asset_id)
        .subquery()
    )
