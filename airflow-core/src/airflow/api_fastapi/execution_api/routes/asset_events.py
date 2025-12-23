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

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import and_, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.execution_api.datamodels.asset import AssetResponse
from airflow.api_fastapi.execution_api.datamodels.asset_event import (
    AssetEventResponse,
    AssetEventsResponse,
)
from airflow.models.asset import AssetAliasModel, AssetEvent, AssetModel

# TODO: Add dependency on JWT token
router = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Asset not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)


def _get_asset_events_through_sql_clauses(
    *, join_clause, where_clause, session: SessionDep, ascending: bool = True, limit: int | None = None
) -> AssetEventsResponse:
    order_by_clause = AssetEvent.timestamp.asc() if ascending else AssetEvent.timestamp.desc()
    asset_events_query = select(AssetEvent).join(join_clause).where(where_clause).order_by(order_by_clause)
    if limit:
        asset_events_query = asset_events_query.limit(limit)
    asset_events = session.scalars(asset_events_query)
    return AssetEventsResponse.model_validate(
        {
            "asset_events": [
                AssetEventResponse(
                    id=event.id,
                    timestamp=event.timestamp,
                    extra=event.extra,
                    asset=AssetResponse(
                        name=event.asset.name,
                        uri=event.asset.uri,
                        group=event.asset.group,
                        extra=event.asset.extra,
                    ),
                    created_dagruns=event.created_dagruns,
                    source_task_id=event.source_task_id,
                    source_dag_id=event.source_dag_id,
                    source_run_id=event.source_run_id,
                    source_map_index=event.source_map_index,
                    partition_key=event.partition_key,
                )
                for event in asset_events
            ]
        }
    )


@router.get("/by-asset")
def get_asset_event_by_asset_name_uri(
    name: Annotated[str | None, Query(description="The name of the Asset")],
    uri: Annotated[str | None, Query(description="The URI of the Asset")],
    session: SessionDep,
    after: Annotated[UtcDateTime | None, Query(description="The start of the time range")] = None,
    before: Annotated[UtcDateTime | None, Query(description="The end of the time range")] = None,
    ascending: Annotated[bool, Query(description="Whether to sort results in ascending order")] = True,
    limit: Annotated[int | None, Query(description="The maximum number of results to return")] = None,
) -> AssetEventsResponse:
    if name and uri:
        where_clause = and_(AssetModel.name == name, AssetModel.uri == uri)
    elif uri:
        where_clause = and_(AssetModel.uri == uri, AssetModel.active.has())
    elif name:
        where_clause = and_(AssetModel.name == name, AssetModel.active.has())
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "Missing parameter",
                "message": "name and uri cannot both be None",
            },
        )

    if after:
        where_clause = and_(where_clause, AssetEvent.timestamp >= after)
    if before:
        where_clause = and_(where_clause, AssetEvent.timestamp <= before)

    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.asset,
        where_clause=where_clause,
        session=session,
        ascending=ascending,
        limit=limit,
    )


@router.get("/by-asset-alias")
def get_asset_event_by_asset_alias(
    name: Annotated[str, Query(description="The name of the Asset Alias")],
    session: SessionDep,
    after: Annotated[UtcDateTime | None, Query(description="The start of the time range")] = None,
    before: Annotated[UtcDateTime | None, Query(description="The end of the time range")] = None,
    ascending: Annotated[bool, Query(description="Whether to sort results in ascending order")] = True,
    limit: Annotated[int | None, Query(description="The maximum number of results to return")] = None,
) -> AssetEventsResponse:
    where_clause = AssetAliasModel.name == name
    if after:
        where_clause = and_(where_clause, AssetEvent.timestamp >= after)
    if before:
        where_clause = and_(where_clause, AssetEvent.timestamp <= before)

    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.source_aliases,
        where_clause=where_clause,
        session=session,
        ascending=ascending,
        limit=limit,
    )
