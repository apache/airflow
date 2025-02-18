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

from fastapi import Query, status
from sqlalchemy import and_, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api.datamodels.asset import (
    AssetEventCollectionResponse,
)
from airflow.models.asset import AssetAliasModel, AssetEvent, AssetModel

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Asset not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)


def _get_asset_events_through_sql_clauses(
    *, join_clause, where_clause, session: SessionDep
) -> AssetEventCollectionResponse:
    asset_events = session.scalars(
        select(AssetEvent).join(join_clause).where(where_clause).order_by(AssetEvent.timestamp)
    )
    return AssetEventCollectionResponse.model_validate({"asset_events": asset_events or []})


@router.get("/by-asset-name-uri")
def get_asset_event_by_asset_name_uri(
    name: Annotated[str, Query(description="The name of the Asset")],
    uri: Annotated[str, Query(description="The URI of the Asset")],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.asset,
        where_clause=and_(AssetModel.name == name, AssetModel.uri == uri),
        session=session,
    )


@router.get("/by-asset-uri")
def get_asset_event_by_uri(
    uri: Annotated[str, Query(description="The URI of the Asset")],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.asset,
        where_clause=and_(AssetModel.uri == uri, AssetModel.active.has()),
        session=session,
    )


@router.get("/by-asset-name")
def get_asset_event_by_name(
    name: Annotated[str, Query(description="The name of the Asset")],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.asset,
        where_clause=and_(AssetModel.uri == name, AssetModel.active.has()),
        session=session,
    )


@router.get("/by-alias-name")
def get_asset_event_by_alias_name(
    name: Annotated[str, Query(description="The name of the Asset Alias")],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.source_aliases,
        where_clause=(AssetAliasModel.name == name),
        session=session,
    )
