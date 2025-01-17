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

from fastapi import HTTPException, Query, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api.datamodels.asset import AssetResponse
from airflow.models.asset import AssetModel

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Asset not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)


@router.get("/by-name")
def get_asset_by_name(
    name: Annotated[str, Query(description="The name of the Asset")],
    session: SessionDep,
) -> AssetResponse:
    """Get an Airflow Asset by `name`."""
    asset = session.scalar(select(AssetModel).where(AssetModel.name == name, AssetModel.active.has()))
    _raise_if_not_found(asset, f"Asset with name {name} not found")

    return AssetResponse.model_validate(asset)


@router.get("/by-uri")
def get_asset_by_uri(
    uri: Annotated[str, Query(description="The URI of the Asset")],
    session: SessionDep,
) -> AssetResponse:
    """Get an Airflow Asset by `uri`."""
    asset = session.scalar(select(AssetModel).where(AssetModel.uri == uri, AssetModel.active.has()))
    _raise_if_not_found(asset, f"Asset with URI {uri} not found")

    return AssetResponse.model_validate(asset)


def _raise_if_not_found(asset, msg):
    if asset is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": msg,
            },
        )
