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

from datetime import datetime

from pydantic import Field, field_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.sdk.execution_time.secrets_masker import redact


class AssetResponse(BaseModel):
    """Asset schema for responses with fields that are needed for Runtime."""

    name: str
    uri: str
    group: str
    extra: dict | None = None


class AssetAliasResponse(BaseModel):
    """Asset alias schema with fields that are needed for Runtime."""

    name: str
    group: str


class DagRunAssetReference(StrictBaseModel):
    """DAGRun serializer for asset responses."""

    run_id: str
    dag_id: str
    logical_date: datetime | None
    start_date: datetime
    end_date: datetime | None
    state: str
    data_interval_start: datetime | None
    data_interval_end: datetime | None


class AssetEventResponse(BaseModel):
    """Asset event schema with fields that are needed for Runtime."""

    id: int
    asset_id: int
    uri: str | None = Field(alias="uri", default=None)
    name: str | None = Field(alias="name", default=None)
    group: str | None = Field(alias="group", default=None)
    extra: dict | None = None
    source_task_id: str | None = None
    source_dag_id: str | None = None
    source_run_id: str | None = None
    source_map_index: int
    created_dagruns: list[DagRunAssetReference]
    timestamp: datetime

    @field_validator("extra", mode="after")
    @classmethod
    def redact_extra(cls, v: dict):
        return redact(v)


class AssetProfile(BaseModel):
    """
    Profile of an Asset.

    Asset will have name, uri and asset_type defined.
    AssetNameRef will have name and asset_type defined.
    AssetUriRef will have uri and asset_type defined.

    """

    name: str | None = None
    uri: str | None = None
    asset_type: str
