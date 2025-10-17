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

from pydantic import AliasPath, ConfigDict, Field, NonNegativeInt, field_validator

from airflow._shared.secrets_masker import redact
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class DagScheduleAssetReference(StrictBaseModel):
    """DAG schedule reference serializer for assets."""

    dag_id: str
    created_at: datetime
    updated_at: datetime


class TaskInletAssetReference(StrictBaseModel):
    """Task inlet reference serializer for assets."""

    dag_id: str
    task_id: str
    created_at: datetime
    updated_at: datetime


class TaskOutletAssetReference(StrictBaseModel):
    """Task outlet reference serializer for assets."""

    dag_id: str
    task_id: str
    created_at: datetime
    updated_at: datetime


class LastAssetEventResponse(BaseModel):
    """Last asset event response serializer."""

    id: NonNegativeInt | None = None
    timestamp: datetime | None = None


class AssetResponse(BaseModel):
    """Asset serializer for responses."""

    id: int
    name: str
    uri: str
    group: str
    extra: dict | None = None
    created_at: datetime
    updated_at: datetime
    scheduled_dags: list[DagScheduleAssetReference]
    producing_tasks: list[TaskOutletAssetReference]
    consuming_tasks: list[TaskInletAssetReference]
    aliases: list[AssetAliasResponse]
    last_asset_event: LastAssetEventResponse | None = None

    @field_validator("extra", mode="after")
    @classmethod
    def redact_extra(cls, v: dict):
        return redact(v)


class AssetCollectionResponse(BaseModel):
    """Asset collection response."""

    assets: list[AssetResponse]
    total_entries: int


class AssetAliasResponse(BaseModel):
    """Asset alias serializer for responses."""

    id: int
    name: str
    group: str


class AssetAliasCollectionResponse(BaseModel):
    """Asset alias collection response."""

    asset_aliases: list[AssetAliasResponse]
    total_entries: int


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
    """Asset event serializer for responses."""

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


class AssetEventCollectionResponse(BaseModel):
    """Asset event collection response."""

    asset_events: list[AssetEventResponse]
    total_entries: int


class QueuedEventResponse(BaseModel):
    """Queued Event serializer for responses.."""

    dag_id: str
    asset_id: int
    created_at: datetime
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))


class QueuedEventCollectionResponse(BaseModel):
    """Queued Event Collection serializer for responses."""

    queued_events: list[QueuedEventResponse]
    total_entries: int


class CreateAssetEventsBody(StrictBaseModel):
    """Create asset events request."""

    asset_id: int
    extra: dict = Field(default_factory=dict)

    @field_validator("extra", mode="after")
    def set_from_rest_api(cls, v: dict) -> dict:
        v["from_rest_api"] = True
        return v

    model_config = ConfigDict(extra="forbid")
