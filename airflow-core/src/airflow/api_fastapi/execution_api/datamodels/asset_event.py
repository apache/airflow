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

from pydantic import AwareDatetime, Field, field_validator, model_validator
from pydantic.types import JsonValue

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.execution_api.datamodels.asset import AssetResponse


class EmitAssetEventBody(StrictBaseModel):
    """Immediate emission; source identity is supplied only by trusted supervisors."""

    name: str
    uri: str
    extra: dict[str, JsonValue] = Field(default_factory=dict)
    partition_key: str | None = Field(default=None, max_length=250)
    partition_date: AwareDatetime | None = None
    source_dag_id: str | None = None
    source_run_id: str | None = None

    @field_validator("partition_key")
    @classmethod
    def validate_partition_key(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("partition_key must not be empty or whitespace-only")
        return value

    @model_validator(mode="after")
    def validate_partition_date(self):
        if self.partition_date is not None and self.partition_key is None:
            raise ValueError("partition_date requires partition_key")
        return self


class AssetEventReceipt(StrictBaseModel):
    """Acknowledgement of a persisted event, not of downstream scheduling."""

    id: int
    timestamp: datetime


class DagRunAssetReference(StrictBaseModel):
    """DagRun serializer for asset responses."""

    run_id: str
    dag_id: str
    logical_date: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    state: str
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    partition_key: str | None


class AssetEventResponse(BaseModel):
    """Asset event schema with fields that are needed for Runtime."""

    id: int
    timestamp: datetime
    extra: dict[str, JsonValue] | None = None

    asset: AssetResponse
    created_dagruns: list[DagRunAssetReference]

    source_task_id: str | None = None
    source_dag_id: str | None = None
    source_run_id: str | None = None
    source_map_index: int | None = None
    partition_key: str | None = None


class AssetEventsResponse(BaseModel):
    """Collection of AssetEventResponse."""

    asset_events: list[AssetEventResponse]
