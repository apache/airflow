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
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import AliasPath, Field

from airflow.api_fastapi.core_api.base import BaseModel


class DagVersionResponse(BaseModel):
    """Dag Version serializer for responses."""

    id: UUID
    version_number: int
    dag_id: str
    bundle_name: str | None
    bundle_version: str | None
    created_at: datetime
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))

    bundle_url: str | None = Field(validation_alias="bundle_url")


class DAGVersionCollectionResponse(BaseModel):
    """Dag Version Collection serializer for responses."""

    dag_versions: Iterable[DagVersionResponse]
    total_entries: int


class DagVersionDiffChange(BaseModel):
    """One observed-state change between two serialized Dag versions."""

    path: str
    operation: Literal["added", "removed", "changed"]
    category: Literal[
        "task",
        "dependency",
        "schedule",
        "param",
        "asset",
        "callback",
        "deadline",
        "metadata",
        "provenance",
        "unknown",
    ]
    impact: Literal["execution", "metadata", "provenance", "unknown"]
    before_digest: str | None = None
    after_digest: str | None = None
    before_value: Any | None = None
    after_value: Any | None = None


class DagVersionDiffSourceSide(BaseModel):
    """Source metadata for one side of a Dag version comparison."""

    digest: str | None = None
    content: str | None = None


class DagVersionDiffSource(BaseModel):
    """Source comparison metadata."""

    status: Literal["current_stored_code", "redacted", "unavailable"]
    fidelity: Literal["current_stored_code", "redacted", "unavailable"]
    changed: bool | None = None
    base: DagVersionDiffSourceSide | None = None
    target: DagVersionDiffSourceSide | None = None


class DagVersionDiffValues(BaseModel):
    """Visibility metadata for raw serialized Dag values."""

    status: Literal["available", "unavailable"]


class DagVersionDiffResponse(BaseModel):
    """Observed-state diff response for two Dag versions."""

    diff_schema_version: int
    serialized_dag_schema_versions: dict[str, int | None]
    mode: Literal["observed_state", "unavailable"]
    changes: list[DagVersionDiffChange]
    source: DagVersionDiffSource
    values: DagVersionDiffValues | None = None
    truncated: bool
    unavailable_reason: str | None = None
