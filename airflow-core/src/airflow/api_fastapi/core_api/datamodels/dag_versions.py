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
from enum import Enum
from typing import Annotated, Any, Literal
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


class DagVersionDiffCategory(str, Enum):
    """What part of a Dag a difference belongs to. Mirrors ``DiffCategory``."""

    ASSET = "asset"
    AUTHORIZATION = "authorization"
    CALLBACK = "callback"
    DEADLINE = "deadline"
    DEPENDENCY = "dependency"
    METADATA = "metadata"
    PARAM = "param"
    PROVENANCE = "provenance"
    SCHEDULE = "schedule"
    TASK = "task"
    UNKNOWN = "unknown"


class DagVersionDiffImpact(str, Enum):
    """What a difference affects. Mirrors ``DiffImpact``."""

    AUTHORIZATION = "authorization"
    EXECUTION = "execution"
    METADATA = "metadata"
    PROVENANCE = "provenance"
    UNKNOWN = "unknown"


class DagVersionDiffOperation(str, Enum):
    """How a difference presents at its path."""

    ADDED = "added"
    REMOVED = "removed"
    CHANGED = "changed"


class DagVersionDiffMode(str, Enum):
    """Whether a comparison could be made at all."""

    OBSERVED_STATE = "observed_state"
    UNAVAILABLE = "unavailable"


class DagVersionDiffValuesStatus(str, Enum):
    """Whether the caller was authorized to see values. Mirrors ``ValuesStatus``."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class DagVersionDiffChangeResponse(BaseModel):
    """One structural difference between two stored Dag versions."""

    path: str
    operation: DagVersionDiffOperation
    category: DagVersionDiffCategory
    impact: DagVersionDiffImpact
    occurrence_count: Annotated[
        int,
        Field(
            description=(
                "How many underlying changes this record stands for. Always 1 when values are "
                "disclosed, since each change is then its own record; a redacted record merges "
                "every change sharing its path and operation."
            )
        ),
    ]

    # Only populated when the caller is authorized to see values. A digest is null for the side a
    # change does not have; a value is omitted entirely, which is how a missing side is told apart
    # from a stored null.
    before_digest: str | None = None
    after_digest: str | None = None
    before_value: Any | None = None
    after_value: Any | None = None


class DagVersionDiffResponse(BaseModel):
    """Observed-state difference between two stored Dag versions."""

    diff_schema_version: int
    base_version_number: int
    target_version_number: int
    serialized_dag_schema_versions: dict[Literal["base", "target"], int | None]
    mode: DagVersionDiffMode
    unavailable_reason: str | None = None
    values_status: DagVersionDiffValuesStatus
    truncated: Annotated[
        bool,
        Field(
            description=(
                "Whether a change at a path not already in `changes` was dropped to stay within "
                "`max_changes`. Paths that are absent are absent, not unchanged."
            )
        ),
    ]
    total_changes: Annotated[
        int,
        Field(
            description=(
                "Underlying changes across every disclosed path, not the number of records. Exact "
                "when `truncated` is false; a lower bound when it is true, because the changes at "
                "dropped paths are not counted."
            )
        ),
    ]
    changes: list[DagVersionDiffChangeResponse]
