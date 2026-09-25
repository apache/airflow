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
"""Experimental filesystem definition transport for the executor parsing proof of concept."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath, PureWindowsPath
from typing import ClassVar, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from airflow.executors.workloads.base import BaseWorkloadSchema, BundleInfo, WorkloadType

MAX_PARSING_REQUEST_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class ParseDagDefinitionsKey:
    """Executor identity distinct from task and callback keys."""

    id: UUID

    def __str__(self) -> str:
        return str(self.id)


class ParseDagDefinitionsState(str, Enum):
    """Executor lifecycle; per-definition receipts determine parsing outcomes."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class DagDefinitionAttempt(BaseModel):
    """One filesystem definition resolved against the worker's configured bundle root."""

    model_config = ConfigDict(extra="forbid")

    attempt_id: UUID
    relative_path: str = Field(min_length=1)
    source_revision: str = Field(min_length=1)
    timeout_seconds: float = Field(gt=0, allow_inf_nan=False)

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or PureWindowsPath(value).drive
            or ".." in path.parts
            or "\\" in value
            or "\x00" in value
            or not path.parts
        ):
            raise ValueError("Definition paths must be relative POSIX paths within the bundle")
        return value


class ParseDagDefinitions(BaseWorkloadSchema):
    """A bounded parsing batch; enabled only on dedicated prototype executor instances."""

    model_config = ConfigDict(extra="forbid")
    token_scope: ClassVar[str] = "dag-parsing-poc"

    workload_id: UUID
    bundle_info: BundleInfo
    definitions: tuple[DagDefinitionAttempt, ...] = Field(min_length=1, max_length=100)
    start_deadline: AwareDatetime
    stop_deadline: AwareDatetime
    token: str = Field(repr=False, min_length=1)
    queue: str | None = None
    type: Literal[WorkloadType.PARSE_DAG_DEFINITIONS] = Field(
        init=False, default=WorkloadType.PARSE_DAG_DEFINITIONS
    )

    @model_validator(mode="after")
    def validate_batch(self) -> ParseDagDefinitions:
        if self.stop_deadline <= self.start_deadline:
            raise ValueError("stop_deadline must follow start_deadline")
        if len({definition.attempt_id for definition in self.definitions}) != len(self.definitions):
            raise ValueError("Definition attempt IDs must be unique within a workload")
        return self

    @property
    def key(self) -> ParseDagDefinitionsKey:
        return ParseDagDefinitionsKey(self.workload_id)

    @property
    def display_name(self) -> str:
        return f"parse {self.bundle_info.name} {self.workload_id}"

    @property
    def success_state(self) -> ParseDagDefinitionsState:
        return ParseDagDefinitionsState.SUCCESS

    @property
    def failure_state(self) -> ParseDagDefinitionsState:
        return ParseDagDefinitionsState.FAILED

    @property
    def running_state(self) -> ParseDagDefinitionsState:
        return ParseDagDefinitionsState.RUNNING


class DagDefinitionResult(BaseModel):
    """Serialized publication envelope shared by the prototype worker and receipt API."""

    model_config = ConfigDict(extra="forbid")

    attempt_id: UUID
    relative_path: str
    source_revision: str
    outcome: Literal["success", "import_error", "timeout", "worker_error"]
    serialized_dags: list[dict[str, JsonValue]] = Field(default_factory=list)
    source_code: str | None = Field(default=None, repr=False)
    diagnostics: list[str] = Field(default_factory=list)
    import_errors: dict[str, str] = Field(default_factory=dict)
    warnings: list[JsonValue] = Field(default_factory=list)
    duration_seconds: float = Field(ge=0, allow_inf_nan=False)
