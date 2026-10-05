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
"""Local file parsing workload for the executor proof of concept."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import Field

from airflow.executors.workloads.base import BaseWorkloadSchema, BundleInfo, WorkloadType


@dataclass(frozen=True)
class ParseDagFileKey:
    """Identity of one local parsing submission."""

    id: UUID


class ParseDagFileState(str, Enum):
    """Executor state independent of task and callback state."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class ParseDagFile(BaseWorkloadSchema):
    """Parse one file on the manager's host; paths are private local transport, not a remote API."""

    workload_id: UUID
    bundle_info: BundleInfo
    bundle_path: Path
    relative_path: str
    log_path: str
    control_dir: Path
    timeout: float = Field(gt=0, allow_inf_nan=False)
    callbacks: list[str] = Field(default_factory=list)
    token: str = Field(default="", repr=False)
    queue: str | None = None
    type: Literal[WorkloadType.PARSE_DAG_FILE] = Field(default=WorkloadType.PARSE_DAG_FILE, init=False)

    @property
    def key(self) -> ParseDagFileKey:
        return ParseDagFileKey(self.workload_id)

    @property
    def display_name(self) -> str:
        return f"parse {self.bundle_info.name}/{self.relative_path}"

    @property
    def result_path(self) -> Path:
        return self.control_dir / f"{self.workload_id}.json"

    @property
    def cancel_path(self) -> Path:
        return self.control_dir / f"{self.workload_id}.cancel"

    @property
    def success_state(self) -> ParseDagFileState:
        return ParseDagFileState.SUCCESS

    @property
    def failure_state(self) -> ParseDagFileState:
        return ParseDagFileState.FAILED

    @property
    def running_state(self) -> None:
        return None
