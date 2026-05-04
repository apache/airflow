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
"""Callback workload schemas for executor communication."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import structlog
from pydantic import BaseModel, Field, field_validator

from airflow.executors.workloads.base import BaseDagBundleWorkload, BundleInfo
from airflow.utils.state import CallbackState

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.models import DagRun
    from airflow.models.callback import Callback as CallbackModel, CallbackKey

log = structlog.get_logger(__name__)


class CallbackFetchMethod(str, Enum):
    """Methods used to fetch callback at runtime."""

    # For future use once Dag Processor callbacks (on_success_callback/on_failure_callback) get moved to executors
    DAG_ATTRIBUTE = "dag_attribute"

    # For deadline callbacks since they import callbacks through the import path
    IMPORT_PATH = "import_path"


class CallbackDTO(BaseModel):
    """Schema for Callback with minimal required fields needed for Executors and Task SDK."""

    id: str  # A uuid.UUID stored as a string
    fetch_method: CallbackFetchMethod
    data: dict

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, v):
        """Convert UUID to str if needed."""
        if isinstance(v, UUID):
            return str(v)
        return v

    @property
    def key(self) -> CallbackKey:
        """Return callback ID as key (CallbackKey = str)."""
        return self.id


class ExecuteCallback(BaseDagBundleWorkload):
    """Execute the given Callback."""

    callback: CallbackDTO

    type: Literal["ExecuteCallback"] = Field(init=False, default="ExecuteCallback")

    @property
    def key(self) -> CallbackKey:
        """Return the callback key for this workload."""
        return self.callback.key

    @property
    def display_name(self) -> str:
        """Return a human-readable name for logging and process titles."""
        if path := self.callback.data.get("path", ""):
            # Use just the function/class name for brevity in process titles.
            # The full path and UUID are available in log messages if needed.
            return path.rsplit(".", 1)[-1]
        return str(self.callback.id)

    @property
    def success_state(self) -> CallbackState:
        return CallbackState.SUCCESS

    @property
    def failure_state(self) -> CallbackState:
        return CallbackState.FAILED

    @property
    def running_state(self) -> CallbackState:
        return CallbackState.RUNNING

    @classmethod
    def make(
        cls,
        callback: CallbackModel,
        dag_run: DagRun,
        dag_rel_path: Path | None = None,
        generator: JWTGenerator | None = None,
        bundle_info: BundleInfo | None = None,
    ) -> ExecuteCallback:
        """Create an ExecuteCallback workload from a Callback ORM model."""
        if not bundle_info:
            bundle_info = BundleInfo(
                name=dag_run.dag_model.bundle_name,
                version=dag_run.bundle_version,
            )
        fname = f"executor_callbacks/{dag_run.dag_id}/{dag_run.run_id}/{callback.id}"

        return cls(
            callback=CallbackDTO.model_validate(callback, from_attributes=True),
            dag_rel_path=dag_rel_path or Path(dag_run.dag_model.relative_fileloc or ""),
            token=cls.generate_token(str(callback.id), generator),
            log_path=fname,
            bundle_info=bundle_info,
        )
