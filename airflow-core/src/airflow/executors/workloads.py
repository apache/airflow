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

import os
import uuid
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

import structlog
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.models import DagRun
    from airflow.models.callback import Callback as CallbackModel, CallbackFetchMethod
    from airflow.models.taskinstance import TaskInstance as TIModel
    from airflow.models.taskinstancekey import TaskInstanceKey


__all__ = ["All", "ExecuteTask", "ExecuteCallback"]

log = structlog.get_logger(__name__)


class BaseWorkload(BaseModel):
    token: str
    """The identity token for this workload"""

    @staticmethod
    def generate_token(sub_id: str, generator: JWTGenerator | None = None) -> str:
        return generator.generate({"sub": sub_id}) if generator else ""


class BundleInfo(BaseModel):
    """Schema for telling task which bundle to run with."""

    name: str
    version: str | None = None


class TaskInstance(BaseModel):
    """Schema for TaskInstance with minimal required fields needed for Executors and Task SDK."""

    id: uuid.UUID
    dag_version_id: uuid.UUID
    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    map_index: int = -1

    pool_slots: int
    queue: str
    priority_weight: int
    executor_config: dict | None = Field(default=None, exclude=True)

    parent_context_carrier: dict | None = None
    context_carrier: dict | None = None

    # TODO: Task-SDK: Can we replace TastInstanceKey with just the uuid across the codebase?
    @property
    def key(self) -> TaskInstanceKey:
        from airflow.models.taskinstancekey import TaskInstanceKey

        return TaskInstanceKey(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            try_number=self.try_number,
            map_index=self.map_index,
        )


class Callback(BaseModel):
    """Schema for Callback with minimal required fields needed for Executors and Task SDK."""

    id: uuid.UUID
    fetch_type: CallbackFetchMethod
    data: dict


class BaseDagBundleWorkload(BaseWorkload, ABC):
    """Base class for Workloads that are associated with a DAG bundle."""

    dag_rel_path: os.PathLike[str]
    """The filepath where the DAG can be found (likely prefixed with `DAG_FOLDER/`)"""

    bundle_info: BundleInfo

    log_path: str | None
    """The rendered relative log filename template the task logs should be written to"""


class ExecuteTask(BaseDagBundleWorkload):
    """Execute the given Task."""

    ti: TaskInstance
    sentry_integration: str = ""

    type: Literal["ExecuteTask"] = Field(init=False, default="ExecuteTask")

    @classmethod
    def make(
        cls,
        ti: TIModel,
        dag_rel_path: Path | None = None,
        generator: JWTGenerator | None = None,
        bundle_info: BundleInfo | None = None,
        sentry_integration: str = "",
    ) -> ExecuteTask:
        from airflow.utils.helpers import log_filename_template_renderer

        ser_ti = TaskInstance.model_validate(ti, from_attributes=True)
        ser_ti.parent_context_carrier = ti.dag_run.context_carrier
        if not bundle_info:
            bundle_info = BundleInfo(
                name=ti.dag_model.bundle_name,
                version=ti.dag_run.bundle_version,
            )
        fname = log_filename_template_renderer()(ti=ti)

        return cls(
            ti=ser_ti,
            dag_rel_path=dag_rel_path or Path(ti.dag_model.relative_fileloc or ""),
            token=cls.generate_token(str(ti.id), generator),
            log_path=fname,
            bundle_info=bundle_info,
            sentry_integration=sentry_integration,
        )


class ExecuteCallback(BaseDagBundleWorkload):
    """Execute the given Callback."""

    callback: Callback

    type: Literal["ExecuteCallback"] = Field(init=False, default="ExecuteCallback")

    @classmethod
    def make(
        cls,
        callback: CallbackModel,
        dag_run: DagRun,
        dag_rel_path: Path | None = None,
        generator: JWTGenerator | None = None,
        bundle_info: BundleInfo | None = None,
    ) -> ExecuteCallback:
        if not bundle_info:
            bundle_info = BundleInfo(
                name=dag_run.dag_model.bundle_name,
                version=dag_run.bundle_version,
            )
        fname = f"executor_callbacks/{callback.id}"  # TODO: better log file template

        return cls(
            callback=Callback.model_validate(callback, from_attributes=True),
            dag_rel_path=dag_rel_path or Path(dag_run.dag_model.relative_fileloc or ""),
            token=cls.generate_token(str(callback.id), generator),
            log_path=fname,
            bundle_info=bundle_info,
        )


class RunTrigger(BaseModel):
    """Execute an async "trigger" process that yields events."""

    id: int

    ti: TaskInstance | None
    """
    The task instance associated with this trigger.

    Could be none for asset-based triggers.
    """

    classpath: str
    """
    Dot-separated name of the module+fn to import and run this workload.

    Consumers of this Workload must perform their own validation of this input.
    """

    encrypted_kwargs: str

    timeout_after: datetime | None = None

    type: Literal["RunTrigger"] = Field(init=False, default="RunTrigger")


All = Annotated[
    ExecuteTask | RunTrigger,
    Field(discriminator="type"),
]
