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
"""Task workload schemas for executor communication."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import Field

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
from airflow.executors.workloads.base import BaseDagBundleWorkload, BundleInfo
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.models.taskinstance import TaskInstance as TIModel
    from airflow.models.taskinstancekey import TaskInstanceKey


class TaskInstanceDTO(TaskInstance):
    """
    The versioned execution API ``TaskInstance`` schema plus executor-only fields.

    The base class is the single source of truth for the fields a worker needs;
    the fields added here are executor concerns (queueing order and pool
    accounting) the worker never reads.
    """

    pool_slots: int
    priority_weight: int

    external_executor_id: str | None = Field(default=None, exclude=True)
    executor_config: dict | None = Field(default=None, exclude=True)

    # TODO: Task-SDK: Can we replace TaskInstanceKey with just the uuid across the codebase?
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


class ExecuteTask(BaseDagBundleWorkload):
    """Execute the given Task."""

    ti: TaskInstanceDTO
    sentry_integration: str = ""

    type: Literal["ExecuteTask"] = Field(init=False, default="ExecuteTask")

    @property
    def key(self) -> TaskInstanceKey:
        """Return the TaskInstanceKey for this workload."""
        return self.ti.key

    @property
    def display_name(self) -> str:
        """Return the task instance ID as a display name."""
        return str(self.ti.id)

    @property
    def success_state(self) -> TaskInstanceState:
        return TaskInstanceState.SUCCESS

    @property
    def failure_state(self) -> TaskInstanceState:
        return TaskInstanceState.FAILED

    @classmethod
    def make(
        cls,
        ti: TIModel,
        dag_rel_path: Path | None = None,
        generator: JWTGenerator | None = None,
        bundle_info: BundleInfo | None = None,
        sentry_integration: str = "",
    ) -> ExecuteTask:
        """Create an ExecuteTask workload from a TaskInstance ORM model."""
        from airflow.utils.helpers import log_filename_template_renderer

        ser_ti = TaskInstanceDTO.model_validate(ti, from_attributes=True)
        if not bundle_info:
            version_data = None
            if ti.dag_version is not None and ti.dag_run.bundle_version is not None:
                version_data = ti.dag_version.version_data
            bundle_info = BundleInfo(
                name=ti.dag_model.bundle_name,
                version=ti.dag_run.bundle_version,
                version_data=version_data,
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
