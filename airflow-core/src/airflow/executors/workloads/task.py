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
from sqlalchemy.orm import object_session

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance, task_instance_to_runtime
from airflow.configuration import conf
from airflow.executors.workloads.base import BaseDagBundleWorkload, BundleInfo, WorkloadType
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.models.taskinstance import TaskInstance as TIModel
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.log.task_log_address import TaskLogContext


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

    @property
    def key(self) -> TaskInstanceKey:
        """Identify the attempt the way the ORM does, by its stored index; ``map_index`` is the public one."""
        from airflow.models.taskinstancekey import TaskInstanceKey

        return TaskInstanceKey(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            try_number=self.try_number,
            map_index=self.region_index if self.region_index is not None else self.map_index,
        )


class ExecuteTask(BaseDagBundleWorkload):
    """Execute the given Task."""

    ti: TaskInstanceDTO
    sentry_integration: str = ""

    type: Literal[WorkloadType.EXECUTE_TASK] = Field(init=False, default=WorkloadType.EXECUTE_TASK)

    @property
    def key(self) -> TaskInstanceKey:
        """Return the coordinate key used by existing executor providers."""
        return self.ti.key

    @property
    def sort_key(self) -> int:
        """Return the negated task priority weight so the ascending sort dispatches the highest ``priority_weight`` first."""
        return -self.ti.priority_weight

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
        log_context: TaskLogContext | None = None,
    ) -> ExecuteTask:
        """Create an ExecuteTask workload from a TaskInstance ORM model."""
        from airflow.utils.log.task_log_address import prepare_task_log_contexts, render_task_log_filename

        if log_context is None:
            log_context = prepare_task_log_contexts(
                [ti],
                session=object_session(ti),
                filename_template=conf.get("logging", "log_filename_template"),
            )[ti.id]
        ser_ti = task_instance_to_runtime(ti, model=TaskInstanceDTO, map_index=log_context.map_index)
        if not bundle_info:
            from airflow.models.dag_version import _resolve_version_data

            bundle_info = BundleInfo(
                name=ti.dag_model.bundle_name,
                version=ti.dag_run.bundle_version,
                # Source version_data from the run's pinned version (matching ``version`` above),
                # not the TI's dag_version. A mid-run DAG re-parse can bump the TI's dag_version
                # to a newer version while the run stays pinned; sourcing from created_dag_version
                # keeps the shipped hash and manifest consistent so versioned bundles stay reproducible.
                version_data=_resolve_version_data(ti.dag_run.created_dag_version, ti.dag_run.bundle_version),
            )
        fname = render_task_log_filename(ti, ti.try_number, context=log_context)

        return cls(
            ti=ser_ti,
            dag_rel_path=dag_rel_path or Path(ti.dag_model.relative_fileloc or ""),
            token=cls.generate_token(str(ti.id), generator),
            log_path=fname,
            bundle_info=bundle_info,
            sentry_integration=sentry_integration,
        )
