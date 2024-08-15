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

import json
import time
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any

from mergedeep import merge

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunLifeCycleState
from airflow.providers.databricks.plugins.databricks_workflow import (
    WorkflowJobRepairAllFailedLink,
    WorkflowJobRunLink,
)
from airflow.utils.task_group import TaskGroup
from tests.providers.google.cloud.transfers.test_gcs_to_bigquery import job_id

if TYPE_CHECKING:
    from types import TracebackType

    from airflow.models.taskmixin import DAGNode
    from airflow.utils.context import Context


@dataclass
class WorkflowRunMetadata:
    """
    Metadata for a Databricks workflow run.

    :param run_id: The ID of the Databricks workflow run.
    :param job_id: The ID of the Databricks workflow job.
    :param conn_id: The connection ID used to connect to Databricks.
    """

    conn_id: str
    job_id: str
    run_id: int


def _flatten_node(
    node: TaskGroup | BaseOperator | DAGNode, tasks: list[BaseOperator] | None = None
) -> list[BaseOperator]:
    """Flatten a node (either a TaskGroup or Operator) to a list of nodes."""
    if tasks is None:
        tasks = []
    if isinstance(node, BaseOperator):
        return [node]

    if isinstance(node, TaskGroup):
        new_tasks = []
        for _, child in node.children.items():
            new_tasks += _flatten_node(child, tasks)

        return tasks + new_tasks

    return tasks


class _CreateDatabricksWorkflowOperator(BaseOperator):
    """
    Creates a Databricks workflow from a DatabricksWorkflowTaskGroup specified in a DAG.

    :param task_id: The task_id of the operator
    :param databricks_conn_id: The connection ID to use when connecting to Databricks.
    :param existing_clusters: A list of existing clusters to use for the workflow.
    :param extra_job_params: A dictionary of extra properties which will override the default Databricks
        Workflow Job definitions.
    :param job_clusters: A list of job clusters to use for the workflow.
    :param max_concurrent_runs: The maximum number of concurrent runs for the workflow.
    :param notebook_params: A dictionary of notebook parameters to pass to the workflow. These parameters
        will be passed to all notebooks in the workflow.
    :param tasks_to_convert: A list of tasks to convert to a Databricks workflow. This list can also be
        populated after instantiation using the `add_task` method.
    """

    operator_extra_links = (WorkflowJobRunLink(), WorkflowJobRepairAllFailedLink())
    template_fields = ("notebook_params",)
    caller = "_CreateDatabricksWorkflowOperator"

    def __init__(
        self,
        task_id: str,
        databricks_conn_id: str,
        existing_clusters: list[str] | None = None,
        extra_job_params: dict[str, Any] | None = None,
        job_clusters: list[dict[str, object]] | None = None,
        max_concurrent_runs: int = 1,
        notebook_params: dict | None = None,
        tasks_to_convert: list[BaseOperator] | None = None,
        **kwargs,
    ):
        self.databricks_conn_id = databricks_conn_id
        self.existing_clusters = existing_clusters or []
        self.extra_job_params = extra_job_params or {}
        self.job_clusters = job_clusters or []
        self.max_concurrent_runs = max_concurrent_runs
        self.notebook_params = notebook_params or {}
        self.tasks_to_convert = tasks_to_convert or []
        self.relevant_upstreams = [task_id]
        self.workflow_run_metadata: WorkflowRunMetadata | None = None
        super().__init__(task_id=task_id, **kwargs)

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            caller=caller,
        )

    @cached_property
    def _hook(self) -> DatabricksHook:
        return self._get_hook(caller=self.caller)

    def add_task(self, task: BaseOperator) -> None:
        """Add a task to the list of tasks to convert to a Databricks workflow."""
        self.tasks_to_convert.append(task)

    @property
    def job_name(self) -> str:
        if not self.task_group:
            raise AirflowException("Task group must be set before accessing job_name")
        return f"{self.dag_id}.{self.task_group.group_id}"

    def create_workflow_json(self, context: Context | None = None) -> dict[str, object]:
        """Create a workflow json to be used in the Databricks API."""
        task_json = [
            task._convert_to_databricks_workflow_task(  # type: ignore[attr-defined]
                relevant_upstreams=self.relevant_upstreams, context=context
            )
            for task in self.tasks_to_convert
        ]

        default_json = {
            "name": self.job_name,
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "tasks": task_json,
            "format": "MULTI_TASK",
            "job_clusters": self.job_clusters,
            "max_concurrent_runs": self.max_concurrent_runs,
        }
        return merge(default_json, self.extra_job_params)

    def _create_or_reset_job(self, context: Context) -> int:
        job_spec = self.create_workflow_json(context=context)
        existing_jobs = self._hook.list_jobs(job_name=self.job_name)
        job_id = existing_jobs[0]["job_id"] if existing_jobs else None
        if job_id:
            self.log.info(
                "Updating existing Databricks workflow job %s with spec %s",
                self.job_name,
                json.dumps(job_spec, indent=2),
            )
            self._hook.reset_job(job_id, job_spec)
        else:
            self.log.info(
                "Creating new Databricks workflow job %s with spec %s",
                self.job_name,
                json.dumps(job_spec, indent=2),
            )
            job_id = self._hook.create_job(job_spec)
        return job_id

    def _wait_for_job_to_start(self, run_id: int) -> None:
        run_url = self._hook.get_run_page_url(run_id)
        self.log.info("Check the progress of the Databricks job at %s", run_url)
        life_cycle_state = self._hook.get_run_state(run_id).life_cycle_state
        if life_cycle_state not in (
            RunLifeCycleState.PENDING.value,
            RunLifeCycleState.RUNNING.value,
            RunLifeCycleState.BLOCKED.value,
        ):
            raise AirflowException(f"Could not start the workflow job. State: {life_cycle_state}")
        while life_cycle_state in (RunLifeCycleState.PENDING.value, RunLifeCycleState.BLOCKED.value):
            self.log.info("Waiting for the Databricks job to start running")
            time.sleep(5)
            life_cycle_state = self._hook.get_run_state(run_id).life_cycle_state
        self.log.info("Databricks job started. State: %s", life_cycle_state)

    def execute(self, context: Context) -> Any:
        if not isinstance(self.task_group, DatabricksWorkflowTaskGroup):
            raise AirflowException("Task group must be a DatabricksWorkflowTaskGroup")

        job_id = self._create_or_reset_job(context)

        run_id = self._hook.run_now(
            {
                "job_id": job_id,
                "jar_params": self.task_group.jar_params,
                "notebook_params": self.notebook_params,
                "python_params": self.task_group.python_params,
                "spark_submit_params": self.task_group.spark_submit_params,
            }
        )

        self._wait_for_job_to_start(run_id)

        self.workflow_run_metadata = WorkflowRunMetadata(
            self.databricks_conn_id,
            job_id,
            run_id,
        )

        return {
            "conn_id": self.databricks_conn_id,
            "job_id": job_id,
            "run_id": run_id,
        }

    def on_kill(self) -> None:

        if self.workflow_run_metadata:

            run_id = self.workflow_run_metadata.run_id
            job_id = self.workflow_run_metadata.job_id

            self._hook.cancel_run(run_id)
            self.log.info(
                f"Run: {run_id} of job_id: {job_id} was requested to be cancelled."
            )
        else:
            self.log.error(
                f"""
                Error: Workflow Run metadata is not populated, so the run was not canceled. This could be due
                to the workflow not being started or an error in the workflow creation process.
                """
            )


class DatabricksWorkflowTaskGroup(TaskGroup):
    """
    A task group that takes a list of tasks and creates a databricks workflow.

    The DatabricksWorkflowTaskGroup takes a list of tasks and creates a databricks workflow
    based on the metadata produced by those tasks. For a task to be eligible for this
    TaskGroup, it must contain the ``_convert_to_databricks_workflow_task`` method. If any tasks
    do not contain this method then the Taskgroup will raise an error at parse time.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksWorkflowTaskGroup`

    :param databricks_conn_id: The name of the databricks connection to use.
    :param existing_clusters: A list of existing clusters to use for this workflow.
    :param extra_job_params: A dictionary containing properties which will override the default
        Databricks Workflow Job definitions.
    :param jar_params: A list of jar parameters to pass to the workflow. These parameters will be passed to all jar
        tasks in the workflow.
    :param job_clusters: A list of job clusters to use for this workflow.
    :param max_concurrent_runs: The maximum number of concurrent runs for this workflow.
    :param notebook_packages: A list of dictionary of Python packages to be installed. Packages defined
        at the workflow task group level are installed for each of the notebook tasks under it. And
        packages defined at the notebook task level are installed specific for the notebook task.
    :param notebook_params: A dictionary of notebook parameters to pass to the workflow. These parameters
        will be passed to all notebook tasks in the workflow.
    :param python_params: A list of python parameters to pass to the workflow. These parameters will be passed to
        all python tasks in the workflow.
    :param spark_submit_params: A list of spark submit parameters to pass to the workflow. These parameters
        will be passed to all spark submit tasks.
    """

    is_databricks = True

    def __init__(
        self,
        databricks_conn_id: str,
        existing_clusters: list[str] | None = None,
        extra_job_params: dict[str, Any] | None = None,
        jar_params: list[str] | None = None,
        job_clusters: list[dict] | None = None,
        max_concurrent_runs: int = 1,
        notebook_packages: list[dict[str, Any]] | None = None,
        notebook_params: dict | None = None,
        python_params: list | None = None,
        spark_submit_params: list | None = None,
        **kwargs,
    ):
        self.databricks_conn_id = databricks_conn_id
        self.existing_clusters = existing_clusters or []
        self.extra_job_params = extra_job_params or {}
        self.jar_params = jar_params or []
        self.job_clusters = job_clusters or []
        self.max_concurrent_runs = max_concurrent_runs
        self.notebook_packages = notebook_packages or []
        self.notebook_params = notebook_params or {}
        self.python_params = python_params or []
        self.spark_submit_params = spark_submit_params or []
        super().__init__(**kwargs)

    def __exit__(
        self, _type: type[BaseException] | None, _value: BaseException | None, _tb: TracebackType | None
    ) -> None:
        """Exit the context manager and add tasks to a single ``_CreateDatabricksWorkflowOperator``."""
        roots = list(self.get_roots())
        tasks = _flatten_node(self)

        create_databricks_workflow_task = _CreateDatabricksWorkflowOperator(
            dag=self.dag,
            task_group=self,
            task_id="launch",
            databricks_conn_id=self.databricks_conn_id,
            existing_clusters=self.existing_clusters,
            extra_job_params=self.extra_job_params,
            job_clusters=self.job_clusters,
            max_concurrent_runs=self.max_concurrent_runs,
            notebook_params=self.notebook_params,
        )

        for task in tasks:
            if not (
                hasattr(task, "_convert_to_databricks_workflow_task")
                and callable(task._convert_to_databricks_workflow_task)
            ):
                raise AirflowException(
                    f"Task {task.task_id} does not support conversion to databricks workflow task."
                )

            task.workflow_run_metadata = create_databricks_workflow_task.output
            create_databricks_workflow_task.relevant_upstreams.append(task.task_id)
            create_databricks_workflow_task.add_task(task)

        for root_task in roots:
            root_task.set_upstream(create_databricks_workflow_task)

        super().__exit__(_type, _value, _tb)
