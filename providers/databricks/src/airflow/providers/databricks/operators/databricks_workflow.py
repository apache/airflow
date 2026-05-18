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
import warnings
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any

from mergedeep import merge

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator, TaskGroup, conf
from airflow.providers.databricks.exceptions import (
    DatabricksWorkflowRepairBudgetExhausted,
    DatabricksWorkflowRepairMetadataError,
    DatabricksWorkflowRepairTriggerError,
)
from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunLifeCycleState
from airflow.providers.databricks.plugins.databricks_workflow import (
    WorkflowJobRepairAllFailedLink,
    WorkflowJobRunLink,
    store_databricks_job_run_link,
)
from airflow.providers.databricks.triggers.databricks import DatabricksWorkflowRepairCoordinatorTrigger
from airflow.providers.databricks.utils.databricks import build_repair_run_json, extract_failed_task_errors
from airflow.providers.databricks.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from types import TracebackType

    from airflow.models.taskmixin import DAGNode
    from airflow.providers.common.compat.sdk import Context


@dataclass
class WorkflowRunMetadata:
    """
    Metadata for a Databricks workflow run.

    :param run_id: The ID of the Databricks workflow run.
    :param job_id: The ID of the Databricks workflow job.
    :param conn_id: The connection ID used to connect to Databricks.
    """

    conn_id: str
    job_id: int
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
    :param access_control_list: List of permissions to set on the job. Array of object
        (AccessControlRequestForUser) or object (AccessControlRequestForGroup) or object
        (AccessControlRequestForServicePrincipal).

        .. seealso::
            The access control list is applied both when creating a new job and when resetting
            an existing job. If access_control_list != [], the supplied ACL replaces the
            job's current permissions. Otherwise, the prior ACL stays in place. You can also manage
            job permissions directly in the Databricks UI.
    :param existing_clusters: A list of existing clusters to use for the workflow.
    :param extra_job_params: A dictionary of extra properties which will override the default Databricks
        Workflow Job definitions.
    :param jar_params: A list of jar parameters to pass to the workflow. These parameters will be passed to
        all jar tasks in the workflow.
    :param job_clusters: A list of job clusters to use for the workflow.
    :param max_concurrent_runs: The maximum number of concurrent runs for the workflow.
    :param notebook_params: A dictionary of notebook parameters to pass to the workflow. These parameters
        will be passed to all notebooks in the workflow.
    :param python_params: A list of python parameters to pass to the workflow. These parameters will be
        passed to all python tasks in the workflow.
    :param spark_submit_params: A list of spark submit parameters to pass to the workflow. These parameters
        will be passed to all spark submit tasks.
    :param tasks_to_convert: A dict of tasks to convert to a Databricks workflow. This list can also be
        populated after instantiation using the `add_task` method.
    """

    template_fields = (
        "databricks_conn_id",
        "access_control_list",
        "existing_clusters",
        "extra_job_params",
        "jar_params",
        "job_clusters",
        "max_concurrent_runs",
        "notebook_params",
        "python_params",
        "spark_submit_params",
    )
    caller = "_CreateDatabricksWorkflowOperator"
    # Conditionally set operator_extra_links based on Airflow version
    if AIRFLOW_V_3_0_PLUS:
        # In Airflow 3, disable "Repair All Failed Tasks" since we can't pre-determine failed tasks
        operator_extra_links = (WorkflowJobRunLink(),)
    else:
        # In Airflow 2.x, keep both links
        operator_extra_links = (  # type: ignore[assignment]
            WorkflowJobRunLink(),
            WorkflowJobRepairAllFailedLink(),
        )

    def __init__(
        self,
        task_id: str,
        databricks_conn_id: str,
        access_control_list: list[dict] | None = None,
        existing_clusters: list[str] | None = None,
        extra_job_params: dict[str, Any] | None = None,
        jar_params: list[str] | None = None,
        job_clusters: list[dict[str, object]] | None = None,
        max_concurrent_runs: int = 1,
        notebook_params: dict | None = None,
        python_params: list | None = None,
        spark_submit_params: list | None = None,
        tasks_to_convert: dict[str, BaseOperator] | None = None,
        **kwargs,
    ):
        self.databricks_conn_id = databricks_conn_id
        self.access_control_list = access_control_list
        self.existing_clusters = existing_clusters or []
        self.extra_job_params = extra_job_params or {}
        self.jar_params = jar_params or []
        self.job_clusters = job_clusters or []
        self.max_concurrent_runs = max_concurrent_runs
        self.notebook_params = notebook_params or {}
        self.python_params = python_params or []
        self.spark_submit_params = spark_submit_params or []
        self.tasks_to_convert = tasks_to_convert or {}
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

    def add_task(self, task_id, task: BaseOperator) -> None:
        """Add a task to the dict of tasks to convert to a Databricks workflow."""
        self.tasks_to_convert[task_id] = task

    @property
    def job_name(self) -> str:
        if not self.task_group:
            raise AirflowException("Task group must be set before accessing job_name")
        return f"{self.dag_id}.{self.task_group.group_id}"

    def create_workflow_json(self, context: Context | None = None) -> dict[str, object]:
        """Create a workflow json to be used in the Databricks API."""
        task_json = [
            task._convert_to_databricks_workflow_task(  # type: ignore[attr-defined]
                relevant_upstreams=self.relevant_upstreams, task_dict=self.tasks_to_convert, context=context
            )
            for task_id, task in self.tasks_to_convert.items()
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

        if self.access_control_list is not None:
            default_json["access_control_list"] = self.access_control_list

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
                "jar_params": self.jar_params,
                "notebook_params": self.notebook_params,
                "python_params": self.python_params,
                "spark_submit_params": self.spark_submit_params,
            }
        )

        self._wait_for_job_to_start(run_id)

        self.workflow_run_metadata = WorkflowRunMetadata(
            self.databricks_conn_id,
            job_id,
            run_id,
        )

        # Store operator links in XCom for Airflow 3 compatibility
        if AIRFLOW_V_3_0_PLUS:
            # Store the job run link
            store_databricks_job_run_link(
                context=context,
                metadata=self.workflow_run_metadata,
                logger=self.log,
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
                "Run: %(run_id)s of job_id: %(job_id)s was requested to be cancelled.",
                {"run_id": run_id, "job_id": job_id},
            )
        else:
            self.log.error(
                """
                Error: Workflow Run metadata is not populated, so the run was not canceled. This could be due
                to the workflow not being started or an error in the workflow creation process.
                """
            )


class _DatabricksFullRunRepairCoordinatorOperator(BaseOperator):
    """
    Watch a Databricks Workflow run and trigger ``rerun_all_failed_tasks`` repairs.

    Runs as a sibling of the downstream Databricks task monitors inside a
    :class:`DatabricksWorkflowTaskGroup`. The ``launch`` task creates or resets the job, starts the
    run, and publishes ``{conn_id, job_id, run_id}`` so monitors can fan out. This operator then
    owns the parent-run repair budget, waits for terminal run states, and issues one repair call per
    failed batch.

    Downstream task monitors observe repairs independently from the Databricks API when their
    sub-run fails; they do not share repair state through XCom. The coordinator's final return
    value carries ``{run_id, repair_attempts, latest_repair_id}`` for any user code that wants a
    post-run summary.

    :param task_id: The task id of the operator (typically ``"repair_coordinator"``).
    :param databricks_conn_id: Connection id used by the coordinator trigger and repair calls.
    :param launch_task_id: The full task id of the workflow ``launch`` task whose return value
        carries ``{conn_id, job_id, run_id}``.
    :param max_full_run_repairs: Total repair attempts allowed across the run.
    :param repair_polling_period_seconds: Poll interval used by the trigger or sync poll loop.
    :param databricks_retry_limit: Hook retry limit for transient API failures.
    :param databricks_retry_delay: Hook retry delay (seconds).
    :param databricks_retry_args: Optional ``tenacity.Retrying`` kwargs forwarded to the hook.
    :param deferrable: If ``True``, watch the run by deferring on
        :class:`DatabricksWorkflowRepairCoordinatorTrigger`. If ``False``, watch it via a
        synchronous poll loop that runs the same state machine inline.
    """

    caller = "_DatabricksFullRunRepairCoordinatorOperator"

    def __init__(
        self,
        task_id: str,
        databricks_conn_id: str,
        launch_task_id: str,
        max_full_run_repairs: int,
        repair_polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 10,
        databricks_retry_args: dict[Any, Any] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        if max_full_run_repairs < 1:
            raise ValueError(
                f"max_full_run_repairs must be >= 1 for the workflow coordinator task, got {max_full_run_repairs}"
            )
        super().__init__(task_id=task_id, **kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.launch_task_id = launch_task_id
        self.max_full_run_repairs = max_full_run_repairs
        self.repair_polling_period_seconds = repair_polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.deferrable = deferrable

    @cached_property
    def _hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=self.caller,
        )

    def _make_trigger(
        self,
        run_id: int,
        repair_attempts: int,
        latest_repair_id: int | None,
    ) -> DatabricksWorkflowRepairCoordinatorTrigger:
        return DatabricksWorkflowRepairCoordinatorTrigger(
            run_id=run_id,
            databricks_conn_id=self.databricks_conn_id,
            max_full_run_repairs=self.max_full_run_repairs,
            repair_attempts=repair_attempts,
            latest_repair_id=latest_repair_id,
            polling_period_seconds=self.repair_polling_period_seconds,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=self.caller,
        )

    def execute(self, context: Context) -> Any:
        launch_value = context["ti"].xcom_pull(task_ids=self.launch_task_id)
        if not launch_value:
            raise DatabricksWorkflowRepairMetadataError(
                f"Launch task {self.launch_task_id!r} did not publish workflow run metadata; "
                "cannot coordinate repairs."
            )
        metadata = WorkflowRunMetadata(**launch_value)

        if self.deferrable:
            self.defer(
                trigger=self._make_trigger(
                    run_id=metadata.run_id,
                    repair_attempts=0,
                    latest_repair_id=None,
                ),
                method_name="execute_complete",
            )
        return self._run_sync(metadata.run_id)

    def _run_sync(self, run_id: int) -> dict[str, Any]:
        """Sync equivalent of :class:`DatabricksWorkflowRepairCoordinatorTrigger`'s state machine."""
        repair_attempts = 0
        latest_repair_id: int | None = None
        while True:
            run_state = self._hook.get_run_state(run_id)
            while not run_state.is_terminal:
                self.log.info(
                    "Databricks run %s in state %s. Sleeping for %s seconds.",
                    run_id,
                    run_state,
                    self.repair_polling_period_seconds,
                )
                time.sleep(self.repair_polling_period_seconds)
                run_state = self._hook.get_run_state(run_id)

            if run_state.is_successful:
                self.log.info(
                    "Databricks workflow run %s completed (repair_attempts=%s).",
                    run_id,
                    repair_attempts,
                )
                return {
                    "run_id": run_id,
                    "repair_attempts": repair_attempts,
                    "latest_repair_id": latest_repair_id,
                }

            run_info = self._hook.get_run(run_id)
            errors = extract_failed_task_errors(self._hook, run_info, run_state)

            if repair_attempts >= self.max_full_run_repairs:
                raise DatabricksWorkflowRepairBudgetExhausted(
                    f"Databricks workflow run {run_id} failed after {repair_attempts} repair "
                    f"attempt(s); repair budget exhausted (max_full_run_repairs={self.max_full_run_repairs}). "
                    f"Errors: {errors}"
                )

            self.log.info(
                "Databricks run %s reached terminal failure state %s. Repairing all failed "
                "tasks (attempt %s of %s, latest_repair_id=%s).",
                run_id,
                run_state.result_state,
                repair_attempts + 1,
                self.max_full_run_repairs,
                latest_repair_id,
            )
            repair_json = build_repair_run_json(
                run_id=run_id,
                latest_repair_id=latest_repair_id,
                overriding_parameters=run_info.get("overriding_parameters"),
            )
            latest_repair_id = self._hook.repair_run(repair_json)
            repair_attempts += 1
            self.log.info(
                "Databricks repair_run accepted for run %s; new repair_id=%s.",
                run_id,
                latest_repair_id,
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        status = event.get("status")
        run_id = event["run_id"]
        repair_attempts = event["repair_attempts"]
        latest_repair_id = event.get("latest_repair_id")

        if status == "completed":
            self.log.info(
                "Databricks workflow run %s completed (repair_attempts=%s).",
                run_id,
                repair_attempts,
            )
            return {
                "run_id": run_id,
                "repair_attempts": repair_attempts,
                "latest_repair_id": latest_repair_id,
            }

        if status == "repaired":
            self.log.info(
                "Databricks workflow run %s repaired (repair_attempts=%s, latest_repair_id=%s); "
                "re-deferring to monitor the repaired run.",
                run_id,
                repair_attempts,
                latest_repair_id,
            )
            self.defer(
                trigger=self._make_trigger(
                    run_id=run_id,
                    repair_attempts=repair_attempts,
                    latest_repair_id=latest_repair_id,
                ),
                method_name="execute_complete",
            )
            return None

        if status == "failed":
            errors = event.get("errors", [])
            raise DatabricksWorkflowRepairBudgetExhausted(
                f"Databricks workflow run {run_id} failed after {repair_attempts} repair "
                f"attempt(s); repair budget exhausted (max_full_run_repairs={self.max_full_run_repairs}). "
                f"Errors: {errors}"
            )

        raise DatabricksWorkflowRepairTriggerError(
            f"DatabricksWorkflowRepairCoordinatorTrigger emitted unexpected status {status!r}: {event}"
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
    :param access_control_list: List of permissions to set on the job. Array of object
        (AccessControlRequestForUser) or object (AccessControlRequestForGroup) or object
        (AccessControlRequestForServicePrincipal).

        .. seealso::
            When provided, this is applied when the job is created and when an existing job is
            reset. Setting this replaces any existing job permissions, unless its value is
            an empty list. If access_control_list == [], the prior ACL remains in place.
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
    :param max_full_run_repairs: Maximum number of automatic ``rerun_all_failed_tasks`` repair attempts to
        issue against the Databricks run when downstream tasks fail. Each repair reuses the
        original job cluster. Set to ``0`` to disable auto-repair (current behavior). Only takes
        effect on Airflow 3+; ignored on Airflow 2.x. Defaults to ``0``.
    :param repair_polling_period_seconds: How often the repair coordinator polls the
        Databricks run state. Only used when ``max_full_run_repairs > 0``.
    """

    is_databricks = True

    def __init__(
        self,
        databricks_conn_id: str,
        access_control_list: list[dict] | None = None,
        existing_clusters: list[str] | None = None,
        extra_job_params: dict[str, Any] | None = None,
        jar_params: list[str] | None = None,
        job_clusters: list[dict] | None = None,
        max_concurrent_runs: int = 1,
        notebook_packages: list[dict[str, Any]] | None = None,
        notebook_params: dict | None = None,
        python_params: list | None = None,
        spark_submit_params: list | None = None,
        max_full_run_repairs: int = 0,
        repair_polling_period_seconds: int = 30,
        **kwargs,
    ):
        if max_full_run_repairs < 0:
            raise ValueError(f"max_full_run_repairs must be >= 0, got {max_full_run_repairs}")
        self.databricks_conn_id = databricks_conn_id
        self.access_control_list = access_control_list
        self.existing_clusters = existing_clusters or []
        self.extra_job_params = extra_job_params or {}
        self.jar_params = jar_params or []
        self.job_clusters = job_clusters or []
        self.max_concurrent_runs = max_concurrent_runs
        self.notebook_packages = notebook_packages or []
        self.notebook_params = notebook_params or {}
        self.python_params = python_params or []
        self.spark_submit_params = spark_submit_params or []
        self.max_full_run_repairs = max_full_run_repairs
        self.repair_polling_period_seconds = repair_polling_period_seconds
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
            access_control_list=self.access_control_list,
            existing_clusters=self.existing_clusters,
            extra_job_params=self.extra_job_params,
            jar_params=self.jar_params,
            job_clusters=self.job_clusters,
            max_concurrent_runs=self.max_concurrent_runs,
            notebook_params=self.notebook_params,
            python_params=self.python_params,
            spark_submit_params=self.spark_submit_params,
        )

        try:
            for task in tasks:
                if not (
                    hasattr(task, "_convert_to_databricks_workflow_task")
                    and callable(task._convert_to_databricks_workflow_task)
                ):
                    raise AirflowException(
                        f"Task {task.task_id} does not support conversion to databricks workflow task."
                    )

                if AIRFLOW_V_3_0_PLUS and self.max_full_run_repairs > 0:
                    task_retries = getattr(task, "retries", 0)
                    if isinstance(task_retries, int) and task_retries > 0:
                        warnings.warn(
                            f"Task {task.task_id!r} in DatabricksWorkflowTaskGroup "
                            f"{self.group_id!r} has retries={task_retries} while "
                            f"max_full_run_repairs={self.max_full_run_repairs}. Databricks-side repair supersedes "
                            "task-level retries for sub-run failures: a failed sub-run defers the "
                            "monitor on the repair-wait trigger and resumes in the same Airflow "
                            "attempt. Retries on the monitor will not trigger additional repairs "
                            "and only add cost on non-repair-related transient failures. Consider "
                            "setting retries=0 on workflow tasks when max_full_run_repairs > 0.",
                            UserWarning,
                            stacklevel=2,
                        )
                task.workflow_run_metadata = create_databricks_workflow_task.output
                create_databricks_workflow_task.relevant_upstreams.append(task.task_id)
                create_databricks_workflow_task.add_task(task.task_id, task)

            for root_task in roots:
                root_task.set_upstream(create_databricks_workflow_task)

            if AIRFLOW_V_3_0_PLUS and self.max_full_run_repairs > 0:
                repair_coordinator_task = _DatabricksFullRunRepairCoordinatorOperator(
                    dag=self.dag,
                    task_group=self,
                    task_id="repair_coordinator",
                    databricks_conn_id=self.databricks_conn_id,
                    launch_task_id=create_databricks_workflow_task.task_id,
                    max_full_run_repairs=self.max_full_run_repairs,
                    repair_polling_period_seconds=self.repair_polling_period_seconds,
                    # Retrying the coordinator would re-enter execute() with repair_attempts=0
                    # and start the budget over.
                    retries=0,
                )
                repair_coordinator_task.set_upstream(create_databricks_workflow_task)
        finally:
            super().__exit__(_type, _value, _tb)
