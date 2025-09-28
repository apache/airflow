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

import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from functools import cache
from typing import TYPE_CHECKING

import psutil
from openlineage.client.serde import Serde

from airflow import settings
from airflow.listeners import hookimpl
from airflow.models import DagRun, TaskInstance
from airflow.observability.stats import Stats
from airflow.providers.common.compat.sdk import timeout, timezone
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors import ExtractorManager, OperatorLineage
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter, RunState
from airflow.providers.openlineage.utils.utils import (
    AIRFLOW_V_3_0_PLUS,
    get_airflow_dag_run_facet,
    get_airflow_debug_facet,
    get_airflow_job_facet,
    get_airflow_mapped_task_facet,
    get_airflow_run_facet,
    get_dag_documentation,
    get_dag_parent_run_facet,
    get_job_name,
    get_task_documentation,
    get_task_parent_run_facet,
    get_user_provided_run_facets,
    is_operator_disabled,
    is_selective_lineage_enabled,
    print_warning,
)
from airflow.settings import configure_orm
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

if sys.platform == "darwin":
    from setproctitle import getproctitle

    setproctitle = lambda title: logging.getLogger(__name__).debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import getproctitle, setproctitle


def _executor_initializer():
    """
    Initialize processes for the executor used with DAGRun listener's methods (on scheduler).

    This function must be picklable, so it cannot be defined as an inner method or local function.

    Reconfigures the ORM engine to prevent issues that arise when multiple processes interact with
    the Airflow database.
    """
    # This initializer is used only on the scheduler
    # We can configure_orm regardless of the Airflow version, as DB access is always allowed from scheduler.
    settings.configure_orm()


class OpenLineageListener:
    """OpenLineage listener sends events on task instance and dag run starts, completes and failures."""

    def __init__(self):
        self._executor = None
        self.log = logging.getLogger(__name__)
        self.extractor_manager = ExtractorManager()
        self.adapter = OpenLineageAdapter()

    if AIRFLOW_V_3_0_PLUS:

        @hookimpl
        def on_task_instance_running(
            self,
            previous_state: TaskInstanceState,
            task_instance: RuntimeTaskInstance,
        ):
            self.log.debug("OpenLineage listener got notification about task instance start")
            context = task_instance.get_template_context()

            task = context["task"]
            if TYPE_CHECKING:
                assert task
            dagrun = context["dag_run"]
            dag = context["dag"]
            start_date = task_instance.start_date
            self._on_task_instance_running(task_instance, dag, dagrun, task, start_date)
    else:

        @hookimpl
        def on_task_instance_running(  # type: ignore[misc]
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            session: Session,
        ) -> None:
            from airflow.providers.openlineage.utils.utils import is_ti_rescheduled_already

            if not getattr(task_instance, "task", None) is not None:
                self.log.warning(
                    "No task set for TI object task_id: %s - dag_id: %s - run_id %s",
                    task_instance.task_id,
                    task_instance.dag_id,
                    task_instance.run_id,
                )
                return

            self.log.debug("OpenLineage listener got notification about task instance start")
            task = task_instance.task
            if TYPE_CHECKING:
                assert task
            start_date = task_instance.start_date if task_instance.start_date else timezone.utcnow()

            if is_ti_rescheduled_already(task_instance):
                self.log.debug("Skipping this instance of rescheduled task - START event was emitted already")
                return
            self._on_task_instance_running(task_instance, task.dag, task_instance.dag_run, task, start_date)

    def _on_task_instance_running(
        self, task_instance: RuntimeTaskInstance | TaskInstance, dag, dagrun, task, start_date: datetime
    ):
        if is_operator_disabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for operator `%s` "
                "due to its presence in [openlineage] disabled_for_operators.",
                task.task_type,
            )
            return

        if not is_selective_lineage_enabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for task `%s` "
                "due to lack of explicit lineage enablement for task or DAG while "
                "[openlineage] selective_enable is on.",
                task_instance.task_id,
            )
            return

        # Needs to be calculated outside of inner method so that it gets cached for usage in fork processes
        debug_facet = get_airflow_debug_facet()

        @print_warning(self.log)
        def on_running():
            context = task_instance.get_template_context()
            if hasattr(context, "task_reschedule_count") and context["task_reschedule_count"] > 0:
                self.log.debug("Skipping this instance of rescheduled task - START event was emitted already")
                return

            date = dagrun.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dagrun.run_after

            clear_number = 0
            if hasattr(dagrun, "clear_number"):
                clear_number = dagrun.clear_number

            parent_run_id = self.adapter.build_dag_run_id(
                dag_id=task_instance.dag_id,
                logical_date=date,
                clear_number=clear_number,
            )

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                try_number=task_instance.try_number,
                logical_date=date,
                map_index=task_instance.map_index,
            )
            event_type = RunState.RUNNING.value.lower()
            operator_name = task.task_type.lower()

            data_interval_start = dagrun.data_interval_start
            if isinstance(data_interval_start, datetime):
                data_interval_start = data_interval_start.isoformat()
            data_interval_end = dagrun.data_interval_end
            if isinstance(data_interval_end, datetime):
                data_interval_end = data_interval_end.isoformat()

            doc, doc_type = get_task_documentation(task)
            if not doc:
                doc, doc_type = get_dag_documentation(dag)

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun=dagrun, task=task, task_instance_state=TaskInstanceState.RUNNING
                )

            redacted_event = self.adapter.start_task(
                run_id=task_uuid,
                job_name=get_job_name(task_instance),
                job_description=doc,
                job_description_type=doc_type,
                event_time=start_date.isoformat(),
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                # If task owner is default ("airflow"), use DAG owner instead that may have more details
                owners=[x.strip() for x in (task if task.owner != "airflow" else dag).owner.split(",")],
                tags=dag.tags,
                task=task_metadata,
                run_facets={
                    **get_user_provided_run_facets(task_instance, TaskInstanceState.RUNNING),
                    **get_task_parent_run_facet(
                        parent_run_id=parent_run_id,
                        parent_job_name=dag.dag_id,
                        dr_conf=getattr(dagrun, "conf", {}),
                    ),
                    **get_airflow_mapped_task_facet(task_instance),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid),
                    **debug_facet,
                },
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        self._execute(on_running, "on_running", use_fork=True)

    if AIRFLOW_V_3_0_PLUS:

        @hookimpl
        def on_task_instance_success(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance | TaskInstance
        ) -> None:
            self.log.debug("OpenLineage listener got notification about task instance success")

            if isinstance(task_instance, TaskInstance):
                self._on_task_instance_manual_state_change(
                    ti=task_instance,
                    dagrun=task_instance.dag_run,
                    ti_state=TaskInstanceState.SUCCESS,
                )
                return

            context = task_instance.get_template_context()
            task = context["task"]
            if TYPE_CHECKING:
                assert task
            dagrun = context["dag_run"]
            dag = context["dag"]
            self._on_task_instance_success(task_instance, dag, dagrun, task)

    else:

        @hookimpl
        def on_task_instance_success(  # type: ignore[misc]
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            session: Session,
        ) -> None:
            self.log.debug("OpenLineage listener got notification about task instance success")
            task = task_instance.task
            if TYPE_CHECKING:
                assert task
            self._on_task_instance_success(task_instance, task.dag, task_instance.dag_run, task)

    def _on_task_instance_success(self, task_instance: RuntimeTaskInstance, dag, dagrun, task):
        end_date = timezone.utcnow()

        if is_operator_disabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for operator `%s` "
                "due to its presence in [openlineage] disabled_for_operators.",
                task.task_type,
            )
            return

        if not is_selective_lineage_enabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for task `%s` "
                "due to lack of explicit lineage enablement for task or DAG while "
                "[openlineage] selective_enable is on.",
                task_instance.task_id,
            )
            return

        @print_warning(self.log)
        def on_success():
            date = dagrun.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dagrun.run_after

            parent_run_id = self.adapter.build_dag_run_id(
                dag_id=task_instance.dag_id,
                logical_date=date,
                clear_number=dagrun.clear_number,
            )

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                try_number=task_instance.try_number,
                logical_date=date,
                map_index=task_instance.map_index,
            )
            event_type = RunState.COMPLETE.value.lower()
            operator_name = task.task_type.lower()

            data_interval_start = dagrun.data_interval_start
            if isinstance(data_interval_start, datetime):
                data_interval_start = data_interval_start.isoformat()
            data_interval_end = dagrun.data_interval_end
            if isinstance(data_interval_end, datetime):
                data_interval_end = data_interval_end.isoformat()

            doc, doc_type = get_task_documentation(task)
            if not doc:
                doc, doc_type = get_dag_documentation(dag)

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun=dagrun,
                    task=task,
                    task_instance_state=TaskInstanceState.SUCCESS,
                    task_instance=task_instance,
                )

            redacted_event = self.adapter.complete_task(
                run_id=task_uuid,
                job_name=get_job_name(task_instance),
                end_time=end_date.isoformat(),
                task=task_metadata,
                # If task owner is default ("airflow"), use DAG owner instead that may have more details
                owners=[x.strip() for x in (task if task.owner != "airflow" else dag).owner.split(",")],
                tags=dag.tags,
                job_description=doc,
                job_description_type=doc_type,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                run_facets={
                    **get_user_provided_run_facets(task_instance, TaskInstanceState.SUCCESS),
                    **get_task_parent_run_facet(
                        parent_run_id=parent_run_id,
                        parent_job_name=dag.dag_id,
                        dr_conf=getattr(dagrun, "conf", {}),
                    ),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid),
                    **get_airflow_debug_facet(),
                },
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        self._execute(on_success, "on_success", use_fork=True)

    if AIRFLOW_V_3_0_PLUS:

        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState,
            task_instance: RuntimeTaskInstance | TaskInstance,
            error: None | str | BaseException,
        ) -> None:
            self.log.debug("OpenLineage listener got notification about task instance failure")

            if isinstance(task_instance, TaskInstance):
                self._on_task_instance_manual_state_change(
                    ti=task_instance,
                    dagrun=task_instance.dag_run,
                    ti_state=TaskInstanceState.FAILED,
                    error=error,
                )
                return

            context = task_instance.get_template_context()
            task = context["task"]
            if TYPE_CHECKING:
                assert task
            dagrun = context["dag_run"]
            dag = context["dag"]
            self._on_task_instance_failed(task_instance, dag, dagrun, task, error)
    else:

        @hookimpl
        def on_task_instance_failed(  # type: ignore[misc]
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            error: None | str | BaseException,
            session: Session,
        ) -> None:
            self.log.debug("OpenLineage listener got notification about task instance failure")
            task = task_instance.task
            if TYPE_CHECKING:
                assert task
            self._on_task_instance_failed(task_instance, task.dag, task_instance.dag_run, task, error)

    def _on_task_instance_failed(
        self,
        task_instance: TaskInstance | RuntimeTaskInstance,
        dag,
        dagrun,
        task,
        error: None | str | BaseException = None,
    ) -> None:
        end_date = timezone.utcnow()

        if is_operator_disabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for operator `%s` "
                "due to its presence in [openlineage] disabled_for_operators.",
                task.task_type,
            )
            return

        if not is_selective_lineage_enabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for task `%s` "
                "due to lack of explicit lineage enablement for task or DAG while "
                "[openlineage] selective_enable is on.",
                task_instance.task_id,
            )
            return

        @print_warning(self.log)
        def on_failure():
            date = dagrun.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dagrun.run_after

            parent_run_id = self.adapter.build_dag_run_id(
                dag_id=task_instance.dag_id,
                logical_date=date,
                clear_number=dagrun.clear_number,
            )

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                try_number=task_instance.try_number,
                logical_date=date,
                map_index=task_instance.map_index,
            )
            event_type = RunState.FAIL.value.lower()
            operator_name = task.task_type.lower()

            data_interval_start = dagrun.data_interval_start
            if isinstance(data_interval_start, datetime):
                data_interval_start = data_interval_start.isoformat()
            data_interval_end = dagrun.data_interval_end
            if isinstance(data_interval_end, datetime):
                data_interval_end = data_interval_end.isoformat()

            doc, doc_type = get_task_documentation(task)
            if not doc:
                doc, doc_type = get_dag_documentation(dag)

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun=dagrun,
                    task=task,
                    task_instance_state=TaskInstanceState.FAILED,
                    task_instance=task_instance,
                )

            redacted_event = self.adapter.fail_task(
                run_id=task_uuid,
                job_name=get_job_name(task_instance),
                end_time=end_date.isoformat(),
                task=task_metadata,
                error=error,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                tags=dag.tags,
                # If task owner is default ("airflow"), use DAG owner instead that may have more details
                owners=[x.strip() for x in (task if task.owner != "airflow" else dag).owner.split(",")],
                job_description=doc,
                job_description_type=doc_type,
                run_facets={
                    **get_user_provided_run_facets(task_instance, TaskInstanceState.FAILED),
                    **get_task_parent_run_facet(
                        parent_run_id=parent_run_id,
                        parent_job_name=dag.dag_id,
                        dr_conf=getattr(dagrun, "conf", {}),
                    ),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid),
                    **get_airflow_debug_facet(),
                },
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        self._execute(on_failure, "on_failure", use_fork=True)

    def _on_task_instance_manual_state_change(
        self,
        ti: TaskInstance,
        dagrun: DagRun,
        ti_state: TaskInstanceState,
        error: None | str | BaseException = None,
    ) -> None:
        self.log.debug("`_on_task_instance_manual_state_change` was called with state: `%s`.", ti_state)
        end_date = timezone.utcnow()

        @print_warning(self.log)
        def on_state_change():
            date = dagrun.logical_date or dagrun.run_after
            parent_run_id = self.adapter.build_dag_run_id(
                dag_id=ti.dag_id,
                logical_date=date,
                clear_number=dagrun.clear_number,
            )

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                try_number=ti.try_number,
                logical_date=date,
                map_index=ti.map_index,
            )

            adapter_kwargs = {
                "run_id": task_uuid,
                "job_name": get_job_name(ti),
                "end_time": end_date.isoformat(),
                "task": OperatorLineage(),
                "nominal_start_time": None,
                "nominal_end_time": None,
                "tags": None,
                "owners": None,
                "job_description": None,
                "job_description_type": None,
                "run_facets": {
                    **get_task_parent_run_facet(
                        parent_run_id=parent_run_id,
                        parent_job_name=ti.dag_id,
                        dr_conf=getattr(dagrun, "conf", {}),
                    ),
                    **get_airflow_debug_facet(),
                },
            }

            if ti_state == TaskInstanceState.FAILED:
                event_type = RunState.FAIL.value.lower()
                redacted_event = self.adapter.fail_task(**adapter_kwargs, error=error)
            elif ti_state == TaskInstanceState.SUCCESS:
                event_type = RunState.COMPLETE.value.lower()
                redacted_event = self.adapter.complete_task(**adapter_kwargs)
            else:
                raise ValueError(f"Unsupported ti_state: `{ti_state}`.")

            operator_name = ti.operator.lower()
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        self._execute(on_state_change, "on_state_change", use_fork=True)

    def _execute(self, callable, callable_name: str, use_fork: bool = False):
        if use_fork:
            self._fork_execute(callable, callable_name)
        else:
            callable()

    def _terminate_with_wait(self, process: psutil.Process):
        process.terminate()
        try:
            # Waiting for max 3 seconds to make sure process can clean up before being killed.
            process.wait(timeout=3)
        except psutil.TimeoutExpired:
            # If it's not dead by then, then force kill.
            process.kill()

    def _fork_execute(self, callable, callable_name: str):
        self.log.debug("Will fork to execute OpenLineage process.")
        pid = os.fork()
        if pid:
            process = psutil.Process(pid)
            try:
                self.log.debug("Waiting for process %s", pid)
                process.wait(conf.execution_timeout())
            except psutil.TimeoutExpired:
                self.log.warning(
                    "OpenLineage process with pid `%s` expired and will be terminated by listener. "
                    "This has no impact on actual task execution status.",
                    pid,
                )
                self._terminate_with_wait(process)
            except BaseException:
                # Kill the process directly.
                self._terminate_with_wait(process)
            self.log.debug("Process with pid %s finished - parent", pid)
        else:
            setproctitle(getproctitle() + " - OpenLineage - " + callable_name)
            if not AIRFLOW_V_3_0_PLUS:
                configure_orm(disable_connection_pool=True)
            self.log.debug("Executing OpenLineage process - %s - pid %s", callable_name, os.getpid())
            callable()
            self.log.debug("Process with current pid finishes after %s", callable_name)
            os._exit(0)

    @property
    def executor(self) -> ProcessPoolExecutor:
        if not self._executor:
            self._executor = ProcessPoolExecutor(
                max_workers=conf.dag_state_change_process_pool_size(),
                initializer=_executor_initializer,
            )
        return self._executor

    @hookimpl
    def on_starting(self, component) -> None:
        self.log.debug("on_starting: %s", component.__class__.__name__)

    @hookimpl
    def before_stopping(self, component) -> None:
        self.log.debug("before_stopping: %s", component.__class__.__name__)
        with timeout(30):
            self.executor.shutdown(wait=True)

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str) -> None:
        try:
            if dag_run.dag and not is_selective_lineage_enabled(dag_run.dag):
                self.log.debug(
                    "Skipping OpenLineage event emission for DAG `%s` "
                    "due to lack of explicit lineage enablement for DAG while "
                    "[openlineage] selective_enable is on.",
                    dag_run.dag_id,
                )
                return

            if not self.executor:
                self.log.debug("Executor have not started before `on_dag_run_running`")
                return

            data_interval_start = (
                dag_run.data_interval_start.isoformat() if dag_run.data_interval_start else None
            )
            data_interval_end = dag_run.data_interval_end.isoformat() if dag_run.data_interval_end else None

            date = dag_run.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dag_run.run_after

            doc, doc_type = get_dag_documentation(dag_run.dag)

            self.submit_callable(
                self.adapter.dag_started,
                dag_id=dag_run.dag_id,
                logical_date=date,
                start_date=dag_run.start_date,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                clear_number=dag_run.clear_number,
                owners=[x.strip() for x in dag_run.dag.owner.split(",")] if dag_run.dag else None,
                job_description=doc,
                job_description_type=doc_type,
                tags=dag_run.dag.tags if dag_run.dag else [],
                # AirflowJobFacet should be created outside ProcessPoolExecutor that pickles objects,
                # as it causes lack of some TaskGroup attributes and crashes event emission.
                job_facets=get_airflow_job_facet(dag_run=dag_run),
                run_facets={
                    **get_airflow_dag_run_facet(dag_run),
                    **get_dag_parent_run_facet(getattr(dag_run, "conf", {})),
                },
            )
        except BaseException as e:
            self.log.warning("OpenLineage received exception in method on_dag_run_running", exc_info=e)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str) -> None:
        try:
            if dag_run.dag and not is_selective_lineage_enabled(dag_run.dag):
                self.log.debug(
                    "Skipping OpenLineage event emission for DAG `%s` "
                    "due to lack of explicit lineage enablement for DAG while "
                    "[openlineage] selective_enable is on.",
                    dag_run.dag_id,
                )
                return

            if not self.executor:
                self.log.debug("Executor have not started before `on_dag_run_success`")
                return

            task_ids = DagRun._get_partial_task_ids(dag_run.dag)

            date = dag_run.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dag_run.run_after

            data_interval_start = (
                dag_run.data_interval_start.isoformat() if dag_run.data_interval_start else None
            )
            data_interval_end = dag_run.data_interval_end.isoformat() if dag_run.data_interval_end else None
            doc, doc_type = get_dag_documentation(dag_run.dag)

            self.submit_callable(
                self.adapter.dag_success,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                end_date=dag_run.end_date,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                logical_date=date,
                clear_number=dag_run.clear_number,
                owners=[x.strip() for x in dag_run.dag.owner.split(",")] if dag_run.dag else None,
                tags=dag_run.dag.tags if dag_run.dag else [],
                job_description=doc,
                job_description_type=doc_type,
                task_ids=task_ids,
                dag_run_state=dag_run.get_state(),
                run_facets={
                    **get_airflow_dag_run_facet(dag_run),
                    **get_dag_parent_run_facet(getattr(dag_run, "conf", {})),
                },
            )
        except BaseException as e:
            self.log.warning("OpenLineage received exception in method on_dag_run_success", exc_info=e)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str) -> None:
        try:
            if dag_run.dag and not is_selective_lineage_enabled(dag_run.dag):
                self.log.debug(
                    "Skipping OpenLineage event emission for DAG `%s` "
                    "due to lack of explicit lineage enablement for DAG while "
                    "[openlineage] selective_enable is on.",
                    dag_run.dag_id,
                )
                return

            if not self.executor:
                self.log.debug("Executor have not started before `on_dag_run_failed`")
                return

            task_ids = DagRun._get_partial_task_ids(dag_run.dag)

            date = dag_run.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dag_run.run_after

            data_interval_start = (
                dag_run.data_interval_start.isoformat() if dag_run.data_interval_start else None
            )
            data_interval_end = dag_run.data_interval_end.isoformat() if dag_run.data_interval_end else None
            doc, doc_type = get_dag_documentation(dag_run.dag)

            self.submit_callable(
                self.adapter.dag_failed,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                end_date=dag_run.end_date,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                logical_date=date,
                clear_number=dag_run.clear_number,
                owners=[x.strip() for x in dag_run.dag.owner.split(",")] if dag_run.dag else None,
                tags=dag_run.dag.tags if dag_run.dag else [],
                job_description=doc,
                job_description_type=doc_type,
                dag_run_state=dag_run.get_state(),
                task_ids=task_ids,
                msg=msg,
                run_facets={
                    **get_airflow_dag_run_facet(dag_run),
                    **get_dag_parent_run_facet(getattr(dag_run, "conf", {})),
                },
            )
        except BaseException as e:
            self.log.warning("OpenLineage received exception in method on_dag_run_failed", exc_info=e)

    def submit_callable(self, callable, *args, **kwargs):
        fut = self.executor.submit(callable, *args, **kwargs)
        fut.add_done_callback(self.log_submit_error)
        return fut

    def log_submit_error(self, fut):
        if fut.exception():
            self.log.warning("Failed to submit method to executor", exc_info=fut.exception())
        else:
            self.log.debug("Successfully submitted method to executor")


@cache
def get_openlineage_listener() -> OpenLineageListener:
    """Get singleton listener manager."""
    return OpenLineageListener()
