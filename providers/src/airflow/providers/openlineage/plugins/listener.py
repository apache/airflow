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
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

import psutil
from openlineage.client.serde import Serde
from setproctitle import getproctitle, setproctitle

from airflow import settings
from airflow.listeners import hookimpl
from airflow.models import DagRun
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors import ExtractorManager
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter, RunState
from airflow.providers.openlineage.utils.utils import (
    IS_AIRFLOW_2_10_OR_HIGHER,
    get_airflow_dag_run_facet,
    get_airflow_debug_facet,
    get_airflow_job_facet,
    get_airflow_mapped_task_facet,
    get_airflow_run_facet,
    get_job_name,
    get_user_provided_run_facets,
    is_operator_disabled,
    is_selective_lineage_enabled,
    print_warning,
)
from airflow.settings import configure_orm
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState
from airflow.utils.timeout import timeout

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import TaskInstance

_openlineage_listener: OpenLineageListener | None = None


def _get_try_number_success(val):
    # todo: remove when min airflow version >= 2.10.0
    if IS_AIRFLOW_2_10_OR_HIGHER:
        return val.try_number
    return val.try_number - 1


def _executor_initializer():
    """
    Initialize worker processes for the executor used for DagRun listener.

    This function must be picklable, so it cannot be defined as an inner method or local function.

    Reconfigures the ORM engine to prevent issues that arise when multiple processes interact with
    the Airflow database.
    """
    settings.configure_orm()


class OpenLineageListener:
    """OpenLineage listener sends events on task instance and dag run starts, completes and failures."""

    def __init__(self):
        self._executor = None
        self.log = logging.getLogger(__name__)
        self.extractor_manager = ExtractorManager()
        self.adapter = OpenLineageAdapter()

    @hookimpl
    def on_task_instance_running(
        self,
        previous_state: TaskInstanceState,
        task_instance: TaskInstance,
        session: Session,  # This will always be QUEUED
    ) -> None:
        if not getattr(task_instance, "task", None) is not None:
            self.log.warning(
                "No task set for TI object task_id: %s - dag_id: %s - run_id %s",
                task_instance.task_id,
                task_instance.dag_id,
                task_instance.run_id,
            )
            return

        self.log.debug("OpenLineage listener got notification about task instance start")
        dagrun = task_instance.dag_run
        task = task_instance.task
        if TYPE_CHECKING:
            assert task
        dag = task.dag
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
                task.task_id,
            )
            return

        # Needs to be calculated outside of inner method so that it gets cached for usage in fork processes
        debug_facet = get_airflow_debug_facet()

        @print_warning(self.log)
        def on_running():
            # that's a workaround to detect task running from deferred state
            # we return here because Airflow 2.3 needs task from deferred state
            if task_instance.next_method is not None:
                return
            parent_run_id = self.adapter.build_dag_run_id(
                dag_id=dag.dag_id,
                logical_date=dagrun.logical_date,
            )

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                try_number=task_instance.try_number,
                execution_date=task_instance.execution_date,
            )
            event_type = RunState.RUNNING.value.lower()
            operator_name = task.task_type.lower()

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(dagrun, task)

            start_date = (
                task_instance.start_date
                if task_instance.start_date
                else timezone.utcnow()
            )
            data_interval_start = (
                dagrun.data_interval_start.isoformat()
                if dagrun.data_interval_start
                else None
            )
            data_interval_end = (
                dagrun.data_interval_end.isoformat() if dagrun.data_interval_end else None
            )
            redacted_event = self.adapter.start_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                job_description=dag.description,
                event_time=start_date.isoformat(),
                parent_job_name=dag.dag_id,
                parent_run_id=parent_run_id,
                code_location=None,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                owners=dag.owner.split(", "),
                task=task_metadata,
                run_facets={
                    **get_user_provided_run_facets(
                        task_instance, TaskInstanceState.RUNNING
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

    @hookimpl
    def on_task_instance_success(
        self,
        previous_state: TaskInstanceState,
        task_instance: TaskInstance,
        session: Session,
    ) -> None:
        self.log.debug(
            "OpenLineage listener got notification about task instance success"
        )

        dagrun = task_instance.dag_run
        task = task_instance.task
        if TYPE_CHECKING:
            assert task
        dag = task.dag

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
                task.task_id,
            )
            return

        @print_warning(self.log)
        def on_success():
            parent_run_id = OpenLineageAdapter.build_dag_run_id(
                dag_id=dag.dag_id,
                logical_date=dagrun.logical_date,
            )

            task_uuid = OpenLineageAdapter.build_task_instance_run_id(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                try_number=_get_try_number_success(task_instance),
                execution_date=task_instance.execution_date,
            )
            event_type = RunState.COMPLETE.value.lower()
            operator_name = task.task_type.lower()

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun, task, complete=True, task_instance=task_instance
                )

            end_date = (
                task_instance.end_date if task_instance.end_date else timezone.utcnow()
            )

            redacted_event = self.adapter.complete_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                parent_job_name=dag.dag_id,
                parent_run_id=parent_run_id,
                end_time=end_date.isoformat(),
                task=task_metadata,
                run_facets={
                    **get_user_provided_run_facets(
                        task_instance, TaskInstanceState.SUCCESS
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

    if IS_AIRFLOW_2_10_OR_HIGHER:

        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            error: None | str | BaseException,
            session: Session,
        ) -> None:
            self._on_task_instance_failed(
                previous_state=previous_state,
                task_instance=task_instance,
                error=error,
                session=session,
            )

    else:

        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            session: Session,
        ) -> None:
            self._on_task_instance_failed(
                previous_state=previous_state,
                task_instance=task_instance,
                error=None,
                session=session,
            )

    def _on_task_instance_failed(
        self,
        previous_state: TaskInstanceState,
        task_instance: TaskInstance,
        session: Session,
        error: None | str | BaseException = None,
    ) -> None:
        self.log.debug(
            "OpenLineage listener got notification about task instance failure"
        )

        dagrun = task_instance.dag_run
        task = task_instance.task
        if TYPE_CHECKING:
            assert task
        dag = task.dag

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
                task.task_id,
            )
            return

        @print_warning(self.log)
        def on_failure():
            parent_run_id = OpenLineageAdapter.build_dag_run_id(
                dag_id=dag.dag_id,
                logical_date=dagrun.logical_date,
            )

            task_uuid = OpenLineageAdapter.build_task_instance_run_id(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                try_number=task_instance.try_number,
                execution_date=task_instance.execution_date,
            )
            event_type = RunState.FAIL.value.lower()
            operator_name = task.task_type.lower()

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun, task, complete=True, task_instance=task_instance
                )

            end_date = (
                task_instance.end_date if task_instance.end_date else timezone.utcnow()
            )

            redacted_event = self.adapter.fail_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                parent_job_name=dag.dag_id,
                parent_run_id=parent_run_id,
                end_time=end_date.isoformat(),
                task=task_metadata,
                error=error,
                run_facets={
                    **get_user_provided_run_facets(
                        task_instance, TaskInstanceState.FAILED
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
                    "OpenLineage process %s expired. This should not affect process execution.",
                    pid,
                )
                self._terminate_with_wait(process)
            except BaseException:
                # Kill the process directly.
                self._terminate_with_wait(process)
            self.log.debug("Process with pid %s finished - parent", pid)
        else:
            setproctitle(getproctitle() + " - OpenLineage - " + callable_name)
            configure_orm(disable_connection_pool=True)
            self.log.debug(
                "Executing OpenLineage process - %s - pid %s", callable_name, os.getpid()
            )
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
                dag_run.data_interval_start.isoformat()
                if dag_run.data_interval_start
                else None
            )
            data_interval_end = (
                dag_run.data_interval_end.isoformat()
                if dag_run.data_interval_end
                else None
            )

            run_facets = {**get_airflow_dag_run_facet(dag_run)}

            self.submit_callable(
                self.adapter.dag_started,
                dag_id=dag_run.dag_id,
                logical_date=dag_run.logical_date,
                start_date=dag_run.start_date,
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                run_facets=run_facets,
                owners=[x.strip() for x in dag_run.dag.owner.split(",")]
                if dag_run.dag
                else None,
                description=dag_run.dag.description if dag_run.dag else None,
                # AirflowJobFacet should be created outside ProcessPoolExecutor that pickles objects,
                # as it causes lack of some TaskGroup attributes and crashes event emission.
                job_facets=get_airflow_job_facet(dag_run=dag_run),
            )
        except BaseException as e:
            self.log.warning(
                "OpenLineage received exception in method on_dag_run_running", exc_info=e
            )

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

            if IS_AIRFLOW_2_10_OR_HIGHER:
                task_ids = DagRun._get_partial_task_ids(dag_run.dag)
            else:
                task_ids = (
                    dag_run.dag.task_ids if dag_run.dag and dag_run.dag.partial else None
                )
            self.submit_callable(
                self.adapter.dag_success,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                end_date=dag_run.end_date,
                logical_date=dag_run.logical_date,
                task_ids=task_ids,
                dag_run_state=dag_run.get_state(),
            )
        except BaseException as e:
            self.log.warning(
                "OpenLineage received exception in method on_dag_run_success", exc_info=e
            )

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

            if IS_AIRFLOW_2_10_OR_HIGHER:
                task_ids = DagRun._get_partial_task_ids(dag_run.dag)
            else:
                task_ids = (
                    dag_run.dag.task_ids if dag_run.dag and dag_run.dag.partial else None
                )
            self.submit_callable(
                self.adapter.dag_failed,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                end_date=dag_run.end_date,
                logical_date=dag_run.logical_date,
                dag_run_state=dag_run.get_state(),
                task_ids=task_ids,
                msg=msg,
            )
        except BaseException as e:
            self.log.warning(
                "OpenLineage received exception in method on_dag_run_failed", exc_info=e
            )

    def submit_callable(self, callable, *args, **kwargs):
        fut = self.executor.submit(callable, *args, **kwargs)
        fut.add_done_callback(self.log_submit_error)
        return fut

    def log_submit_error(self, fut):
        if fut.exception():
            self.log.warning(
                "Failed to submit method to executor", exc_info=fut.exception()
            )
        else:
            self.log.debug("Successfully submitted method to executor")


def get_openlineage_listener() -> OpenLineageListener:
    """Get singleton listener manager."""
    global _openlineage_listener
    if not _openlineage_listener:
        _openlineage_listener = OpenLineageListener()
    return _openlineage_listener
