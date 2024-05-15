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
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import TYPE_CHECKING

from openlineage.client.serde import Serde

from airflow import __version__ as airflow_version
from airflow.listeners import hookimpl
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors import ExtractorManager
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter, RunState
from airflow.providers.openlineage.utils.utils import (
    get_airflow_run_facet,
    get_custom_facets,
    get_job_name,
    is_operator_disabled,
    is_selective_lineage_enabled,
    print_warning,
)
from airflow.stats import Stats
from airflow.utils.timeout import timeout

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import DagRun, TaskInstance

_openlineage_listener: OpenLineageListener | None = None


def _get_try_number_success(val):
    # todo: remove when min airflow version >= 2.10.0
    from packaging.version import parse

    if parse(parse(airflow_version).base_version) < parse("2.10.0"):
        return val.try_number - 1
    else:
        return val.try_number


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
        previous_state,
        task_instance: TaskInstance,
        session: Session,  # This will always be QUEUED
    ):
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

        @print_warning(self.log)
        def on_running():
            # that's a workaround to detect task running from deferred state
            # we return here because Airflow 2.3 needs task from deferred state
            if task_instance.next_method is not None:
                return
            parent_run_id = self.adapter.build_dag_run_id(dag.dag_id, dagrun.run_id)

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                execution_date=task_instance.execution_date,
                try_number=task_instance.try_number,
            )
            event_type = RunState.RUNNING.value.lower()
            operator_name = task.task_type.lower()

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(dagrun, task)

            start_date = task_instance.start_date if task_instance.start_date else datetime.now()
            data_interval_start = (
                dagrun.data_interval_start.isoformat() if dagrun.data_interval_start else None
            )
            data_interval_end = dagrun.data_interval_end.isoformat() if dagrun.data_interval_end else None

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
                    **get_custom_facets(task_instance),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid),
                },
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        on_running()

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance: TaskInstance, session):
        self.log.debug("OpenLineage listener got notification about task instance success")

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
            parent_run_id = OpenLineageAdapter.build_dag_run_id(dag.dag_id, dagrun.run_id)

            task_uuid = OpenLineageAdapter.build_task_instance_run_id(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                execution_date=task_instance.execution_date,
                try_number=_get_try_number_success(task_instance),
            )
            event_type = RunState.COMPLETE.value.lower()
            operator_name = task.task_type.lower()

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun, task, complete=True, task_instance=task_instance
                )

            end_date = task_instance.end_date if task_instance.end_date else datetime.now()

            redacted_event = self.adapter.complete_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                parent_job_name=dag.dag_id,
                parent_run_id=parent_run_id,
                end_time=end_date.isoformat(),
                task=task_metadata,
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        on_success()

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance: TaskInstance, session):
        self.log.debug("OpenLineage listener got notification about task instance failure")

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
            parent_run_id = OpenLineageAdapter.build_dag_run_id(dag.dag_id, dagrun.run_id)

            task_uuid = OpenLineageAdapter.build_task_instance_run_id(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                execution_date=task_instance.execution_date,
                try_number=task_instance.try_number,
            )
            event_type = RunState.FAIL.value.lower()
            operator_name = task.task_type.lower()

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun, task, complete=True, task_instance=task_instance
                )

            end_date = task_instance.end_date if task_instance.end_date else datetime.now()

            redacted_event = self.adapter.fail_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                parent_job_name=dag.dag_id,
                parent_run_id=parent_run_id,
                end_time=end_date.isoformat(),
                task=task_metadata,
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        on_failure()

    @property
    def executor(self):
        if not self._executor:
            self._executor = ProcessPoolExecutor(max_workers=conf.dag_state_change_process_pool_size())
        return self._executor

    @hookimpl
    def on_starting(self, component):
        self.log.debug("on_starting: %s", component.__class__.__name__)

    @hookimpl
    def before_stopping(self, component):
        self.log.debug("before_stopping: %s", component.__class__.__name__)
        with timeout(30):
            self.executor.shutdown(wait=True)

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
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

        data_interval_start = dag_run.data_interval_start.isoformat() if dag_run.data_interval_start else None
        data_interval_end = dag_run.data_interval_end.isoformat() if dag_run.data_interval_end else None
        self.executor.submit(
            self.adapter.dag_started,
            dag_run=dag_run,
            msg=msg,
            nominal_start_time=data_interval_start,
            nominal_end_time=data_interval_end,
        )

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
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

        self.executor.submit(self.adapter.dag_success, dag_run=dag_run, msg=msg)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
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

        self.executor.submit(self.adapter.dag_failed, dag_run=dag_run, msg=msg)


def get_openlineage_listener() -> OpenLineageListener:
    """Get singleton listener manager."""
    global _openlineage_listener
    if not _openlineage_listener:
        _openlineage_listener = OpenLineageListener()
    return _openlineage_listener
