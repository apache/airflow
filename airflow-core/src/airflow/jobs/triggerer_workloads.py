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
"""
Shared builder that turns ``Trigger`` rows into ``RunTrigger`` workloads.

This logic is used in two places that must produce identical workloads:

* :class:`~airflow.jobs.triggerer_job_runner.TriggerRunnerSupervisor`, which builds
  workloads in-process from its own metadata-DB session.
* The Execution API ``/triggers/workloads`` endpoint (AIP-92), which builds the
  same workloads server-side so a DB-free triggerer can fetch them over HTTP.

Keeping a single implementation here avoids the two paths drifting apart.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.executors import workloads
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.models.trigger import Trigger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag

log = logging.getLogger(__name__)


def build_run_trigger_workload(
    trigger: Trigger,
    *,
    dag_bag: DBDagBag,
    render_log_fname: Callable[..., str],
    session: Session,
    on_ti_logging: Callable[[int, TaskInstanceDTO, str], None] | None = None,
) -> workloads.RunTrigger | None:
    """
    Build a single :class:`~airflow.executors.workloads.RunTrigger` for ``trigger``.

    :param trigger: The trigger ORM row to build a workload for.
    :param dag_bag: DagBag used to load the serialized DAG for task-backed triggers.
    :param render_log_fname: Callable returning the trigger log path for a task instance.
    :param session: The database session.
    :param on_ti_logging: Optional callback invoked for task-backed triggers with
        ``(trigger_id, ser_ti, log_path)``. The supervisor uses this to register a
        per-trigger logging factory; the Execution API endpoint omits it because it only
        needs the serializable workload. Kept as a callback so the workload-building logic
        stays in one place while the supervisor's ``self``-bound side effect lives there.
    :return: The built workload, or ``None`` if the trigger should be skipped.
    """
    if trigger.task_instance is None:
        return workloads.RunTrigger(
            id=trigger.id,
            classpath=trigger.classpath,
            encrypted_kwargs=trigger.encrypted_kwargs,
        )

    if not trigger.task_instance.dag_version_id:
        # This is to handle 2 to 3 upgrade where TI.dag_version_id can be none
        log.warning(
            "TaskInstance %s associated with Trigger has no associated Dag Version, skipping the trigger",
            trigger.task_instance.id,
        )
        return None

    log_path = render_log_fname(ti=trigger.task_instance)
    ser_ti = TaskInstanceDTO.model_validate(trigger.task_instance, from_attributes=True)

    if on_ti_logging is not None:
        on_ti_logging(trigger.id, ser_ti, log_path)

    serialized_dag_model = dag_bag.get_serialized_dag_model(
        version_id=trigger.task_instance.dag_version_id,
        session=session,
    )

    if serialized_dag_model:
        task = serialized_dag_model.dag.get_task(trigger.task_instance.task_id)

        # When a TaskInstance of a Trigger contains a task with start_from_trigger enabled,
        # it means we need to load the SerializedDagModel so we can build a RuntimeTaskInstance later on
        # which will allow us to build a context on which we will render the templated fields.
        if task.start_from_trigger:
            log.info("Start from trigger enabled for task %s", task.task_id)
            dag_run = trigger.task_instance.get_dagrun(session=session)

            return workloads.RunTrigger(
                id=trigger.id,
                classpath=trigger.classpath,
                encrypted_kwargs=trigger.encrypted_kwargs,
                ti=ser_ti,
                timeout_after=trigger.task_instance.trigger_timeout,
                dag_data=serialized_dag_model.data,
                dag_run_data=dag_run.dag_run_data.model_dump(exclude_unset=True),
            )
    return workloads.RunTrigger(
        id=trigger.id,
        classpath=trigger.classpath,
        encrypted_kwargs=trigger.encrypted_kwargs,
        ti=ser_ti,
        timeout_after=trigger.task_instance.trigger_timeout,
    )


def build_run_trigger_workloads(
    trigger_ids: set[int],
    *,
    dag_bag: DBDagBag,
    render_log_fname: Callable[..., str],
    session: Session,
    on_ti_logging: Callable[[int, TaskInstanceDTO, str], None] | None = None,
    bulk_fetch: Callable[..., dict[int, Trigger]] | None = None,
    fetch_non_task_ids: Callable[..., set[int]] | None = None,
) -> list[workloads.RunTrigger]:
    """
    Build :class:`~airflow.executors.workloads.RunTrigger` workloads for ``trigger_ids``.

    Triggers that vanished, or that are no longer associated with a task, asset, or callback
    (e.g. resolved by another triggerer in an HA setup), are skipped.

    :param trigger_ids: The trigger IDs to build workloads for.
    :param dag_bag: DagBag used to load serialized DAGs for task-backed triggers.
    :param render_log_fname: Callable returning the trigger log path for a task instance.
    :param session: The database session.
    :param on_ti_logging: Optional per-trigger callback, see :func:`build_run_trigger_workload`.
    :param bulk_fetch: Override for fetching trigger rows; defaults to ``Trigger.bulk_fetch``. The
        supervisor injects its own hook so it can instrument/override the fetch.
    :param fetch_non_task_ids: Override for fetching non-task trigger IDs; defaults to
        ``Trigger.fetch_trigger_ids_with_non_task_associations``.
    """
    if bulk_fetch is None:
        bulk_fetch = Trigger.bulk_fetch
    if fetch_non_task_ids is None:
        fetch_non_task_ids = Trigger.fetch_trigger_ids_with_non_task_associations

    new_triggers = bulk_fetch(trigger_ids, session=session)
    trigger_ids_with_non_task_associations = fetch_non_task_ids(session=session)

    to_create: list[workloads.RunTrigger] = []
    for trigger_id in trigger_ids:
        # Check it didn't vanish in the meantime
        if trigger_id not in new_triggers:
            log.warning("Trigger %s disappeared before we could start it", trigger_id)
            continue

        new_trigger_orm = new_triggers[trigger_id]

        # If the trigger is not associated to a task, an asset, or a callback, this means the TaskInstance
        # row was updated by either Trigger.submit_event or Trigger.submit_failure and can happen when a
        # single trigger Job is being run on multiple TriggerRunners in a High-Availability setup.
        if new_trigger_orm.task_instance is None and trigger_id not in trigger_ids_with_non_task_associations:
            log.info(
                "TaskInstance of Trigger %s is None. It was likely updated by another trigger job. "
                "Skipping trigger instantiation.",
                trigger_id,
            )
            continue

        if workload := build_run_trigger_workload(
            new_trigger_orm,
            dag_bag=dag_bag,
            render_log_fname=render_log_fname,
            session=session,
            on_ti_logging=on_ti_logging,
        ):
            to_create.append(workload)

    return to_create
