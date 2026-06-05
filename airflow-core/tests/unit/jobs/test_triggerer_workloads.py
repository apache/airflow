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
Direct tests for the shared ``RunTrigger`` workload builder.

This logic used to be exercised through the (now DB-free) triggerer supervisor; it now lives only
in :mod:`airflow.jobs.triggerer_workloads` and is consumed server-side by the Execution API
``/triggers/workloads`` endpoint. These tests cover the build edge cases directly: the happy
task-backed path with its ``on_ti_logging`` callback, ``start_from_trigger`` workloads carrying the
serialized DAG, the 2->3-upgrade ``dag_version_id is None`` skip, non-task (asset/callback)
triggers, the #38599 vanished-task-instance skip, and the fetch-hook overrides.
"""

from __future__ import annotations

import datetime

import pendulum
import pytest

from airflow.jobs.triggerer_workloads import build_run_trigger_workload, build_run_trigger_workloads
from airflow.models import DagModel, DagRun, Trigger
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, BaseOperator
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.helpers import log_filename_template_renderer
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_triggers,
)
from tests_common.test_utils.taskinstance import create_task_instance

pytestmark = pytest.mark.db_test

BUNDLE_NAME = "testing"


@pytest.fixture(autouse=True)
def _clean():
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_triggers()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_triggers()


def _create_task_trigger_in_db(session, trigger, *, operator=None):
    """Create a Trigger linked to a task instance with a serialized DAG version."""
    session.merge(DagBundleModel(name=BUNDLE_NAME))
    session.flush()

    date = pendulum.datetime(2023, 1, 1)
    dag_model = DagModel(dag_id="test_dag", bundle_name=BUNDLE_NAME)
    dag = DAG(dag_id=dag_model.dag_id, schedule="@daily", start_date=date)
    run = DagRun(
        dag_id=dag_model.dag_id,
        run_id="test_run",
        logical_date=date,
        data_interval=(date, date),
        run_after=date,
        run_type=DagRunType.MANUAL,
    )
    trigger_orm = Trigger.from_object(trigger)
    if operator:
        operator.dag = dag
    else:
        operator = BaseOperator(task_id="test_ti", dag=dag)
    session.add(dag_model)
    SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name=BUNDLE_NAME)
    session.add(run)
    session.add(trigger_orm)
    session.flush()
    dag_version = DagVersion.get_latest_version(dag.dag_id)
    task_instance = create_task_instance(operator, run_id=run.run_id, dag_version_id=dag_version.id)
    task_instance.trigger_id = trigger_orm.id
    session.add(task_instance)
    session.commit()
    return trigger_orm, task_instance


def test_build_workload_for_task_trigger_invokes_on_ti_logging(session, testing_dag_bundle):
    """A task-backed trigger yields a workload carrying its TI and fires the on_ti_logging callback."""
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm, ti = _create_task_trigger_in_db(session, trigger)

    logged: list[tuple] = []
    workloads = build_run_trigger_workloads(
        {trigger_orm.id},
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
        on_ti_logging=lambda trigger_id, ser_ti, log_path: logged.append((trigger_id, log_path)),
    )

    assert len(workloads) == 1
    workload = workloads[0]
    assert workload.id == trigger_orm.id
    assert workload.ti is not None
    assert workload.ti.task_id == ti.task_id
    # The callback fires once for the task-backed trigger so the supervisor can register a logger.
    assert len(logged) == 1
    assert logged[0][0] == trigger_orm.id


class _StartFromTriggerOperator(BaseOperator):
    """An operator that begins execution directly in the triggerer (``start_from_trigger``)."""

    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )
    start_from_trigger = True

    def execute_complete(self, *args, **kwargs):
        pass


def test_build_workload_start_from_trigger_carries_serialized_dag(session, testing_dag_bundle):
    """A start_from_trigger task carries the serialized DAG + dag-run data so a context can be built."""
    operator = _StartFromTriggerOperator(task_id="sft_task")
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm, _ = _create_task_trigger_in_db(session, trigger, operator=operator)

    workload = build_run_trigger_workload(
        session.get(Trigger, trigger_orm.id),
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
    )

    assert workload is not None
    assert workload.dag_data is not None
    assert workload.dag_run_data is not None


def test_build_workload_skips_ti_without_dag_version(session, testing_dag_bundle):
    """A task instance with no dag_version_id (2->3 upgrade) is skipped, not built."""
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm, ti = _create_task_trigger_in_db(session, trigger)
    ti.dag_version_id = None
    session.merge(ti)
    session.commit()

    workloads = build_run_trigger_workloads(
        {trigger_orm.id},
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
    )

    assert workloads == []


def test_build_workload_for_non_task_trigger(session):
    """A trigger with no task instance but a non-task association builds a TI-less workload."""
    trigger_orm = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    session.add(trigger_orm)
    session.commit()

    # The trigger has no task instance; include it via the non-task association hook.
    workloads = build_run_trigger_workloads(
        {trigger_orm.id},
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
        fetch_non_task_ids=lambda *, session: {trigger_orm.id},
    )

    assert len(workloads) == 1
    assert workloads[0].id == trigger_orm.id
    assert workloads[0].ti is None


def test_build_workload_skips_vanished_task_instance_38599(session, testing_dag_bundle):
    """
    A trigger whose task instance was unlinked by another triggerer (#38599 race) is skipped.

    When ``Trigger.submit_event``/``submit_failure`` clears the TI's trigger_id in an HA setup,
    the trigger is left with no task instance and no non-task association, so the builder must skip
    it rather than crash dereferencing the missing TI.
    """
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm, ti = _create_task_trigger_in_db(session, trigger)
    # Simulate the other triggerer unlinking the task instance.
    ti.trigger_id = None
    session.merge(ti)
    session.commit()

    workloads = build_run_trigger_workloads(
        {trigger_orm.id},
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
    )

    assert workloads == []


def test_build_workloads_uses_fetch_hooks(session, testing_dag_bundle):
    """The bulk_fetch / fetch_non_task_ids hooks are honored (used by callers to instrument fetches)."""
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm, _ = _create_task_trigger_in_db(session, trigger)

    bulk_fetch_calls: list = []
    non_task_calls: list = []

    def bulk_fetch(ids, *, session):
        bulk_fetch_calls.append(set(ids))
        return Trigger.bulk_fetch(ids, session=session)

    def fetch_non_task_ids(*, session):
        non_task_calls.append(True)
        return Trigger.fetch_trigger_ids_with_non_task_associations(session=session)

    workloads = build_run_trigger_workloads(
        {trigger_orm.id},
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
        bulk_fetch=bulk_fetch,
        fetch_non_task_ids=fetch_non_task_ids,
    )

    assert bulk_fetch_calls == [{trigger_orm.id}]
    assert non_task_calls == [True]
    assert len(workloads) == 1
    assert workloads[0].id == trigger_orm.id


def test_build_workloads_skips_vanished_trigger(session):
    """A trigger id that no longer exists in the DB is skipped."""
    workloads = build_run_trigger_workloads(
        {999999},
        dag_bag=DBDagBag(),
        render_log_fname=log_filename_template_renderer(),
        session=session,
    )
    assert workloads == []
