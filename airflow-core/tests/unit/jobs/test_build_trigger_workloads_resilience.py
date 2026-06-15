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
"""Error-isolation tests for ``TriggerRunnerSupervisor.build_trigger_workloads``.

``build_trigger_workloads`` runs inside the supervisor's synchronous ``run_once``
loop (``load_triggers`` -> ``update_triggers`` -> ``build_trigger_workloads``). An
unhandled exception there propagates out of ``run()`` and kills the entire
triggerer process, taking every other deferred task down with it. A single bad
trigger -- e.g. one whose deadline-callback context fetch raises, whose serialized
Dag fails to load, or whose log-path rendering throws -- must therefore be isolated
so the rest of the batch still launches and the triggerer stays up. This mirrors the
per-item SAVEPOINT isolation already applied to the scheduler's ``handle_miss`` and
``_enqueue_executor_callbacks`` loops.
"""

from __future__ import annotations

import datetime
import selectors
import socket as socket_mod

import pendulum
import psutil
import pytest
from structlog.typing import FilteringBoundLogger

from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggerRunnerSupervisor
from airflow.models import DagModel, DagRun, Trigger
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, BaseOperator
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.taskinstance import create_task_instance

pytestmark = pytest.mark.db_test


class TestBuildTriggerWorkloadsResilience:
    @pytest.fixture(autouse=True)
    def _clean(self):
        clear_db_runs()
        clear_db_dags()
        yield
        clear_db_runs()
        clear_db_dags()

    def _make_task_trigger(self, session, idx):
        bundle_name = "testing"
        session.merge(DagBundleModel(name=bundle_name))
        session.flush()
        date = pendulum.datetime(2023, 1, 1)
        dag_id = f"build_wl_dag_{idx}"
        dag_model = DagModel(dag_id=dag_id, bundle_name=bundle_name)
        dag = DAG(dag_id=dag_id, schedule="@daily", start_date=date)
        run = DagRun(
            dag_id=dag_id,
            run_id=f"run_{idx}",
            logical_date=date,
            data_interval=(date, date),
            run_after=date,
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
        )
        trigger_orm = Trigger.from_object(TimeDeltaTrigger(datetime.timedelta(days=7)))
        operator = BaseOperator(task_id="t", dag=dag)
        session.add(dag_model)
        SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name=bundle_name)
        session.add(run)
        session.add(trigger_orm)
        session.flush()
        dag_version = DagVersion.get_latest_version(dag_id)
        ti = create_task_instance(operator, run_id=run.run_id, dag_version_id=dag_version.id)
        ti.trigger_id = trigger_orm.id
        session.add(ti)
        session.commit()
        return trigger_orm.id

    def _make_supervisor(self, job, mocker):
        process = mocker.Mock(spec=psutil.Process, pid=10 * job.id + 1)
        mock_stdin = mocker.Mock(spec=socket_mod.socket)
        proc = TriggerRunnerSupervisor(
            process_log=mocker.Mock(spec=FilteringBoundLogger),
            id=job.id,
            job=job,
            pid=process.pid,
            stdin=mock_stdin,
            process=process,
            capacity=10,
        )
        mock_selector = mocker.Mock(spec=selectors.DefaultSelector)
        mock_selector.select.return_value = []
        proc.selector = mock_selector
        return proc

    def test_one_failing_trigger_does_not_abort_batch(self, session, mocker):
        """A single trigger whose workload build raises must be skipped, the rest built,
        and no exception must propagate out (which would crash the triggerer)."""
        job = Job()
        session.add(job)
        session.flush()

        ids = [self._make_task_trigger(session, i) for i in range(3)]
        bad_id = ids[1]
        supervisor = self._make_supervisor(job, mocker)

        real_create = TriggerRunnerSupervisor._create_workload

        def flaky_create(self, trigger, dag_bag, render_log_fname, session):
            if trigger.id == bad_id:
                raise RuntimeError("workload build boom on one trigger")
            return real_create(self, trigger, dag_bag, render_log_fname, session=session)

        mocker.patch.object(TriggerRunnerSupervisor, "_create_workload", flaky_create)

        # Must NOT raise: a single bad trigger is isolated like handle_miss / enqueue callbacks.
        result = supervisor.build_trigger_workloads(set(ids))

        built_ids = sorted(w.id for w in result)
        # The two good triggers must still be built despite the bad one in the batch.
        assert built_ids == sorted(i for i in ids if i != bad_id)
        # The bad trigger must NOT leave a leaked logger factory behind.
        assert bad_id not in supervisor.logger_cache

    def test_all_triggers_build_when_none_fail(self, session, mocker):
        """Sanity: with no failures every trigger in the batch is built."""
        job = Job()
        session.add(job)
        session.flush()

        ids = [self._make_task_trigger(session, i) for i in range(3)]
        supervisor = self._make_supervisor(job, mocker)

        result = supervisor.build_trigger_workloads(set(ids))

        assert sorted(w.id for w in result) == sorted(ids)
