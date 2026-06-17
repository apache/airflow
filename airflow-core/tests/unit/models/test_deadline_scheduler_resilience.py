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
Resilience tests for the scheduler deadline-detection loop.

Covers:
* A DeadlineReference evaluated against a DagRun missing the referenced field.
* Error isolation in the scheduler loop: one deadline whose ``handle_miss``
  raises must not crash the scheduler nor block the other overdue deadlines.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone

import pytest

from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test]


async def deadline_callback():
    """Awaitable used as the callback target."""


CB_PATH = f"{__name__}.{deadline_callback.__name__}"


def _clean():
    # Order matters: Deadline -> Callback (Callback.trigger_id FK -> trigger) -> Trigger.
    db.clear_db_deadline()
    db.clear_db_deadline_alert()
    db.clear_db_callbacks()
    db.clear_db_triggers()
    db.clear_db_runs()
    db.clear_db_dags()


@pytest.fixture(autouse=True)
def _cleanup():
    _clean()
    yield
    _clean()


# ---------------------------------------------------------------------------
# Helpers for scheduler-level scenarios.
# ---------------------------------------------------------------------------


def _make_dagrun(dag_maker, session, dag_id, logical_date, run_id):
    with dag_maker(dag_id):
        EmptyOperator(task_id="t")
    dr = dag_maker.create_dagrun(state=DagRunState.RUNNING, logical_date=logical_date, run_id=run_id)
    session.flush()
    return dr


def _make_deadline(session, dagrun, deadline_time, callback=None):
    dl = Deadline(
        deadline_time=deadline_time,
        callback=callback or AsyncCallback(CB_PATH),
        dagrun_id=dagrun.id,
        deadline_alert_id=None,
    )
    session.add(dl)
    session.flush()
    return dl


# ---------------------------------------------------------------------------
# Scenario 7: reference computed against DagRun missing the referenced field.
# logical_date is None -> 0 deadlines, no crash.
# ---------------------------------------------------------------------------


def test_logical_date_none_yields_none_not_crash(dag_maker, session):
    from airflow.serialization.definitions.deadline import SerializedReferenceModels

    dr = _make_dagrun(dag_maker, session, "dl_ld_none", DEFAULT_DATE, "run_x")
    # Force logical_date to None on the row.
    dr.logical_date = None
    session.add(dr)
    session.commit()

    ref = SerializedReferenceModels.DagRunLogicalDateDeadline()
    result = ref.evaluate_with(
        session=session, interval=timedelta(hours=1), dag_id="dl_ld_none", run_id="run_x"
    )
    assert result is None, f"expected None for missing logical_date, got {result!r}"


# ---------------------------------------------------------------------------
# Scenario 6 + 4: error isolation in the REAL scheduler detection loop.
# A bad deadline (handle_miss raises) must not crash the scheduler nor prevent
# the other overdue deadlines from being processed.
#
# This drives the real SchedulerJobRunner._execute() (num_runs=1), patching the
# real Deadline.handle_miss so exactly one of three overdue deadlines raises.
# The desired behaviour: scheduler does not crash, and both good deadlines are
# still handled.
# ---------------------------------------------------------------------------


def test_scheduler_loop_isolates_one_bad_deadline(dag_maker, session, monkeypatch):
    from airflow.jobs.job import Job
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

    try:
        from airflow.executors.local_executor import LocalExecutor as _Exec
    except Exception:
        from tests_common.test_utils.mock_executor import MockExecutor as _Exec

    past = datetime(2025, 6, 1, tzinfo=dt_timezone.utc) - timedelta(days=4000)
    # Three dagruns, each with one overdue deadline.
    deadlines = []
    for i in range(3):
        dr = _make_dagrun(dag_maker, session, f"dl_iso_{i}", DEFAULT_DATE + timedelta(days=i), f"run_{i}")
        deadlines.append(_make_deadline(session, dr, past))
    session.commit()
    bad_id = deadlines[1].id

    real_handle_miss = Deadline.handle_miss
    processed: list = []

    def patched(self, sess):
        if self.id == bad_id:
            raise RuntimeError("induced handle_miss failure")
        processed.append(self.id)
        return real_handle_miss(self, sess)

    monkeypatch.setattr(Deadline, "handle_miss", patched)

    job = Job()
    runner = SchedulerJobRunner(job=job, num_runs=1, executors=[_Exec()])

    crashed = None
    try:
        runner._execute()
    except Exception as e:
        crashed = e

    good_ids = {deadlines[0].id, deadlines[2].id}
    assert crashed is None, (
        f"scheduler crashed on one bad deadline instead of isolating it: {crashed!r}. "
        f"Processed before crash: {processed}."
    )
    assert good_ids.issubset(set(processed)), (
        f"good deadlines not all processed: processed={processed}, expected superset of {good_ids}"
    )
