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
Resilience tests for the scheduler deadline-detection loop and the
DeadlineAlert / DeadlineReference serialization layers.

Covers:
* Round-trip serialization of every DeadlineReference variant and interval type.
* The scheduler's overdue-deadline selection query under volume.
* A DeadlineReference evaluated against a DagRun missing the referenced field.
* ``Deadline.handle_miss`` when the DagRun was deleted (FK race).
* Error isolation in the scheduler loop: one deadline whose ``handle_miss``
  raises must not crash the scheduler nor block the other overdue deadlines.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone

import pytest
from sqlalchemy import delete, select

from airflow.models import DagRun
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import (
    DeadlineReference,
    FixedDatetimeDeadline,
)
from airflow.serialization.decoders import decode_deadline_alert
from airflow.serialization.encoders import encode_deadline_alert
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
# Scenario 1 + 8: DeadlineReference variants serialize/deserialize round-trip;
# re-serialization stability.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reference",
    [
        pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
        pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
        pytest.param(
            DeadlineReference.FIXED_DATETIME(
                datetime(2025, 5, 4, 12, 30, 15, 123456, tzinfo=dt_timezone.utc)
            ),
            id="fixed_microseconds",
        ),
        pytest.param(DeadlineReference.AVERAGE_RUNTIME(max_runs=7, min_runs=3), id="average_runtime"),
    ],
)
def test_reference_roundtrip_and_restability(reference):
    """Round-trip each reference through the DAG-level deadline encode/decode."""
    from airflow.sdk.definitions.deadline import DeadlineAlert

    alert = DeadlineAlert(
        reference=reference,
        interval=timedelta(hours=1),
        callback=AsyncCallback(CB_PATH),
    )
    encoded1 = encode_deadline_alert(alert)
    decoded1 = decode_deadline_alert(encoded1)

    # Re-serialize the decoded reference and compare the reference dicts.
    ref_dict_1 = encoded1["reference"]
    ref_dict_2 = decoded1.reference.serialize_reference()
    # drop __class_path if present (only builtins here)
    ref_dict_1 = {k: v for k, v in ref_dict_1.items() if k != "__class_path"}
    assert ref_dict_1 == ref_dict_2, f"reference drifted: {ref_dict_1} != {ref_dict_2}"

    # Full re-encode stability (scenario 8).
    re_alert = DeadlineAlert(
        reference=decoded1.reference,
        interval=decoded1.interval,
        callback=AsyncCallback(CB_PATH),
    )
    encoded2 = encode_deadline_alert(re_alert)
    assert encoded2["reference"] == encoded1["reference"], "reference not stable across re-serialization"
    assert encoded2["interval"] == encoded1["interval"], "interval not stable across re-serialization"


def test_fixed_datetime_preserves_microseconds_and_tz():
    """
    FixedDatetimeDeadline serializes via .timestamp(); probe whether the
    computed deadline_time matches after round-trip (sub-second + tz).
    """
    dt = datetime(2025, 5, 4, 12, 30, 15, 123456, tzinfo=dt_timezone.utc)
    ref = FixedDatetimeDeadline(dt)
    restored = FixedDatetimeDeadline.deserialize_reference(ref.serialize_reference())
    assert restored._datetime == dt, f"FIXED datetime corrupted: {restored._datetime} != {dt}"


# ---------------------------------------------------------------------------
# Scenario 2: interval as different types.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("interval", "expected_td"),
    [
        pytest.param(timedelta(hours=2), timedelta(hours=2), id="timedelta"),
        pytest.param(timedelta(seconds=0), timedelta(seconds=0), id="zero"),
        pytest.param(timedelta(seconds=-30), timedelta(seconds=-30), id="negative"),
        pytest.param(timedelta(microseconds=500000), timedelta(microseconds=500000), id="subsecond"),
    ],
)
def test_interval_types_roundtrip(interval, expected_td):
    from airflow.sdk.definitions.deadline import DeadlineAlert

    alert = DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=interval,
        callback=AsyncCallback(CB_PATH),
    )
    decoded = decode_deadline_alert(encode_deadline_alert(alert))
    assert decoded.interval == expected_td, f"interval drift: {decoded.interval} != {expected_td}"


def test_interval_backcompat_numeric_seconds():
    """Numeric (legacy total_seconds) interval should decode to a timedelta."""
    encoded = {
        "name": None,
        "reference": DeadlineReference.DAGRUN_LOGICAL_DATE.serialize_reference(),
        "interval": 3600,  # legacy numeric form
        "callback": __import__("airflow.sdk.serde", fromlist=["serialize"]).serialize(AsyncCallback(CB_PATH)),
    }
    decoded = decode_deadline_alert(encoded)
    assert decoded.interval == timedelta(seconds=3600)


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


# The exact scheduler detection query (mirrors scheduler_job_runner ~L1752).
def _overdue_query(now):
    return select(Deadline).where(Deadline.deadline_time < now).where(~Deadline.missed)


# ---------------------------------------------------------------------------
# Scenario 3: volume query — 100 deadlines, half overdue.
# ---------------------------------------------------------------------------


def test_scheduler_query_under_volume(dag_maker, session):
    now = datetime(2025, 6, 1, tzinfo=dt_timezone.utc)
    overdue_ids = set()
    future_ids = set()
    # 10 dagruns, 10 deadlines each = 100.
    for d in range(10):
        dr = _make_dagrun(dag_maker, session, f"dl_vol_{d}", DEFAULT_DATE + timedelta(days=d), f"run_{d}")
        for i in range(10):
            overdue = i % 2 == 0
            dt = now - timedelta(hours=1) if overdue else now + timedelta(hours=1)
            dl = _make_deadline(session, dr, dt)
            (overdue_ids if overdue else future_ids).add(dl.id)
    session.commit()

    selected = list(session.scalars(_overdue_query(now)))
    selected_ids = [dl.id for dl in selected]

    assert len(selected_ids) == len(set(selected_ids)), "double-selected a deadline"
    assert set(selected_ids) == overdue_ids, (
        f"missed/extra: missed={overdue_ids - set(selected_ids)} extra={set(selected_ids) - overdue_ids}"
    )
    assert not (set(selected_ids) & future_ids), "selected a future deadline"


def test_already_missed_deadline_excluded_from_selection(dag_maker, session):
    """Cross-loop idempotency: a deadline already marked ``missed`` is NOT re-selected.

    The scheduler's detection query filters ``~Deadline.missed``. Once ``handle_miss`` has
    marked a deadline missed (and queued its callback) in one loop, the next loop's query
    must exclude it — otherwise the scheduler would queue a duplicate callback for the same
    deadline on every subsequent pass. This is also the per-row idempotency guarantee that,
    together with ``FOR UPDATE SKIP LOCKED``, prevents HA scheduler replicas from
    double-processing the same overdue deadline.
    """
    now = datetime(2025, 6, 1, tzinfo=dt_timezone.utc)
    dr = _make_dagrun(dag_maker, session, "dl_missed_excl", DEFAULT_DATE, "run_missed")
    overdue_time = now - timedelta(hours=1)

    handled = _make_deadline(session, dr, overdue_time)  # already processed in a prior loop
    handled.missed = True
    pending = _make_deadline(session, dr, overdue_time)  # not yet handled
    session.add(handled)
    session.commit()

    selected_ids = {dl.id for dl in session.scalars(_overdue_query(now))}

    assert handled.id not in selected_ids, "already-missed deadline was re-selected (duplicate-callback risk)"
    assert pending.id in selected_ids, "unhandled overdue deadline was not selected"


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
# Scenario 5: DagRun deleted between detection and handle_miss.
# ---------------------------------------------------------------------------


def test_handle_miss_after_dagrun_deleted(dag_maker, session):
    now = datetime(2025, 6, 1, tzinfo=dt_timezone.utc)
    dr = _make_dagrun(dag_maker, session, "dl_del", DEFAULT_DATE, "run_del")
    dl = _make_deadline(session, dr, now - timedelta(hours=1))
    session.commit()
    deadline_id = dl.id

    # Simulate detection having selected the deadline, then the DagRun row
    # disappearing (FK race). Expire so the relationship reloads from DB.
    session.execute(delete(DagRun).where(DagRun.id == dr.id))
    session.commit()
    session.expire_all()

    dl = session.get(Deadline, deadline_id)
    if dl is None:
        # CASCADE removed it -> clean (no work to do).
        return
    # Deadline survived; handle_miss must not raise.
    try:
        dl.handle_miss(session)
    except Exception as e:
        pytest.fail(f"handle_miss crashed on deleted DagRun: {type(e).__name__}: {e}")


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
