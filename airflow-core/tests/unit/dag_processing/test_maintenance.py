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

import sqlite3
from contextlib import contextmanager
from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import event, select
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.dag_processing.maintenance import cleanup_processor_metadata
from airflow.jobs.job import Job, JobState
from airflow.models.dag import DagModel
from airflow.models.dag_processor_publication_receipt import DagProcessorPublicationReceipt
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_jobs

pytestmark = pytest.mark.db_test


@contextmanager
def _set_sqlite_variable_limit(engine, limit):
    original_limits = {}

    def set_limit(connection, connection_record, connection_proxy):
        original_limits[connection_record] = (
            connection,
            connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit),
        )

    def restore_limit(connection, connection_record):
        original = original_limits.pop(connection_record, None)
        if original is not None and connection is not None:
            connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, original[1])

    event.listen(engine, "checkout", set_limit)
    event.listen(engine, "checkin", restore_limit)  # codespell:ignore checkin
    try:
        yield
    finally:
        event.remove(engine, "checkout", set_limit)
        event.remove(engine, "checkin", restore_limit)  # codespell:ignore checkin
        for connection, original_limit in original_limits.values():
            connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, original_limit)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_jobs()
    clear_db_dags()
    clear_db_dag_bundles()
    yield
    clear_db_jobs()
    clear_db_dags()
    clear_db_dag_bundles()


@mock.patch("airflow.dag_processing.maintenance.create_session", autospec=True)
def test_cleanup_drains_inactive_dags_in_committed_batches(mock_session, session, time_machine, caplog):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    inactive = DagBundleModel(name="removed")
    inactive.active = False
    session.add_all([inactive, DagBundleModel(name="live")])
    session.flush()
    session.add_all(
        [DagModel(dag_id=f"removed_{i}", bundle_name="removed", is_stale=False) for i in range(3)]
        + [DagModel(dag_id="current", bundle_name="live", is_stale=False)]
    )
    session.commit()
    committed_dags = []

    @contextmanager
    def observe_commits():
        with create_session(scoped=False) as maintenance_session:
            yield maintenance_session
        with create_session(scoped=False) as observer:
            committed_dags.append(observer.scalars(select(DagModel.dag_id).where(DagModel.is_stale)).all())

    mock_session.side_effect = observe_commits
    cleanup_processor_metadata(batch_size=2)
    session.expire_all()
    assert sorted(committed_dags[0]) == ["removed_0", "removed_1"]
    assert sorted(committed_dags[1]) == ["removed_0", "removed_1", "removed_2"]
    assert len(session.scalars(select(DagModel).where(DagModel.is_stale.is_(True))).all()) == 3
    assert [
        (entry["dag_id"], entry["bundle_name"])
        for entry in caplog
        if entry["event"] == "Deactivating Dag because its bundle is no longer active"
    ] == [("removed_0", "removed"), ("removed_1", "removed"), ("removed_2", "removed")]
    assert not session.get(DagModel, "current").is_stale


def _add_checkpoints(job_id, count, *, session):
    session.add_all(
        DagProcessorPublicationReceipt(
            job_id=job_id,
            source_key=f"{index:064x}",
            attempt_id=uuid7(),
            dispatch_sequence=1,
            payload_hash="0" * 64,
            accepted_at=timezone.utcnow(),
        )
        for index in range(count)
    )


@conf_vars(
    {
        ("dag_processor", "health_check_threshold"): "30",
        ("dag_processor", "job_heartbeat_timeout"): "120",
    }
)
def test_cleanup_retires_only_expired_api_processor_jobs(session, time_machine, caplog):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    jobs = [Job(job_type="DagProcessorJob", state=JobState.RUNNING) for _ in range(4)]
    for job in jobs[:3]:
        job.session_id = uuid7()
    jobs[0].hostname = "expired-processor"
    jobs[0].latest_heartbeat = jobs[3].latest_heartbeat = timezone.utcnow() - timedelta(seconds=180)
    jobs[1].latest_heartbeat = timezone.utcnow() - timedelta(seconds=60)
    session.add_all(jobs)
    session.flush()
    for job in jobs:
        _add_checkpoints(job.id, 1, session=session)
    session.commit()
    job_ids = [job.id for job in jobs]
    cleanup_processor_metadata()
    jobs = [session.get(Job, job_id) for job_id in job_ids]
    assert jobs[0].end_date is not None
    assert jobs[0].state == JobState.FAILED
    assert [job.end_date for job in jobs[1:]] == [None, None, None]
    assert set(session.scalars(select(DagProcessorPublicationReceipt.job_id))) == set(job_ids[1:])
    fields = ("job_id", "hostname", "latest_heartbeat", "job_heartbeat_timeout")
    assert [
        {field: entry.get(field) for field in fields}
        for entry in caplog
        if entry["event"] == "Expiring Dag processor Job and releasing its claimed work"
    ] == [
        {
            "job_id": job_ids[0],
            "hostname": "expired-processor",
            "latest_heartbeat": timezone.utcnow() - timedelta(seconds=180),
            "job_heartbeat_timeout": 120,
        }
    ]


@conf_vars({("dag_processor", "job_heartbeat_timeout"): "120"})
def test_cleanup_expires_api_processor_jobs_in_bounded_batches(session, time_machine):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    jobs = [
        Job(
            job_type="DagProcessorJob",
            state=JobState.RUNNING,
            session_id=uuid7(),
            latest_heartbeat=timezone.utcnow() - timedelta(seconds=180),
        )
        for _ in range(3)
    ]
    session.add_all(jobs)
    session.commit()
    job_ids = sorted(job.id for job in jobs)

    def get_expired_job_ids():
        session.expire_all()
        return session.scalars(select(Job.id).where(Job.end_date.is_not(None)).order_by(Job.id)).all()

    cleanup_processor_metadata(batch_size=2)
    assert get_expired_job_ids() == job_ids[:2]
    cleanup_processor_metadata(batch_size=2)
    assert get_expired_job_ids() == job_ids


@pytest.mark.parametrize("batch_size", [0, -1])
def test_cleanup_rejects_nonpositive_batches(batch_size):
    with pytest.raises(ValueError, match="batch_size must be positive"):
        cleanup_processor_metadata(batch_size=batch_size)


@pytest.mark.parametrize("timeout", [29, 30, 31])
def test_cleanup_rejects_expiry_before_the_health_threshold(timeout, session, time_machine, caplog):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    job = Job(
        job_type="DagProcessorJob",
        state=JobState.RUNNING,
        session_id=uuid7(),
        latest_heartbeat=timezone.utcnow() - timedelta(seconds=60),
    )
    session.add(job)
    session.flush()
    job_id = job.id
    session.commit()

    with conf_vars(
        {
            ("dag_processor", "health_check_threshold"): "30",
            ("dag_processor", "job_heartbeat_timeout"): str(timeout),
        }
    ):
        cleanup_processor_metadata()

    skipped = {
        "event": "Skipping Dag processor Job expiration because of invalid configuration",
        "error": (
            "[dag_processor] job_heartbeat_timeout must be greater than or equal to health_check_threshold"
        ),
    }
    assert (skipped in caplog) == (timeout < 30)

    job = session.get(Job, job_id)
    assert job.state == (JobState.RUNNING if timeout < 30 else JobState.FAILED)
    assert job.end_date == (None if timeout < 30 else timezone.utcnow())


@conf_vars(
    {
        ("dag_processor", "health_check_threshold"): "600",
        ("dag_processor", "job_heartbeat_timeout"): "300",
    }
)
@pytest.mark.parametrize("has_api_job", [False, True])
def test_cleanup_continues_when_api_heartbeat_timeouts_are_invalid(
    session, time_machine, caplog, has_api_job
):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    bundle = DagBundleModel(name="removed")
    bundle.active = False
    session.add(bundle)
    session.flush()
    session.add(DagModel(dag_id="removed", bundle_name="removed", is_stale=False))
    session.flush()
    session.add(DagWarning(dag_id="removed", warning_type="python:test", message="warning"))
    retired = Job(job_type="DagProcessorJob", state=JobState.FAILED, end_date=timezone.utcnow())
    session.add(retired)
    session.flush()
    _add_checkpoints(retired.id, 1, session=session)
    live_job_id = None
    if has_api_job:
        live = Job(
            job_type="DagProcessorJob",
            state=JobState.RUNNING,
            session_id=uuid7(),
            latest_heartbeat=timezone.utcnow() - timedelta(seconds=900),
        )
        session.add(live)
        session.flush()
        live_job_id = live.id
        _add_checkpoints(live_job_id, 1, session=session)
    session.commit()

    cleanup_processor_metadata()

    skipped = "Skipping Dag processor Job expiration because of invalid configuration"
    assert (skipped in caplog) == has_api_job
    session.expire_all()
    assert session.get(DagModel, "removed").is_stale
    assert session.scalars(select(DagWarning)).all() == []
    assert session.scalars(select(DagProcessorPublicationReceipt.job_id)).all() == (
        [live_job_id] if has_api_job else []
    )
    if has_api_job:
        live = session.get(Job, live_job_id)
        assert live.state == JobState.RUNNING
        assert live.end_date is None


@conf_vars({("dag_processor", "job_heartbeat_timeout"): "120"})
@mock.patch("airflow.dag_processing.maintenance.create_session", autospec=True)
def test_checkpoint_cleanup_resumes_after_a_committed_batch(mock_session, session, time_machine):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    expired = Job(
        job_type="DagProcessorJob",
        state=JobState.RUNNING,
        session_id=uuid7(),
        latest_heartbeat=timezone.utcnow() - timedelta(seconds=180),
    )
    live = Job(job_type="DagProcessorJob", state=JobState.RUNNING, session_id=uuid7())
    session.add_all([expired, live])
    session.flush()
    _add_checkpoints(expired.id, 5, session=session)
    _add_checkpoints(live.id, 1, session=session)
    session.commit()
    expired_id, live_id = expired.id, live.id
    interrupted = False
    committed_counts = []

    @contextmanager
    def interrupt_after_checkpoint_commit():
        nonlocal interrupted
        with create_session(scoped=False) as maintenance_session:
            yield maintenance_session
        with create_session(scoped=False) as observer:
            remaining = len(observer.scalars(select(DagProcessorPublicationReceipt)).all())
        if not committed_counts or committed_counts[-1] != remaining:
            committed_counts.append(remaining)
        if remaining == 4 and not interrupted:
            interrupted = True
            raise RuntimeError("Interrupted after a checkpoint batch committed")

    mock_session.side_effect = interrupt_after_checkpoint_commit
    with pytest.raises(RuntimeError, match="Interrupted after a checkpoint batch committed"):
        cleanup_processor_metadata(batch_size=2)

    session.expire_all()
    assert session.get(Job, expired_id).state == JobState.FAILED
    assert session.get(Job, expired_id).end_date == timezone.utcnow()
    assert len(session.scalars(select(DagProcessorPublicationReceipt)).all()) == 4
    session.rollback()

    cleanup_processor_metadata(batch_size=2)

    assert committed_counts == [6, 4, 2, 1]
    assert session.scalars(select(DagProcessorPublicationReceipt.job_id)).all() == [live_id]
    assert session.get(Job, live_id).end_date is None


@mock.patch("airflow.dag_processing.maintenance.create_session", autospec=True)
def test_cleanup_drains_stale_dag_warnings_in_committed_batches(mock_session, session):
    session.add(DagBundleModel(name="live"))
    session.flush()
    session.add_all([DagModel(dag_id=f"dag_{i}", bundle_name="live", is_stale=i < 3) for i in range(4)])
    session.flush()
    session.add_all(
        [DagWarning(dag_id=f"dag_{i}", warning_type="python:test", message="warning") for i in range(4)]
    )
    session.commit()
    committed_counts = []

    @contextmanager
    def observe_commits():
        with create_session(scoped=False) as maintenance_session:
            yield maintenance_session
        with create_session(scoped=False) as observer:
            remaining = len(observer.scalars(select(DagWarning)).all())
        if not committed_counts or committed_counts[-1] != remaining:
            committed_counts.append(remaining)

    mock_session.side_effect = observe_commits
    cleanup_processor_metadata(batch_size=2)

    assert committed_counts == [4, 2, 1]
    assert [warning.dag_id for warning in session.scalars(select(DagWarning))] == ["dag_3"]


@conf_vars({("dag_processor", "job_heartbeat_timeout"): "120"})
def test_default_cleanup_respects_sqlite_variable_limit_and_retires_jobs(session, time_machine):
    engine = session.get_bind()
    if engine.dialect.name != "sqlite":
        pytest.skip("Exercises SQLite's host parameter limit")
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    session.add(DagBundleModel(name="live"))
    session.flush()
    session.add_all(
        [
            DagModel(dag_id="stale", bundle_name="live", is_stale=True),
            DagModel(dag_id="active", bundle_name="live", is_stale=False),
        ]
    )
    expired_job = Job(
        job_type="DagProcessorJob",
        state=JobState.RUNNING,
        session_id=uuid7(),
        latest_heartbeat=timezone.utcnow() - timedelta(seconds=180),
    )
    session.add(expired_job)
    live_job = Job(job_type="DagProcessorJob", state=JobState.RUNNING, session_id=uuid7())
    session.add(live_job)
    session.flush()
    expired_job_id = expired_job.id
    _add_checkpoints(expired_job_id, 500, session=session)
    _add_checkpoints(live_job.id, 1, session=session)
    live_job_id = live_job.id
    session.add_all(
        [DagWarning(dag_id="stale", warning_type=f"python:test_{i}", message="warning") for i in range(500)]
        + [DagWarning(dag_id="active", warning_type="python:test", message="warning")]
    )
    session.commit()

    with _set_sqlite_variable_limit(engine, 999):
        cleanup_processor_metadata()

    assert session.scalars(select(DagWarning.dag_id)).all() == ["active"]
    retired = session.get(Job, expired_job_id)
    assert retired.state == JobState.FAILED
    assert retired.end_date == timezone.utcnow()
    assert session.scalars(select(DagProcessorPublicationReceipt.job_id)).all() == [live_job_id]
