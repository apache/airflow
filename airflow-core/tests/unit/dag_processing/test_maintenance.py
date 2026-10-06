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

from datetime import timedelta

import pytest
from sqlalchemy import select
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.dag_processing.maintenance import cleanup_processor_metadata
from airflow.jobs.job import Job, JobState
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_jobs

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_jobs()
    clear_db_dags()
    clear_db_dag_bundles()
    yield
    clear_db_jobs()
    clear_db_dags()
    clear_db_dag_bundles()


def test_cleanup_is_bounded_and_retains_live_bundles(session, time_machine):
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
    cleanup_processor_metadata(batch_size=2)
    session.expire_all()
    assert len(session.scalars(select(DagModel).where(DagModel.is_stale.is_(True))).all()) == 2
    cleanup_processor_metadata(batch_size=2)
    session.expire_all()
    assert len(session.scalars(select(DagModel).where(DagModel.is_stale.is_(True))).all()) == 3
    assert not session.get(DagModel, "current").is_stale


@conf_vars(
    {
        ("dag_processor", "health_check_threshold"): "30",
        ("dag_processor", "job_heartbeat_timeout"): "120",
    }
)
def test_cleanup_retires_only_expired_api_processor_jobs(session, time_machine):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    jobs = [Job(job_type="DagProcessorJob", state=JobState.RUNNING) for _ in range(4)]
    for job in jobs[:3]:
        job.session_id = uuid7()
    jobs[0].latest_heartbeat = jobs[3].latest_heartbeat = timezone.utcnow() - timedelta(seconds=180)
    jobs[1].latest_heartbeat = timezone.utcnow() - timedelta(seconds=60)
    session.add_all(jobs)
    session.commit()
    job_ids = [job.id for job in jobs]
    cleanup_processor_metadata()
    jobs = [session.get(Job, job_id) for job_id in job_ids]
    assert jobs[0].end_date is not None
    assert jobs[0].state == JobState.FAILED
    assert [job.end_date for job in jobs[1:]] == [None, None, None]


def test_cleanup_warnings_is_bounded_and_preserves_active_dags(session):
    session.add(DagBundleModel(name="live"))
    session.flush()
    session.add_all([DagModel(dag_id=f"dag_{i}", bundle_name="live", is_stale=i < 3) for i in range(4)])
    session.flush()
    session.add_all(
        [DagWarning(dag_id=f"dag_{i}", warning_type="python:test", message="warning") for i in range(4)]
    )
    session.commit()
    cleanup_processor_metadata(batch_size=2)
    assert len(session.scalars(select(DagWarning)).all()) == 2
    cleanup_processor_metadata(batch_size=2)
    assert [warning.dag_id for warning in session.scalars(select(DagWarning))] == ["dag_3"]
