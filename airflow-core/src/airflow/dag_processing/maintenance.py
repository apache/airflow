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

"""Bounded metadata maintenance owned by the scheduler, including absent bundles."""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import delete, or_, select, tuple_

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.jobs.job import Job, JobState
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import with_row_locks


def cleanup_processor_metadata(batch_size: int = 500) -> None:
    """Commit each bounded batch; a later interval picks up remaining rows."""
    with create_session() as session:
        inactive = select(DagBundleModel.name).where(DagBundleModel.active.is_(False))
        query = (
            select(DagModel)
            .where(
                DagModel.is_stale.is_(False),
                or_(DagModel.bundle_name.is_(None), DagModel.bundle_name.in_(inactive)),
            )
            .order_by(DagModel.dag_id)
            .limit(batch_size)
        )
        for dag in session.scalars(with_row_locks(query, session=session, skip_locked=True)):
            dag.is_stale = True
    with create_session() as session:
        warning_query = (
            select(DagWarning.dag_id, DagWarning.warning_type)
            .join(DagModel)
            .where(
                DagModel.is_stale.is_(True),
            )
            .order_by(DagWarning.dag_id, DagWarning.warning_type)
            .limit(batch_size)
        )
        keys = session.execute(
            with_row_locks(warning_query, of=DagWarning, session=session, skip_locked=True)
        ).all()
        if keys:
            session.execute(
                delete(DagWarning).where(tuple_(DagWarning.dag_id, DagWarning.warning_type).in_(keys))
            )
    with create_session() as session:
        cutoff = timezone.utcnow() - timedelta(seconds=conf.getint("dag_processor", "health_check_threshold"))
        job_query = (
            select(Job)
            .where(
                Job.job_type == "DagProcessorJob",
                Job.session_id.is_not(None),
                Job.end_date.is_(None),
                Job.latest_heartbeat < cutoff,
            )
            .order_by(Job.id)
            .limit(batch_size)
        )
        for job in session.scalars(with_row_locks(job_query, session=session, skip_locked=True)):
            job.state = JobState.FAILED
            job.end_date = timezone.utcnow()
