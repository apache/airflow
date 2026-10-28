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

import logging
from datetime import timedelta

from sqlalchemy import delete, exists, select, tuple_

from airflow._shared.timezones import timezone
from airflow.dag_processing.timeouts import get_job_heartbeat_timeout
from airflow.exceptions import AirflowConfigException
from airflow.jobs.job import Job, JobState
from airflow.models.dag import DagModel
from airflow.models.dag_processor_publication_receipt import DagProcessorPublicationReceipt
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import with_row_locks

log = logging.getLogger(__name__)


def cleanup_processor_metadata(batch_size: int = 500) -> None:
    """Drain inactive Dags and retired checkpoints, committing each bounded batch."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    while True:
        with create_session() as session:
            inactive = select(DagBundleModel.name).where(DagBundleModel.active.is_(False))
            query = (
                select(DagModel)
                .where(
                    DagModel.is_stale.is_(False),
                    DagModel.bundle_name.in_(inactive),
                )
                .order_by(DagModel.dag_id)
                .limit(batch_size)
            )
            dags = session.scalars(with_row_locks(query, session=session, skip_locked=True)).all()
            for dag in dags:
                log.info(
                    "Deactivating Dag %s. Its bundle %s is no longer active.",
                    dag.dag_id,
                    dag.bundle_name,
                )
                dag.is_stale = True
        if len(dags) < batch_size:
            break
    with create_session() as session:
        warning_query = (
            select(DagWarning.dag_id, DagWarning.warning_type)
            .join(DagModel)
            .where(
                DagModel.is_stale.is_(True),
            )
            .order_by(DagWarning.dag_id, DagWarning.warning_type)
            # Each key needs two bind parameters; older SQLite versions allow only 999.
            .limit(min(batch_size, 499))
        )
        keys = session.execute(
            with_row_locks(warning_query, of=DagWarning, session=session, skip_locked=True)
        ).all()
        if keys:
            session.execute(
                delete(DagWarning).where(tuple_(DagWarning.dag_id, DagWarning.warning_type).in_(keys))
            )
    try:
        timeout = get_job_heartbeat_timeout()
    except AirflowConfigException as error:
        log.warning("Skipping Dag processor Job expiration because of invalid configuration: %s", error)
    else:
        with create_session() as session:
            cutoff = timezone.utcnow() - timedelta(seconds=timeout)
            job_query = (
                select(Job)
                .where(
                    Job.job_type == "DagProcessorJob",
                    # API Jobs end with a terminal state; use its heartbeat index to skip completed history.
                    Job.state == JobState.RUNNING,
                    Job.session_id.is_not(None),
                    Job.end_date.is_(None),
                    Job.latest_heartbeat < cutoff,
                )
                .order_by(Job.id)
                .limit(batch_size)
            )
            for job in session.scalars(with_row_locks(job_query, session=session, skip_locked=True)):
                log.warning(
                    "Expiring Dag processor Job and releasing its claimed work",
                    extra={
                        "job_id": job.id,
                        "hostname": job.hostname,
                        "latest_heartbeat": job.latest_heartbeat,
                        "job_heartbeat_timeout": timeout,
                    },
                )
                job.state = JobState.FAILED
                job.end_date = timezone.utcnow()
    # Visit already-ended Jobs too: retirement may have committed before cleanup was interrupted.
    checkpoint_batch_size = min(batch_size, 499)
    while True:
        with create_session() as session:
            checkpoint_query = (
                select(DagProcessorPublicationReceipt.job_id, DagProcessorPublicationReceipt.source_key)
                .where(
                    exists().where(
                        Job.id == DagProcessorPublicationReceipt.job_id,
                        Job.end_date.is_not(None),
                    )
                )
                .order_by(DagProcessorPublicationReceipt.job_id, DagProcessorPublicationReceipt.source_key)
                .limit(checkpoint_batch_size)
            )
            checkpoint_keys = session.execute(
                with_row_locks(checkpoint_query, session=session, skip_locked=True)
            ).all()
            if checkpoint_keys:
                session.execute(
                    delete(DagProcessorPublicationReceipt).where(
                        tuple_(
                            DagProcessorPublicationReceipt.job_id, DagProcessorPublicationReceipt.source_key
                        ).in_(checkpoint_keys)
                    )
                )
        if len(checkpoint_keys) < checkpoint_batch_size:
            break
