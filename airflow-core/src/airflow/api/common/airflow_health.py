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
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import get_configured_bundle_team_names
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"
DEGRADED = "degraded"
DOWN = "down"


@provide_session
def get_jobs_health(job_runner_class, *, session: Session = NEW_SESSION) -> list[Job]:
    """Return unfinished jobs for the runner class, ordered by latest heartbeat."""
    return list(
        session.scalars(
            select(Job)
            .where(
                Job.job_type == job_runner_class.job_type,
                Job.end_date.is_(None),
            )
            .order_by(Job.latest_heartbeat.desc())
        )
    )


def _job_instance_health(job: Job, heartbeat_field_name: str) -> dict[str, Any]:
    heartbeat = job.latest_heartbeat.isoformat() if job.latest_heartbeat else None
    return {
        "hostname": job.hostname,
        heartbeat_field_name: heartbeat,
    }


def _live_jobs(jobs: list[Job]) -> list[Job]:
    """
    Narrow unfinished job rows down to the replicas that are actually running.

    ``instances`` describes the deployment as it is now, so a row left behind by a replica that
    never got to write its ``end_date`` must not be listed: the host it names is gone, and under an
    orchestrator that assigns a fresh hostname per restart it will never come back. The unfinished
    rows are still what ``status`` and ``latest_*_heartbeat`` are derived from, so how long a dead
    component has been silent stays visible even once it drops out of ``instances``.
    """
    return [job for job in jobs if job.is_alive()]


def _legacy_status(jobs: list[Job]) -> str:
    """Top-level status: healthy if any instance is alive."""
    return HEALTHY if any(job.is_alive() for job in jobs) else UNHEALTHY


def _triggerer_instance_health(job: Job) -> dict[str, Any]:
    return {
        **_job_instance_health(job, "latest_triggerer_heartbeat"),
        "team_name": job.team_name,
    }


def _dag_processor_instance_health(job: Job) -> dict[str, Any]:
    return {
        **_job_instance_health(job, "latest_dag_processor_heartbeat"),
        "bundle_names": job.bundle_names,
    }


# ``detailed_status`` answers "is every part of this component's work being done", which needs a
# denominator. Counting job rows cannot supply one: ``end_date`` is only written by a cooperative
# shutdown, so a replica lost to SIGKILL, an OOM kill, or a node eviction leaves an unfinished row
# behind forever and a restarted replica adds a second one. The denominator is therefore taken from
# the declared work partition instead, which is unaffected by how replicas come and go:
#
# * Dag processor -- the bundles in ``[dag_processor] dag_bundle_config_list``.
# * Triggerer -- the team scopes those bundles declare, since a triggerer only picks up triggers for
#   its own team (see ``Trigger.ids_for_triggerer``).
# * Scheduler -- schedulers are symmetric, so there is no partition and no partial state to report.


def _configured_bundle_teams() -> dict[str, str | None]:
    """Map every configured Dag bundle to the team owning it, empty when the config is unreadable."""
    try:
        return get_configured_bundle_team_names()
    except Exception:
        # A health probe must not fail on malformed bundle config; callers fall back to liveness only.
        log.warning("Could not read the Dag bundle configuration", exc_info=True)
        return {}


def _liveness_status(jobs: list[Job]) -> str:
    """Status for a component with no declared work partition: one live replica covers everything."""
    return HEALTHY if any(job.is_alive() for job in jobs) else DOWN


def _coverage_status(expected: set[Any], covered: set[Any]) -> str:
    """Status from how much of a component's declared work partition its live replicas cover."""
    if not expected - covered:
        return HEALTHY
    if expected & covered:
        return DEGRADED
    return DOWN


def _dag_processor_detailed_status(jobs: list[Job]) -> str:
    expected = set(_configured_bundle_teams())
    if not expected:
        return _liveness_status(jobs)

    covered: set[str] = set()
    for job in jobs:
        if job.is_alive():
            # A processor started without ``--bundle-name`` parses every configured bundle.
            covered |= expected if not job.bundle_names else expected & set(job.bundle_names)
    return _coverage_status(expected, covered)


def _triggerer_detailed_status(jobs: list[Job]) -> str:
    if not conf.getboolean("core", "multi_team"):
        # Outside multi-team mode no team filter is applied, so any live triggerer serves every trigger.
        return _liveness_status(jobs)

    expected = set(_configured_bundle_teams().values())
    if not expected:
        return _liveness_status(jobs)

    return _coverage_status(expected, {job.team_name for job in jobs if job.is_alive()})


def get_airflow_health() -> dict[str, Any]:
    """Get the health for Airflow metadatabase, scheduler, triggerer, and dag processor."""
    metadatabase_status = HEALTHY

    latest_scheduler_heartbeat = None
    latest_triggerer_heartbeat = None
    latest_dag_processor_heartbeat = None

    scheduler_instances: list[dict[str, Any]] | None = None
    triggerer_instances: list[dict[str, Any]] | None = None
    dag_processor_instances: list[dict[str, Any]] | None = None

    scheduler_status = UNHEALTHY
    triggerer_status = UNHEALTHY
    dag_processor_status = UNHEALTHY

    scheduler_detailed_status = DOWN
    triggerer_detailed_status = DOWN
    dag_processor_detailed_status = DOWN

    try:
        scheduler_jobs = get_jobs_health(SchedulerJobRunner)
        scheduler_status = _legacy_status(scheduler_jobs)
        scheduler_detailed_status = _liveness_status(scheduler_jobs)
        if scheduler_jobs and scheduler_jobs[0].latest_heartbeat:
            latest_scheduler_heartbeat = scheduler_jobs[0].latest_heartbeat.isoformat()
        if live_scheduler_jobs := _live_jobs(scheduler_jobs):
            scheduler_instances = [
                _job_instance_health(job, "latest_scheduler_heartbeat") for job in live_scheduler_jobs
            ]
    except Exception:
        metadatabase_status = UNHEALTHY

    try:
        triggerer_jobs = get_jobs_health(TriggererJobRunner)
        triggerer_status = _legacy_status(triggerer_jobs)
        triggerer_detailed_status = _triggerer_detailed_status(triggerer_jobs)
        if triggerer_jobs and triggerer_jobs[0].latest_heartbeat:
            latest_triggerer_heartbeat = triggerer_jobs[0].latest_heartbeat.isoformat()
        if live_triggerer_jobs := _live_jobs(triggerer_jobs):
            triggerer_instances = [_triggerer_instance_health(job) for job in live_triggerer_jobs]
    except Exception:
        metadatabase_status = UNHEALTHY
        triggerer_status = UNHEALTHY
        triggerer_detailed_status = DOWN

    try:
        dag_processor_jobs = get_jobs_health(DagProcessorJobRunner)
        dag_processor_status = _legacy_status(dag_processor_jobs)
        dag_processor_detailed_status = _dag_processor_detailed_status(dag_processor_jobs)
        if dag_processor_jobs and dag_processor_jobs[0].latest_heartbeat:
            latest_dag_processor_heartbeat = dag_processor_jobs[0].latest_heartbeat.isoformat()
        if live_dag_processor_jobs := _live_jobs(dag_processor_jobs):
            dag_processor_instances = [_dag_processor_instance_health(job) for job in live_dag_processor_jobs]
    except Exception:
        metadatabase_status = UNHEALTHY
        dag_processor_status = UNHEALTHY
        dag_processor_detailed_status = DOWN

    airflow_health_status = {
        "metadatabase": {"status": metadatabase_status},
        "scheduler": {
            "status": scheduler_status,
            "latest_scheduler_heartbeat": latest_scheduler_heartbeat,
            "detailed_status": scheduler_detailed_status,
            "instances": scheduler_instances,
        },
        "triggerer": {
            "status": triggerer_status,
            "latest_triggerer_heartbeat": latest_triggerer_heartbeat,
            "detailed_status": triggerer_detailed_status,
            "instances": triggerer_instances,
        },
        "dag_processor": {
            "status": dag_processor_status,
            "latest_dag_processor_heartbeat": latest_dag_processor_heartbeat,
            "detailed_status": dag_processor_detailed_status,
            "instances": dag_processor_instances,
        },
    }

    return airflow_health_status
