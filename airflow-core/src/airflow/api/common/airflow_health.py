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

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, JobState
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"
DEGRADED = "degraded"


@provide_session
def get_jobs_health(job_runner_class, *, session: Session = NEW_SESSION) -> list[Job]:
    """Return all running jobs for the runner class, ordered by latest heartbeat."""
    return list(
        session.scalars(
            select(Job)
            .where(
                Job.job_type == job_runner_class.job_type,
                Job.state == JobState.RUNNING,
            )
            .order_by(Job.latest_heartbeat.desc())
        )
    )


def _job_instance_health(job: Job, heartbeat_field_name: str) -> dict[str, Any]:
    heartbeat = job.latest_heartbeat.isoformat() if job.latest_heartbeat else None
    return {
        "hostname": job.hostname,
        "status": HEALTHY if job.is_alive() else UNHEALTHY,
        heartbeat_field_name: heartbeat,
    }


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


def _aggregate_detailed_status(jobs: list[Job]) -> str:
    """detailed_status: healthy (all alive), degraded (some alive), unhealthy (none alive)."""
    alive_count = sum(1 for job in jobs if job.is_alive())
    if alive_count == 0:
        return UNHEALTHY
    if alive_count == len(jobs):
        return HEALTHY
    return DEGRADED


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
    triggerer_status: str | None = UNHEALTHY
    dag_processor_status: str | None = UNHEALTHY

    scheduler_detailed_status = UNHEALTHY
    triggerer_detailed_status: str | None = UNHEALTHY
    dag_processor_detailed_status: str | None = UNHEALTHY

    # --- Scheduler ---
    try:
        scheduler_jobs = get_jobs_health(SchedulerJobRunner)
        if scheduler_jobs:
            scheduler_status = _legacy_status(scheduler_jobs)  # The top sorted job is confirmed alive
            scheduler_detailed_status = _aggregate_detailed_status(scheduler_jobs)
            scheduler_instances = [
                _job_instance_health(job, "latest_scheduler_heartbeat") for job in scheduler_jobs
            ]
            
            if scheduler_jobs[0].latest_heartbeat:
                latest_scheduler_heartbeat = scheduler_jobs[0].latest_heartbeat.isoformat()
    except Exception:
        metadatabase_status = UNHEALTHY

    # --- Triggerer ---
    try:
        triggerer_jobs = get_jobs_health(TriggererJobRunner)
        if triggerer_jobs:
            triggerer_status = _legacy_status(triggerer_jobs)
            triggerer_detailed_status = _aggregate_detailed_status(triggerer_jobs)
            triggerer_instances = [_triggerer_instance_health(job) for job in triggerer_jobs]
            
            if triggerer_jobs[0].latest_heartbeat:
                latest_triggerer_heartbeat = triggerer_jobs[0].latest_heartbeat.isoformat()
        else:
            # If no active/alive running triggerers exist, report old fallback defaults
            triggerer_status = None
            triggerer_detailed_status = None
    except Exception:
        metadatabase_status = UNHEALTHY
        triggerer_status = UNHEALTHY
        triggerer_detailed_status = UNHEALTHY

    # --- DAG Processor ---
    try:
        dag_processor_jobs = get_jobs_health(DagProcessorJobRunner)
        if dag_processor_jobs:
            dag_processor_status = _legacy_status(dag_processor_jobs)
            dag_processor_detailed_status = _aggregate_detailed_status(dag_processor_jobs)
            dag_processor_instances = [_dag_processor_instance_health(job) for job in dag_processor_jobs]
            
            if dag_processor_jobs[0].latest_heartbeat:
                latest_dag_processor_heartbeat = dag_processor_jobs[0].latest_heartbeat.isoformat()
        else:
            dag_processor_status = None
            dag_processor_detailed_status = None
    except Exception:
        metadatabase_status = UNHEALTHY
        dag_processor_status = UNHEALTHY
        dag_processor_detailed_status = UNHEALTHY

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