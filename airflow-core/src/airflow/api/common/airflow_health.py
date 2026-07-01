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
from airflow.jobs.job import Job
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
    """Return all jobs for the runner class ordered by latest heartbeat descending."""
    return list(
        session.scalars(
            select(Job).where(Job.job_type == job_runner_class.job_type).order_by(Job.latest_heartbeat.desc())
        )
    )


def _job_instance_health(job: Job, heartbeat_field_name: str) -> dict[str, Any]:
    heartbeat = job.latest_heartbeat.isoformat() if job.latest_heartbeat else None
    return {
        "hostname": job.hostname,
        "status": HEALTHY if job.is_alive() else UNHEALTHY,
        heartbeat_field_name: heartbeat,
    }


def _aggregate_status(instances: list[dict[str, Any]]) -> str:
    statuses = {instance["status"] for instance in instances}
    if not statuses or statuses == {HEALTHY}:
        return HEALTHY
    if statuses == {UNHEALTHY}:
        return UNHEALTHY
    return DEGRADED


def _alive_jobs(jobs: list[Job]) -> list[Job]:
    return [job for job in jobs if job.is_alive()]


def get_airflow_health() -> dict[str, Any]:
    """Get the health for Airflow metadatabase, scheduler and triggerer."""
    metadatabase_status = HEALTHY
    latest_scheduler_heartbeat = None
    latest_triggerer_heartbeat = None
    latest_dag_processor_heartbeat = None
    scheduler_instances: list[dict[str, Any]] | None = None
    triggerer_instances: list[dict[str, Any]] | None = None
    dag_processor_instances: list[dict[str, Any]] | None = None

    scheduler_status = UNHEALTHY
    triggerer_status: str | None = None
    dag_processor_status: str | None = None

    try:
        scheduler_jobs = get_jobs_health(SchedulerJobRunner)
        alive_scheduler_jobs = _alive_jobs(scheduler_jobs)
        scheduler_instances = [
            _job_instance_health(job, "latest_scheduler_heartbeat") for job in alive_scheduler_jobs
        ]
        scheduler_status = _aggregate_status(scheduler_instances) if scheduler_instances else UNHEALTHY
        schedulers_status = scheduler_status

        if scheduler_jobs and scheduler_jobs[0].latest_heartbeat:
            latest_scheduler_heartbeat = scheduler_jobs[0].latest_heartbeat.isoformat()
    except Exception:
        metadatabase_status = UNHEALTHY

    try:
        triggerer_jobs = get_jobs_health(TriggererJobRunner)
        alive_triggerer_jobs = _alive_jobs(triggerer_jobs)
        triggerer_instances = [
            {
                **_job_instance_health(job, "latest_triggerer_heartbeat"),
                "team_name": None,
            }
            for job in alive_triggerer_jobs
        ]
        triggerer_status = _aggregate_status(triggerer_instances) if triggerer_instances else (UNHEALTHY if triggerer_jobs else None)
        triggerers_status = triggerer_status

        if triggerer_jobs and triggerer_jobs[0].latest_heartbeat:
            latest_triggerer_heartbeat = triggerer_jobs[0].latest_heartbeat.isoformat()
    except Exception:
        metadatabase_status = UNHEALTHY
        triggerer_status = UNHEALTHY

    try:
        dag_processor_jobs = get_jobs_health(DagProcessorJobRunner)
        alive_dag_processor_jobs = _alive_jobs(dag_processor_jobs)
        dag_processor_instances = [
            _job_instance_health(job, "latest_dag_processor_heartbeat") for job in alive_dag_processor_jobs
        ]
        dag_processor_status = _aggregate_status(dag_processor_instances) if dag_processor_instances else (UNHEALTHY if dag_processor_jobs else None)
        dag_processors_status = dag_processor_status

        if dag_processor_jobs and dag_processor_jobs[0].latest_heartbeat:
            latest_dag_processor_heartbeat = dag_processor_jobs[0].latest_heartbeat.isoformat()
    except Exception:
        metadatabase_status = UNHEALTHY
        dag_processor_status = UNHEALTHY

    airflow_health_status = {
        "metadatabase": {"status": metadatabase_status},
        "scheduler": {
            "status": scheduler_status,
            "latest_scheduler_heartbeat": latest_scheduler_heartbeat,
            "detailed_status": scheduler_status,
        },
        "triggerer": {
            "status": triggerer_status,
            "latest_triggerer_heartbeat": latest_triggerer_heartbeat,
            "detailed_status": triggerer_status,
        },
        "dag_processor": {
            "status": dag_processor_status,
            "latest_dag_processor_heartbeat": latest_dag_processor_heartbeat,
            "detailed_status": dag_processor_status,
        },
    }

    return airflow_health_status
