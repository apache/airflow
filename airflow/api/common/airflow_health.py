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

from typing import Any

from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"


def get_airflow_health() -> dict[str, Any]:
    """Get the health for Airflow metadatabase, scheduler and triggerer."""
    metadatabase_status = HEALTHY
    latest_scheduler_heartbeat = None
    latest_triggerer_heartbeat = None
    latest_dag_processor_heartbeat = None

    scheduler_status = UNHEALTHY
    triggerer_status: str | None = UNHEALTHY
    dag_processor_status: str | None = UNHEALTHY

    try:
        latest_scheduler_job = SchedulerJobRunner.most_recent_job()

        if latest_scheduler_job:
            latest_scheduler_heartbeat = latest_scheduler_job.latest_heartbeat.isoformat()
            if latest_scheduler_job.is_alive():
                scheduler_status = HEALTHY
    except Exception:
        metadatabase_status = UNHEALTHY

    try:
        latest_triggerer_job = TriggererJobRunner.most_recent_job()

        if latest_triggerer_job:
            latest_triggerer_heartbeat = latest_triggerer_job.latest_heartbeat.isoformat()
            if latest_triggerer_job.is_alive():
                triggerer_status = HEALTHY
        else:
            triggerer_status = None
    except Exception:
        metadatabase_status = UNHEALTHY

    try:
        latest_dag_processor_job = DagProcessorJobRunner.most_recent_job()

        if latest_dag_processor_job:
            latest_dag_processor_heartbeat = latest_dag_processor_job.latest_heartbeat.isoformat()
            if latest_dag_processor_job.is_alive():
                dag_processor_status = HEALTHY
        else:
            dag_processor_status = None
    except Exception:
        metadatabase_status = UNHEALTHY

    airflow_health_status = {
        "metadatabase": {"status": metadatabase_status},
        "scheduler": {
            "status": scheduler_status,
            "latest_scheduler_heartbeat": latest_scheduler_heartbeat,
        },
        "triggerer": {
            "status": triggerer_status,
            "latest_triggerer_heartbeat": latest_triggerer_heartbeat,
        },
        "dag_processor": {
            "status": dag_processor_status,
            "latest_dag_processor_heartbeat": latest_dag_processor_heartbeat,
        },
    }

    return airflow_health_status
