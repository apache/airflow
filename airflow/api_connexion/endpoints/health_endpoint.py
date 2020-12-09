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
from airflow.api_connexion.schemas.health_schema import health_schema
from airflow.jobs.scheduler_job import SchedulerJob

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"


def get_health():
    """Return the health of the airflow scheduler and metadatabase"""
    metadatabase_status = HEALTHY
    latest_scheduler_heartbeat = None
    scheduler_status = UNHEALTHY
    try:
        scheduler_job = SchedulerJob.most_recent_job()

        if scheduler_job:
            latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
            if scheduler_job.is_alive():
                scheduler_status = HEALTHY
    except Exception:  # pylint: disable=broad-except
        metadatabase_status = UNHEALTHY

    payload = {
        "metadatabase": {"status": metadatabase_status},
        "scheduler": {
            "status": scheduler_status,
            "latest_scheduler_heartbeat": latest_scheduler_heartbeat,
        },
    }

    return health_schema.dump(payload)
