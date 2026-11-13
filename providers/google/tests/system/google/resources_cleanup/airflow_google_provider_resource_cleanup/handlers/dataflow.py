#
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

import subprocess

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import curl, get_resource_path, run_command_async

FINISHED_JOB_STATUSES = [
    "JOB_STATE_CANCELLED",
    "JOB_STATE_DRAINED",
    "JOB_STATE_DONE",
    "JOB_STATE_FAILED",
    "JOB_STATE_UPDATED",
]


async def _archive_dataflow_job(resource: dict, log_prefix: str):
    job_path = get_resource_path(resource)
    url = (
        f"https://dataflow.googleapis.com/v1b3/{job_path}/"
        "?updateMask=job_metadata.user_display_properties.archived"
    )
    await curl(
        url,
        log_prefix=log_prefix,
        method="PUT",
        data={"jobMetadata": {"userDisplayProperties": {"archived": "true"}}},
    )


async def _delete_dataflow_job(resource: dict, log_prefix: str):
    state = resource.get("state")
    location = resource.get("location")
    job_id = get_resource_path(resource).split("/")[-1]
    if state in FINISHED_JOB_STATUSES:
        await _archive_dataflow_job(resource, log_prefix)
        return

    cmd = f"gcloud dataflow jobs cancel {job_id} --region={location} --quiet"
    return_code = await run_command_async(cmd, log_prefix)
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


class DataflowDeleteHandler(BaseDeleteHandler):
    SEMAPHORE_COUNT = 1
    SLEEP_AFTER_EACH_REQUEST = 1

    DELETERS = {"dataflow.googleapis.com/Job": _delete_dataflow_job}

    DELETION_ORDER = [
        "dataflow.googleapis.com/Job",
    ]
