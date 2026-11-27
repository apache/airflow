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

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import (
    build_project_flag,
    get_resource_path,
    run_command_async,
)


async def _delete_bucket(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    name = get_resource_path(resource).split("/")[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud storage rm --recursive gs://{name}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_transfer_job(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    if resource.get("state") == "DELETED":
        print(f"{log_prefix}Transfer job {get_resource_path(resource)} is already deleted. Skipping.")
        return False
    transfer_job_name = get_resource_path(resource).split("/")[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud transfer jobs delete {transfer_job_name}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


class StorageDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "storage.googleapis.com/Bucket": _delete_bucket,
        "storagetransfer.googleapis.com/TransferJob": _delete_transfer_job,
    }

    DELETION_ORDER = [
        "storagetransfer.googleapis.com/TransferJob",
        "storage.googleapis.com/Bucket",
    ]
