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
from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


async def _delete_bucket(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    cmd = f"gcloud storage rm --recursive gs://{name}"
    await run_command_async(cmd, log_prefix)


async def _delete_transfer_job(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    _, project_id, _, transfer_job_name = name.split("/")
    cmd = f"gcloud transfer jobs delete {transfer_job_name} --project {project_id}"
    await run_command_async(cmd, log_prefix)


class StorageDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "storage.googleapis.com/Bucket": _delete_bucket,
        "storagetransfer.googleapis.com/TransferJob": _delete_transfer_job,
    }

    DELETION_ORDER = [
        "storagetransfer.googleapis.com/TransferJob",
        "storage.googleapis.com/Bucket",
    ]
