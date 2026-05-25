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


async def _delete_dlp_job(resource: dict, log_prefix: str):
    job_id = get_resource_path(resource)
    cmd_delete = f"gcloud alpha dlp jobs delete {job_id.split('/')[-1]} --quiet"
    await run_command_async(cmd_delete, log_prefix)


async def _delete_dlp_job_trigger(resource: dict, log_prefix: str):
    job_id = get_resource_path(resource)
    cmd_delete = f"gcloud alpha dlp job-triggers delete {job_id.split('/')[-1]} --quiet"
    await run_command_async(cmd_delete, log_prefix)


class DLPDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "dlp.googleapis.com/DlpJob": _delete_dlp_job,
        "dlp.googleapis.com/JobTrigger": _delete_dlp_job_trigger,
    }

    DELETION_ORDER = [
        "dlp.googleapis.com/DlpJob",
        "dlp.googleapis.com/JobTrigger",
    ]
