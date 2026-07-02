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
from airflow_google_provider_resource_cleanup.helpers import curl, get_resource_path, run_command_async


async def _delete_instance(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    project_id = name.split("/")[1]
    cmd = f"gcloud sql instances delete {name} --project={project_id} --quiet"
    await run_command_async(cmd, log_prefix)


async def _delete_backup(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    project_id = name.split("/")[1]
    cmd = f"gcloud sql backups delete {name} --project={project_id} --quiet"
    await run_command_async(cmd, log_prefix)


async def _delete_backup_run(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    url = f"https://sqladmin.googleapis.com/sql/v1beta4/{name}"
    await curl(url, log_prefix=log_prefix)


class CloudSQLDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "sqladmin.googleapis.com/Backup": _delete_backup,
        "sqladmin.googleapis.com/Instance": _delete_instance,
        "sqladmin.googleapis.com/BackupRun": _delete_backup_run,
    }
    DELETION_ORDER = [
        "sqladmin.googleapis.com/BackupRun",
        "sqladmin.googleapis.com/Backup",
        "sqladmin.googleapis.com/Instance",
    ]
