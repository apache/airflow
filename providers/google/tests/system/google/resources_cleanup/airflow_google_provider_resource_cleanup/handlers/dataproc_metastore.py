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


def _extract_service_params(resource: dict) -> tuple[str, str]:
    path_parts = get_resource_path(resource).split("/")
    location = resource.get("location") or path_parts[3]
    service = path_parts[-1]
    return service, location


def _extract_backup_params(resource: dict) -> tuple[str, str, str]:
    path_parts = get_resource_path(resource).split("/")
    location = resource.get("location") or path_parts[3]
    service = path_parts[-3]
    backup = path_parts[-1]
    return backup, service, location


async def _delete_dataproc_metastore_service(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    service, location = _extract_service_params(resource)
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud metastore services delete {service} --location={location}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_dataproc_metastore_backup(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    backup, service, location = _extract_backup_params(resource)
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = (
        f"gcloud metastore services backups delete {backup} "
        f"--service={service} --location={location}{project_flag} --quiet"
    )
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


class DataprocMetastoreDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "metastore.googleapis.com/Service": _delete_dataproc_metastore_service,
        "metastore.googleapis.com/Backup": _delete_dataproc_metastore_backup,
    }

    DELETION_ORDER = [
        "metastore.googleapis.com/Backup",
        "metastore.googleapis.com/Service",
    ]
