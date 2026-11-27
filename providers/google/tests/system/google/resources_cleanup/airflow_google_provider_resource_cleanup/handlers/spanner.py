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


async def _delete_backup_schedule(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[3]
    database = path_parts[5]
    backup_schedule = path_parts[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = (
        f"gcloud spanner backup-schedules delete {backup_schedule} "
        f"--database={database} --instance={instance}{project_flag} --quiet"
    )
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_database(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[-3]
    database = path_parts[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud spanner databases delete {database} --instance={instance}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_backup(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[-3]
    backup = path_parts[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud spanner backups delete {backup} --instance={instance}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_instance(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    instance = get_resource_path(resource).split("/")[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud spanner instances delete {instance}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_instance_config(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    instance_config = get_resource_path(resource).split("/")[-1]
    if not instance_config.startswith("custom-"):
        print(f"{log_prefix}Managed Spanner instance config {instance_config} cannot be deleted. Skipping.")
        return False
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = f"gcloud spanner instance-configs delete {instance_config}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_instance_partition(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[-3]
    instance_partition = path_parts[-1]
    project_flag = build_project_flag(resource, project_id=project_id)
    cmd = (
        f"gcloud spanner instance-partitions delete {instance_partition} "
        f"--instance={instance}{project_flag} --quiet"
    )
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


class SpannerDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "spanner.googleapis.com/BackupSchedule": _delete_backup_schedule,
        "spanner.googleapis.com/Database": _delete_database,
        "spanner.googleapis.com/Backup": _delete_backup,
        "spanner.googleapis.com/Instance": _delete_instance,
        "spanner.googleapis.com/InstanceConfig": _delete_instance_config,
        "spanner.googleapis.com/InstancePartition": _delete_instance_partition,
    }

    DELETION_ORDER = [
        "spanner.googleapis.com/BackupSchedule",
        "spanner.googleapis.com/Backup",
        "spanner.googleapis.com/Database",
        "spanner.googleapis.com/InstancePartition",
        "spanner.googleapis.com/Instance",
        "spanner.googleapis.com/InstanceConfig",
    ]
