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
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


async def _delete_database(resource: dict, log_prefix: str):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[-3]
    database = path_parts[-1]
    cmd = f"gcloud spanner databases delete {database} --instance={instance} --quiet"
    await run_command_async(cmd, log_prefix)


async def _delete_backup(resource: dict, log_prefix: str):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[-3]
    backup = path_parts[-1]
    cmd = f"gcloud spanner backups delete {backup} --instance={instance} --quiet"
    await run_command_async(cmd, log_prefix)


async def _delete_instance(resource: dict, log_prefix: str):
    instance = get_resource_path(resource).split("/")[-1]
    cmd = f"gcloud spanner instances delete {instance} --quiet"
    await run_command_async(cmd, log_prefix)


async def _delete_instance_config(resource: dict, log_prefix: str):
    instance_config = get_resource_path(resource).split("/")[-1]
    cmd = f"gcloud spanner instance-configs delete {instance_config} --quiet"
    await run_command_async(cmd, log_prefix)


async def _delete_instance_partition(resource: dict, log_prefix: str):
    path_parts = get_resource_path(resource).split("/")
    instance = path_parts[-3]
    instance_partition = path_parts[-1]
    cmd = f"gcloud spanner instance-partitions delete {instance_partition} --instance={instance} --quiet"
    await run_command_async(cmd, log_prefix)


class SpannerDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "spanner.googleapis.com/Database": _delete_database,
        "spanner.googleapis.com/Backup": _delete_backup,
        "spanner.googleapis.com/Instance": _delete_instance,
        "spanner.googleapis.com/InstanceConfig": _delete_instance_config,
        "spanner.googleapis.com/InstancePartition": _delete_instance_partition,
    }

    DELETION_ORDER = [
        "spanner.googleapis.com/Backup",
        "spanner.googleapis.com/Database",
        "spanner.googleapis.com/InstanceConfig",
        "spanner.googleapis.com/InstancePartition",
        "spanner.googleapis.com/Instance",
    ]
