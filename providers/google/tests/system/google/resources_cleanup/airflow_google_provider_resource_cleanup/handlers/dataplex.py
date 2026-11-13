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
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


async def _run_delete_command(cmd: str, log_prefix: str):
    return_code = await run_command_async(cmd, log_prefix)
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


async def _delete_entry_group(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, name = path.split("/")
    if name.startswith("@"):
        print(
            f"{log_prefix}System entry group {name} is managed by Dataplex and cannot be deleted. Skipping."
        )
        return False
    cmd = f"gcloud dataplex entry-groups delete {name} --location={location} --project={project_id} --quiet"
    await _run_delete_command(cmd, log_prefix)


async def _delete_asset(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, lake, _, zone, _, asset = path.split("/")
    cmd = (
        f"gcloud dataplex assets delete {asset} --location={location} --lake={lake} "
        f"--zone={zone} --project={project_id} --quiet"
    )
    await _run_delete_command(cmd, log_prefix)


async def _delete_lake(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, lake = path.split("/")
    cmd = f"gcloud dataplex lakes delete {lake} --location={location} --project={project_id} --quiet"
    await _run_delete_command(cmd, log_prefix)


async def _delete_task(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, lake, _, task = path.split("/")
    cmd = (
        f"gcloud dataplex tasks delete {task} --location={location} --lake={lake} "
        f"--project={project_id} --quiet"
    )
    await _run_delete_command(cmd, log_prefix)


async def _delete_zone(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, lake, _, zone = path.split("/")
    cmd = (
        f"gcloud dataplex zones delete {zone} --location={location} --lake={lake} "
        f"--project={project_id} --quiet"
    )
    await _run_delete_command(cmd, log_prefix)


class DataplexDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "dataplex.googleapis.com/EntryGroup": _delete_entry_group,
        "dataplex.googleapis.com/Asset": _delete_asset,
        "dataplex.googleapis.com/Lake": _delete_lake,
        "dataplex.googleapis.com/Task": _delete_task,
        "dataplex.googleapis.com/Zone": _delete_zone,
    }

    DELETION_ORDER = [
        "dataplex.googleapis.com/EntryGroup",
        "dataplex.googleapis.com/Asset",
        "dataplex.googleapis.com/Task",
        "dataplex.googleapis.com/Zone",
        "dataplex.googleapis.com/Lake",
    ]
