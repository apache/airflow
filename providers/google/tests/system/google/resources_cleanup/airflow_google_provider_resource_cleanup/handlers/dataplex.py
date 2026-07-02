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


async def _delete_entry_group(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, name = path.split("/")
    cmd = f"gcloud dataplex entry-groups delete {name} --location={location} --project={project_id}"
    await run_command_async(cmd, log_prefix)


async def _delete_lake(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, lake = path.split("/")
    cmd = f"gcloud dataplex lakes delete {lake} --location={location}"
    await run_command_async(cmd, log_prefix)


async def _delete_task(resource: dict, log_prefix: str):
    location = resource.get("location")
    path = get_resource_path(resource)
    _, project_id, _, _, _, lake, _, task = path.split("/")
    cmd = f"gcloud dataplex tasks delete {task} --location={location} --lake={lake}"
    await run_command_async(cmd, log_prefix)


class DataplexDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "dataplex.googleapis.com/EntryGroup": _delete_entry_group,
        "dataplex.googleapis.com/Lake": _delete_lake,
        "dataplex.googleapis.com/Task": _delete_task,
    }

    DELETION_ORDER = [
        "dataplex.googleapis.com/EntryGroup",
        "dataplex.googleapis.com/Task",
        "dataplex.googleapis.com/Lake",
    ]
