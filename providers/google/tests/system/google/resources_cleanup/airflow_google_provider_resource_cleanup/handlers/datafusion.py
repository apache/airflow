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
from airflow_google_provider_resource_cleanup.helpers import curl, get_resource_path, run_command_async

API_BASE = "https://datafusion.googleapis.com/v1/"


async def _delete_dns_peering(resource: dict, log_prefix: str):
    url = f"{API_BASE}{get_resource_path(resource)}"
    await curl(url, log_prefix=log_prefix)


async def _delete_instance(resource: dict, log_prefix: str):
    path_parts = get_resource_path(resource).split("/")
    project_id = path_parts[1]
    location = path_parts[3]
    instance_id = path_parts[-1]
    cmd = (
        f"gcloud beta data-fusion instances delete {instance_id} "
        f"--location={location} --project={project_id} --quiet"
    )
    await run_command_async(cmd, log_prefix)


class DataFusionDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "datafusion.googleapis.com/DnsPeering": _delete_dns_peering,
        "datafusion.googleapis.com/Instance": _delete_instance,
    }

    DELETION_ORDER = [
        "datafusion.googleapis.com/DnsPeering",
        "datafusion.googleapis.com/Instance",
    ]
