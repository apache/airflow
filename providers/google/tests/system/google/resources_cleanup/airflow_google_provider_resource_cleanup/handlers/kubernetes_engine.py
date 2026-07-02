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


async def _delete_kubernetes_engine_custer(resource: dict, log_prefix: str):
    location = resource.get("location")
    name = get_resource_path(resource)
    cmd = f"gcloud container clusters delete {name} --location={location} --quiet"
    await run_command_async(cmd, log_prefix)


class GKEDeleteHandler(BaseDeleteHandler):
    DELETERS = {"container.googleapis.com/Cluster": _delete_kubernetes_engine_custer}

    DELETION_ORDER = [
        "container.googleapis.com/Cluster",
    ]
