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
from airflow_google_provider_resource_cleanup.helpers import curl, get_resource_path

API_BASE = "https://dataform.googleapis.com/v1/"


async def _delete_dataform_resource(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    await curl(f"{API_BASE}{name}", log_prefix=log_prefix)


async def _delete_dataform_repository(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    await curl(f"{API_BASE}{name}?force=true", log_prefix=log_prefix)


class DataformDeleteHandler(BaseDeleteHandler):
    SEMAPHORE_COUNT = 1
    SLEEP_AFTER_EACH_REQUEST = 1

    DELETERS = {
        "dataform.googleapis.com/Workspace": _delete_dataform_resource,
        "dataform.googleapis.com/WorkflowInvocation": _delete_dataform_resource,
        "dataform.googleapis.com/WorkflowConfig": _delete_dataform_resource,
        "dataform.googleapis.com/ReleaseConfig": _delete_dataform_resource,
        "dataform.googleapis.com/Repository": _delete_dataform_repository,
    }

    DELETION_ORDER = [
        "dataform.googleapis.com/WorkflowInvocation",
        "dataform.googleapis.com/WorkflowConfig",
        "dataform.googleapis.com/ReleaseConfig",
        "dataform.googleapis.com/Workspace",
        "dataform.googleapis.com/Repository",
    ]
