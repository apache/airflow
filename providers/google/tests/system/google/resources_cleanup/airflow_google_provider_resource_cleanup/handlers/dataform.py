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
from airflow_google_provider_resource_cleanup.helpers import curl, get_resource_path

API_BASE = "https://dataform.googleapis.com/v1/"


async def _delete_dataform_workflow_invocation(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    url_delete = f"{API_BASE}{name}"
    await curl(url_delete, log_prefix)


async def _delete_dataform_workspace(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    url_delete = f"{API_BASE}{name}"
    await curl(url_delete, log_prefix)


async def _delete_dataform_workflow_config(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    url_delete = f"{API_BASE}{name}"
    await curl(url_delete, log_prefix)


async def _delete_dataform_release_config(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    url_delete = f"{API_BASE}{name}"
    await curl(url_delete, log_prefix)


async def _delete_dataform_repository(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    url_delete = f"{API_BASE}{name}"
    await curl(url_delete, log_prefix)


class DataformDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "dataform.googleapis.com/Workspace": _delete_dataform_workspace,
        "dataform.googleapis.com/WorkflowInvocation": _delete_dataform_workflow_invocation,
        "dataform.googleapis.com/WorkflowConfig": _delete_dataform_workflow_config,
        "dataform.googleapis.com/ReleaseConfig": _delete_dataform_release_config,
        "dataform.googleapis.com/Repository": _delete_dataform_repository,
    }

    DELETION_ORDER = [
        "dataform.googleapis.com/WorkflowInvocation",
        "dataform.googleapis.com/Workspace",
        "dataform.googleapis.com/ReleaseConfig",
        "dataform.googleapis.com/WorkflowConfig",
        "dataform.googleapis.com/Repository",
    ]
