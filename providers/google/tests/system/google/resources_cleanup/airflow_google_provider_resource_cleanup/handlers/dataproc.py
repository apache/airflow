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
from airflow_google_provider_resource_cleanup.helpers import run_command_async


def __extract_params(resource: dict) -> tuple[str, str]:
    """
    Extract the location and name of the resource.
    """
    region = resource.get("location")
    if not isinstance(region, str):
        raise TypeError("Dataproc resource location must be a string.")
    name = resource.get("name", "").split("/")[-1]
    return name, region


async def __run_delete_cmd(asset_type: str, resource: dict, prefix: str):
    """
    Create and run the command for the given asset_type and resource information
    """
    name, region = __extract_params(resource)
    cmd = f"gcloud dataproc {asset_type} delete {name} --quiet --region={region}"
    await run_command_async(cmd, log_prefix=prefix)


async def _delete_job(resource: dict, prefix: str):
    await __run_delete_cmd("jobs", resource, prefix)


async def _delete_batch(resource: dict, prefix: str):
    await __run_delete_cmd("batches", resource, prefix)


async def _delete_cluster(resource: dict, prefix: str):
    await __run_delete_cmd("clusters", resource, prefix)


async def _delete_workflow_template(resource: dict, prefix: str):
    await __run_delete_cmd("workflow-templates", resource, prefix)


class DataprocDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "dataproc.googleapis.com/Job": _delete_job,
        "dataproc.googleapis.com/Batch": _delete_batch,
        "dataproc.googleapis.com/Cluster": _delete_cluster,
        "dataproc.googleapis.com/WorkflowTemplate": _delete_workflow_template,
    }

    DELETION_ORDER = [
        "dataproc.googleapis.com/Job",
        "dataproc.googleapis.com/Batch",
        "dataproc.googleapis.com/Cluster",
        "dataproc.googleapis.com/WorkflowTemplate",
    ]
