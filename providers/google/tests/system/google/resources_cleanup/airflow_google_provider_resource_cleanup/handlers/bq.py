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


async def _delete_transfer_config(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    cmd = f"bq rm -f --transfer_config {name}"
    await run_command_async(cmd, log_prefix)


async def _delete_table(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    _, project_id, _, dataset, _, table = name.split("/")
    cmd = f"bq rm -f -t {project_id}:{dataset}.{table}"
    await run_command_async(cmd, log_prefix)


async def _delete_dataset(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    _, project_id, _, dataset = name.split("/")
    cmd = f"bq rm -r -f -d {project_id}:{dataset}"
    await run_command_async(cmd, log_prefix)


async def _delete_model(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    _, project_id, _, dataset, _, model = name.split("/")
    cmd = f"bq rm -f --model {project_id}:{dataset}.{model}"
    await run_command_async(cmd, log_prefix)


class BigQueryDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "bigquery.googleapis.com/Dataset": _delete_dataset,
        "bigquery.googleapis.com/Model": _delete_model,
        "bigquery.googleapis.com/Table": _delete_table,
        "bigquerydatatransfer.googleapis.com/TransferConfig": _delete_transfer_config,
    }

    DELETION_ORDER = [
        "bigquerydatatransfer.googleapis.com/TransferConfig",
        "bigquery.googleapis.com/Table",
        "bigquery.googleapis.com/Model",
        "bigquery.googleapis.com/Dataset",
    ]
