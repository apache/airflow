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

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

from airflow.api_fastapi.execution_api.datamodels.asset_event import AssetEventResponse, AssetEventsResponse
from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun, TIRunContext


class AddPartitionKeyField(VersionChange):
    """Add the `partition_key` field to DagRun model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagRun).field("partition_key").didnt_exist,
        schema(AssetEventResponse).field("partition_key").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_partition_key_from_dag_run(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove the `partition_key` field from the dag_run object when converting to the previous version."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("partition_key", None)

    @convert_response_to_previous_version_for(AssetEventsResponse)  # type: ignore[arg-type]
    def remove_partition_key_from_asset_events(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove the `partition_key` field from the dag_run object when converting to the previous version."""
        events = response.body["asset_events"]
        for elem in events:
            elem.pop("partition_key", None)
