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

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, endpoint, schema

from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun, TIRunContext


class AddTeamNameField(VersionChange):
    """Add the ``team_name`` field to DagRun model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(DagRun).field("team_name").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_team_name_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove the ``team_name`` field from dag_run for older API versions."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("team_name", None)


class AddStateEndpoints(VersionChange):
    """Add task state and asset state CRUD endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/state/ti/{task_instance_id}/{key}", ["GET"]).didnt_exist,
        endpoint("/state/ti/{task_instance_id}/{key}", ["PUT"]).didnt_exist,
        endpoint("/state/ti/{task_instance_id}/{key}", ["DELETE"]).didnt_exist,
        endpoint("/state/ti/{task_instance_id}", ["DELETE"]).didnt_exist,
        endpoint("/state/asset/by-name/value", ["GET"]).didnt_exist,
        endpoint("/state/asset/by-name/value", ["PUT"]).didnt_exist,
        endpoint("/state/asset/by-name/value", ["DELETE"]).didnt_exist,
        endpoint("/state/asset/by-name/clear", ["DELETE"]).didnt_exist,
        endpoint("/state/asset/by-uri/value", ["GET"]).didnt_exist,
        endpoint("/state/asset/by-uri/value", ["PUT"]).didnt_exist,
        endpoint("/state/asset/by-uri/value", ["DELETE"]).didnt_exist,
        endpoint("/state/asset/by-uri/clear", ["DELETE"]).didnt_exist,
    )
