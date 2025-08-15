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
from airflow.api_fastapi.execution_api.routes.xcoms import GetXComSliceFilterParams


class AddDagRunStateFieldAndPreviousEndpoint(VersionChange):
    """Add the `state` field to DagRun model and `/dag-runs/{dag_id}/previous` endpoint."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagRun).field("state").didnt_exist,
        endpoint("/dag-runs/{dag_id}/previous", ["GET"]).didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_state_from_dag_run(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove the `state` field from the dag_run object when converting to the previous version."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("state", None)


class AddIncludePriorDatesToGetXComSlice(VersionChange):
    """Add the `include_prior_dates` field to GetXComSliceFilterParams."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(GetXComSliceFilterParams).field("include_prior_dates").didnt_exist,
    )
