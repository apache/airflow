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

from cadwyn import (
    ResponseInfo,
    VersionChange,
    VersionChangeWithSideEffects,
    convert_response_to_previous_version_for,
    schema,
)

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.execution_api.datamodels.asset_event import (
    AssetEventsResponse,
    DagRunAssetReference,
)
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext


class AddArgBindingsToTIRunContext(VersionChangeWithSideEffects):
    """Add the ``arg_bindings`` argument-binding spec for stub (foreign-runtime) tasks."""

    description = __doc__

    # A side-effect change, not just a schema one, so ti_run can gate the server-side spec
    # derivation on ``is_applied``: clients older than this version never receive the field.
    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("arg_bindings").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_arg_bindings_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``arg_bindings`` from the run context for older clients."""
        response.body.pop("arg_bindings", None)


class MakeAssetEventDagRunStartDateNullable(VersionChange):
    """Make DagRunAssetReference.start_date nullable for runs that have not started yet."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagRunAssetReference).field("start_date").had(type=UtcDateTime),
        schema(DagRunAssetReference).field("run_after").didnt_exist,
    )

    @convert_response_to_previous_version_for(AssetEventsResponse)  # type: ignore[arg-type]
    def ensure_start_date_in_created_dagruns(response: ResponseInfo) -> None:  # type: ignore[misc]
        """
        Keep created_dagruns[*].start_date non-null for previous API versions.

        Clients of those versions declare the field non-nullable and reject the whole response
        when it is null, which happens while a created Dag run is queued or has been cleared.
        Fall back to run_after, then drop the field those clients never knew about.
        """
        for event in response.body.get("asset_events") or ():
            if not isinstance(event, dict):
                continue
            for dag_run in event.get("created_dagruns") or ():
                if not isinstance(dag_run, dict):
                    continue
                if dag_run.get("start_date") is None:
                    dag_run["start_date"] = dag_run.get("run_after")
                dag_run.pop("run_after", None)
