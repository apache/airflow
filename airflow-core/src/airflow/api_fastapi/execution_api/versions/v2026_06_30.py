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
    convert_response_to_previous_version_for,
    endpoint,
    schema,
)

from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun,
    TaskInstance,
    TIAwaitingInputStatePayload,
    TIRetryStatePayload,
    TIRunContext,
)


class AddVariableKeysEndpoint(VersionChange):
    """Add GET /variables/keys endpoint for listing variable keys with optional prefix filter."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (endpoint("/variables/keys", ["GET"]).didnt_exist,)


class AddConnectionTestEndpoint(VersionChange):
    """Add connection-tests endpoints for the async connection-test workflow."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/connection-tests/{connection_test_id}", ["PATCH"]).didnt_exist,
        endpoint("/connection-tests/{connection_test_id}/connection", ["GET"]).didnt_exist,
    )


class AddTaskInstanceQueueField(VersionChange):
    """Add the `queue` field to the TaskInstance model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TaskInstance).field("queue").didnt_exist,)


class AddAwaitingInputStatePayload(VersionChange):
    """Add the awaiting_input task instance state transition payload (Human-in-the-loop, no trigger)."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIAwaitingInputStatePayload).field("state").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("timeout").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("next_method").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("next_kwargs").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("rendered_map_index").didnt_exist,
    )


class AddRetryPolicyFields(VersionChange):
    """Add retry_delay_seconds and retry_reason fields to TIRetryStatePayload for pluggable retry policies."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRetryStatePayload).field("retry_delay_seconds").didnt_exist,
        schema(TIRetryStatePayload).field("retry_reason").didnt_exist,
    )


class AddTeamNameField(VersionChange):
    """Add the ``team_name`` field to DagRun model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(DagRun).field("team_name").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_team_name_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove the ``team_name`` field from dag_run for older API versions."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("team_name", None)


class AddAssetsByAliasEndpoint(VersionChange):
    """Add endpoint to resolve assets from an AssetAlias."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (endpoint("/assets/by-alias", ["GET"]).didnt_exist,)


class AddTaskAndAssetStateStoreEndpoints(VersionChange):
    """Add task state store and asset state store API endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/store/ti/{task_instance_id}/{key}", ["GET"]).didnt_exist,
        endpoint("/store/ti/{task_instance_id}/{key}", ["PUT"]).didnt_exist,
        endpoint("/store/ti/{task_instance_id}/{key}", ["DELETE"]).didnt_exist,
        endpoint("/store/ti/{task_instance_id}", ["DELETE"]).didnt_exist,
        endpoint("/store/asset/by-name/value", ["GET"]).didnt_exist,
        endpoint("/store/asset/by-name/value", ["PUT"]).didnt_exist,
        endpoint("/store/asset/by-name/value", ["DELETE"]).didnt_exist,
        endpoint("/store/asset/by-name/clear", ["DELETE"]).didnt_exist,
        endpoint("/store/asset/by-uri/value", ["GET"]).didnt_exist,
        endpoint("/store/asset/by-uri/value", ["PUT"]).didnt_exist,
        endpoint("/store/asset/by-uri/value", ["DELETE"]).didnt_exist,
        endpoint("/store/asset/by-uri/clear", ["DELETE"]).didnt_exist,
    )


class AddPartitionDateField(VersionChange):
    """Expose the consumer DagRun's partition datetime on the execution API so consumer tasks can template it."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(DagRun).field("partition_date").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_partition_date_from_dag_run(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``partition_date`` from the nested ``dag_run`` payload for older clients."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("partition_date", None)


class AddArgBindingsToTIRunContext(VersionChange):
    """Add the ``arg_bindings`` positional-argument binding spec for stub (foreign-runtime) tasks."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("arg_bindings").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_arg_bindings_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``arg_bindings`` from the run context for older clients."""
        response.body.pop("arg_bindings", None)
