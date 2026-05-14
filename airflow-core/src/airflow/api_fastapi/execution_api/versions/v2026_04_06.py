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

from typing import Any

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, endpoint, schema

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.execution_api.datamodels.asset_event import (
    AssetEventResponse,
    AssetEventsResponse,
    DagRunAssetReference,
)
from airflow.api_fastapi.execution_api.datamodels.dagrun import TriggerDAGRunPayload
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun,
    TIDeferredStatePayload,
    TIRunContext,
)


class AddPartitionKeyField(VersionChange):
    """Add the `partition_key` field to DagRun model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagRun).field("partition_key").didnt_exist,
        schema(AssetEventResponse).field("partition_key").didnt_exist,
        schema(TriggerDAGRunPayload).field("partition_key").didnt_exist,
        schema(DagRunAssetReference).field("partition_key").didnt_exist,
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


class MovePreviousRunEndpoint(VersionChange):
    """Add new previous-run endpoint and migrate old endpoint."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/dag-runs/previous", ["GET"]).didnt_exist,
        endpoint("/dag-runs/{dag_id}/previous", ["GET"]).existed,
    )


class AddDagRunDetailEndpoint(VersionChange):
    """Add dag run detail endpoint."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/dag-runs/{dag_id}/{run_id}", ["GET"]).didnt_exist,
    )


class MakeDagRunStartDateNullable(VersionChange):
    """Make DagRun.start_date field nullable for runs that haven't started yet."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(DagRun).field("start_date").had(type=UtcDateTime),)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def ensure_start_date_in_ti_run_context(response: ResponseInfo) -> None:  # type: ignore[misc]
        """
        Ensure start_date is never None in DagRun for previous API versions.

        Older Task SDK clients expect start_date to be non-nullable. When the
        DagRun hasn't started yet (e.g. queued), fall back to run_after.
        """
        dag_run = response.body.get("dag_run")
        if isinstance(dag_run, dict) and dag_run.get("start_date") is None:
            dag_run["start_date"] = dag_run.get("run_after")

    @convert_response_to_previous_version_for(DagRun)  # type: ignore[arg-type]
    def ensure_start_date_in_dag_run(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Ensure start_date is never None in direct DagRun responses for previous API versions."""
        if response.body.get("start_date") is None:
            response.body["start_date"] = response.body.get("run_after")


class ModifyDeferredTaskKwargsToJsonValue(VersionChange):
    """Change the types of `trigger_kwargs` and `next_kwargs` in TIDeferredStatePayload to JsonValue."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIDeferredStatePayload).field("trigger_kwargs").had(type=dict[str, Any] | str),
        schema(TIDeferredStatePayload).field("next_kwargs").had(type=dict[str, Any]),
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def convert_next_kwargs_to_base_serialization(response: ResponseInfo) -> None:  # type: ignore[misc]
        """
        Convert next_kwargs from SDK serde format to BaseSerialization format for old workers.

        Old workers (task-sdk < 1.2) only know BaseSerialization.deserialize(), which requires
        dicts wrapped as {"__type": "dict", "__var": {...}}. SDK serde produces plain dicts that
        BaseSerialization cannot parse, causing KeyError on __var.

        We must deserialize SDK serde first to recover native Python objects (datetime,
        timedelta, etc.), then re-serialize with BaseSerialization so old workers get
        proper typed values instead of raw {"__classname__": ...} dicts.
        """
        next_kwargs = response.body.get("next_kwargs")
        if next_kwargs is None:
            return

        from airflow.sdk.serde import deserialize
        from airflow.serialization.serialized_objects import BaseSerialization

        try:
            plain = deserialize(next_kwargs)
        except (ImportError, KeyError, AttributeError, TypeError):
            # Already in BaseSerialization format (rolling upgrade, old data in DB)
            return

        response.body["next_kwargs"] = BaseSerialization.serialize(plain)


class RemoveUpstreamMapIndexesField(VersionChange):
    """Remove upstream_map_indexes field from TIRunContext - now computed by Task SDK."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext)
        .field("upstream_map_indexes")
        .existed_as(type=dict[str, int | list[int] | None] | None),
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def add_upstream_map_indexes_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Add upstream_map_indexes field with None for older API versions."""
        response.body["upstream_map_indexes"] = None


class AddNoteField(VersionChange):
    """Add note parameter to DagRun Model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(DagRun).field("note").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_note_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove note field for older API versions."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("note", None)


class AddTaskInstanceStartDateField(VersionChange):
    """Add `start_date` field to TIRunContext."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("start_date").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_start_date_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove start_date field for older API versions."""
        response.body.pop("start_date", None)


class AddDagEndpoint(VersionChange):
    """Add the `/dags/{dag_id}` endpoint."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (endpoint("/dags/{dag_id}", ["GET"]).didnt_exist,)
