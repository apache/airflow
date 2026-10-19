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

import contextlib
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
    """Add ``partition_key`` to the Dag run, asset event, asset reference and trigger payload models."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagRun).field("partition_key").didnt_exist,
        schema(AssetEventResponse).field("partition_key").didnt_exist,
        schema(TriggerDAGRunPayload).field("partition_key").didnt_exist,
        schema(DagRunAssetReference).field("partition_key").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_partition_key_from_dag_run(response: ResponseInfo) -> None:
        """Remove the `partition_key` field from the dag_run object when converting to the previous version."""
        dag_run = response.body.get("dag_run")
        if isinstance(dag_run, dict):
            dag_run.pop("partition_key", None)

    @convert_response_to_previous_version_for(AssetEventsResponse)  # type: ignore[arg-type]
    def remove_partition_key_from_asset_events(response: ResponseInfo) -> None:
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
    def ensure_start_date_in_ti_run_context(response: ResponseInfo) -> None:
        """
        Ensure start_date is never None in DagRun for previous API versions.

        Older Task SDK clients expect start_date to be non-nullable. When the
        DagRun hasn't started yet (e.g. queued), fall back to run_after.
        """
        dag_run = response.body.get("dag_run")
        if isinstance(dag_run, dict) and dag_run.get("start_date") is None:
            dag_run["start_date"] = dag_run.get("run_after")

    @convert_response_to_previous_version_for(DagRun)  # type: ignore[arg-type]
    def ensure_start_date_in_dag_run(response: ResponseInfo) -> None:
        """Ensure start_date is never None in direct DagRun responses for previous API versions."""
        if response.body.get("start_date") is None:
            response.body["start_date"] = response.body.get("run_after")


# serde class names whose deserializers rebuild plain value types. Everything else (attrs,
# dataclasses, pydantic models, classes with their own ``deserialize``) is rebuilt by calling into
# the named class.
_BUILTIN_VALUE_CLASSNAMES = frozenset(
    {
        "builtins.frozenset",
        "builtins.set",
        "builtins.tuple",
        "datetime.date",
        "datetime.datetime",
        "datetime.timedelta",
        "decimal.Decimal",
        "pendulum.date.Date",
        "pendulum.datetime.DateTime",
        "pendulum.tz.timezone.FixedTimezone",
        "pendulum.tz.timezone.Timezone",
        "uuid.UUID",
        "zoneinfo.ZoneInfo",
    }
)


def _holds_only_builtin_values(value: Any) -> bool:
    """
    Whether serde ``deserialize`` would rebuild nothing but built-in value types from an encoded value.

    Mirrors how the decoder reads each dict, and fails safe: a dict it could misread, such as one
    holding envelope keys alongside other keys, counts as not built-in.
    """
    from airflow._shared.serialization import (
        CLASSNAME,
        DATA,
        OLD_DATA,
        OLD_DICT,
        OLD_TYPE,
        OLD_TYPE_TO_FULL_QUALNAME,
        VERSION,
    )

    reserved_keys = {CLASSNAME, VERSION, DATA, OLD_TYPE, OLD_DATA}

    def is_plain_builtin_dict(d: Any) -> bool:
        return (
            isinstance(d, dict)
            and not d.keys() & reserved_keys
            and all(_holds_only_builtin_values(item) for item in d.values())
        )

    if isinstance(value, list):
        return all(_holds_only_builtin_values(item) for item in value)
    if not isinstance(value, dict):
        return True
    if _is_legacy_envelope(value):
        if value[OLD_TYPE] == OLD_DICT:
            # The decoder unwraps this envelope without checking the inner dict for another one.
            return is_plain_builtin_dict(value[OLD_DATA])
        return OLD_TYPE_TO_FULL_QUALNAME.get(
            value[OLD_TYPE]
        ) in _BUILTIN_VALUE_CLASSNAMES and _holds_only_builtin_values(value[OLD_DATA])
    if CLASSNAME in value:
        return (
            value.keys() == {CLASSNAME, VERSION, DATA}
            and value[CLASSNAME] in _BUILTIN_VALUE_CLASSNAMES
            and _holds_only_builtin_values(value[DATA])
        )
    return is_plain_builtin_dict(value)


_ASSET_CLASSNAME = "airflow.sdk.definitions.asset.Asset"
_ASSET_ALIAS_CLASSNAME = "airflow.sdk.definitions.asset.AssetAlias"
_ASSET_ACCESS_CONTROL_CLASSNAME = "airflow.sdk.definitions.asset.access_control.AssetAccessControl"


class _UnconvertibleNextKwargsError(ValueError):
    """next_kwargs hold a value that cannot be translated to the legacy BaseSerialization format."""

    def __init__(self, what: str) -> None:
        super().__init__(
            f"The deferred task cannot be resumed: its next_kwargs hold {what}, "
            "which cannot be sent to this worker's Task SDK version. Upgrade the Task SDK on the worker."
        )


def _is_legacy_envelope(value: Any) -> bool:
    """Whether a value is a BaseSerialization ``{"__type": ..., "__var": ...}`` envelope."""
    from airflow._shared.serialization import OLD_DATA, OLD_TYPE

    return isinstance(value, dict) and value.keys() == {OLD_TYPE, OLD_DATA}


def _is_well_formed_legacy(value: Any) -> bool:
    """Whether every dict in a BaseSerialization-encoded value is an envelope its decoder can read."""
    from airflow._shared.serialization import OLD_DATA, OLD_DICT, OLD_TYPE
    from airflow.serialization.enums import DagAttributeTypes as DAT

    if isinstance(value, list):
        return all(_is_well_formed_legacy(item) for item in value)
    if not isinstance(value, dict):
        return True
    if not _is_legacy_envelope(value):
        return False
    data = value[OLD_DATA]
    if value[OLD_TYPE] == OLD_DICT:
        return isinstance(data, dict) and all(_is_well_formed_legacy(item) for item in data.values())
    if value[OLD_TYPE] in (DAT.TUPLE, DAT.SET):
        return isinstance(data, list) and _is_well_formed_legacy(data)
    return True


def _convert_to_legacy(value: Any) -> Any:
    """
    Translate a serde-encoded value to the legacy BaseSerialization encoding.

    Assets and asset aliases are translated structurally from their encoded fields, so no class
    beyond the built-in value types is ever instantiated here.
    """
    from airflow._shared.serialization import CLASSNAME, DATA, OLD_TYPE
    from airflow.sdk.serde import deserialize
    from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
    from airflow.serialization.serialized_objects import BaseSerialization

    if _holds_only_builtin_values(value):
        return BaseSerialization.serialize(deserialize(value))
    if isinstance(value, list):
        return [_convert_to_legacy(item) for item in value]
    if CLASSNAME not in value:
        if _is_legacy_envelope(value):
            raise _UnconvertibleNextKwargsError(f"a value of type {str(value[OLD_TYPE])!r}")
        return {
            Encoding.TYPE: DAT.DICT,
            Encoding.VAR: {key: _convert_to_legacy(item) for key, item in value.items()},
        }
    classname = value[CLASSNAME]
    data = value.get(DATA)
    # No frozenset: the legacy format has none, and sending a set instead breaks nesting it in a set.
    # Frozensets of built-in values never get here; they are handled with the other built-ins.
    collection_types = {"builtins.tuple": DAT.TUPLE, "builtins.set": DAT.SET}
    if classname in collection_types and isinstance(data, list):
        items = [_convert_to_legacy(item) for item in data]
        if collection_types[classname] == DAT.SET:
            # Mirrors BaseSerialization.serialize, which sorts set items when they are comparable.
            with contextlib.suppress(TypeError):
                items = sorted(items)
        return {Encoding.TYPE: collection_types[classname], Encoding.VAR: items}
    if classname == _ASSET_CLASSNAME:
        return {Encoding.TYPE: DAT.ASSET, Encoding.VAR: _translate_asset(data)}
    if classname == _ASSET_ALIAS_CLASSNAME and isinstance(data, dict) and data.keys() == {"name", "group"}:
        return {Encoding.TYPE: DAT.ASSET_ALIAS, Encoding.VAR: {"name": data["name"], "group": data["group"]}}
    raise _UnconvertibleNextKwargsError(f"a value of type {str(classname)!r}")


def _is_plain_json(value: Any) -> bool:
    """Whether a serde-encoded value holds only JSON values, with no class envelope anywhere."""
    from airflow._shared.serialization import CLASSNAME

    if isinstance(value, list):
        return all(_is_plain_json(item) for item in value)
    if isinstance(value, dict):
        return CLASSNAME not in value and all(_is_plain_json(item) for item in value.values())
    return value is None or isinstance(value, (str, int, float, bool))


def _is_team_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(team, str) for team in value)


def _translate_asset(data: Any) -> dict[str, Any]:
    """Build the legacy encoding of an Asset from its serde data, field for field as ``encode_asset_like``."""
    from airflow._shared.serialization import CLASSNAME, DATA

    unconvertible = _UnconvertibleNextKwargsError(f"a value of type {_ASSET_CLASSNAME!r}")
    required = {"name", "uri", "group", "extra"}
    if (
        not isinstance(data, dict)
        or not required <= data.keys() <= required | {"watchers", "access_control"}
        or data.get("watchers")
        or not _is_plain_json(data["extra"])
    ):
        raise unconvertible
    var = {key: data[key] for key in ("name", "uri", "group", "extra")}

    if (envelope := data.get("access_control")) is None:
        return var
    if not isinstance(envelope, dict) or envelope.get(CLASSNAME) != _ASSET_ACCESS_CONTROL_CLASSNAME:
        raise unconvertible
    ac = envelope.get(DATA)
    if (
        not isinstance(ac, dict)
        or ac.keys() != {"producer_teams", "consumer_teams", "allow_global"}
        or not _is_team_list(ac["producer_teams"])
        or not (ac["consumer_teams"] is None or _is_team_list(ac["consumer_teams"]))
        or not isinstance(ac["allow_global"], bool)
    ):
        raise unconvertible
    if ac["producer_teams"] or ac["consumer_teams"] is not None or not ac["allow_global"]:
        var["access_control"] = {"producer_teams": ac["producer_teams"], "allow_global": ac["allow_global"]}
        if ac["consumer_teams"] is not None:
            var["access_control"]["consumer_teams"] = ac["consumer_teams"]
    return var


class ModifyDeferredTaskKwargsToJsonValue(VersionChange):
    """Change the types of `trigger_kwargs` and `next_kwargs` in TIDeferredStatePayload to JsonValue."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIDeferredStatePayload).field("trigger_kwargs").had(type=dict[str, Any] | str),
        schema(TIDeferredStatePayload).field("next_kwargs").had(type=dict[str, Any]),
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def convert_next_kwargs_to_base_serialization(response: ResponseInfo) -> None:
        """
        Convert next_kwargs from SDK serde format to BaseSerialization format for old workers.

        Old workers (task-sdk < 1.2) only know BaseSerialization.deserialize(), which requires
        dicts wrapped as {"__type": "dict", "__var": {...}}. SDK serde produces plain dicts that
        BaseSerialization cannot parse, causing KeyError on __var.

        We must deserialize SDK serde first to recover native Python objects (datetime,
        timedelta, etc.), then re-serialize with BaseSerialization so old workers get
        proper typed values instead of raw {"__classname__": ...} dicts.
        """
        from airflow.models.trigger import TRIGGER_FAIL_REPR
        from airflow.serialization.serialized_objects import BaseSerialization

        next_kwargs = response.body.get("next_kwargs")
        if next_kwargs is None:
            return

        # Only built-in value types are rebuilt here; assets and asset aliases are translated
        # structurally. Anything else would be reconstructed by calling into its class in the API
        # server, so the task is resumed through the trigger-failure path instead, which old
        # workers report as a TaskDeferralError carrying this message.
        try:
            if _is_legacy_envelope(next_kwargs):
                if not _is_well_formed_legacy(next_kwargs):
                    raise _UnconvertibleNextKwargsError("a malformed BaseSerialization-encoded value")
                # Already in BaseSerialization format (rolling upgrade, old data in DB)
                return
            response.body["next_kwargs"] = _convert_to_legacy(next_kwargs)
        except _UnconvertibleNextKwargsError as e:
            response.body["next_method"] = TRIGGER_FAIL_REPR
            response.body["next_kwargs"] = BaseSerialization.serialize({"error": str(e)})
        except (ImportError, KeyError, AttributeError, TypeError):
            # Already in BaseSerialization format (rolling upgrade, old data in DB)
            return


class RemoveUpstreamMapIndexesField(VersionChange):
    """Remove upstream_map_indexes field from TIRunContext - now computed by Task SDK."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext)
        .field("upstream_map_indexes")
        .existed_as(type=dict[str, int | list[int] | None] | None),
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def add_upstream_map_indexes_field(response: ResponseInfo) -> None:
        """Add upstream_map_indexes field with None for older API versions."""
        response.body["upstream_map_indexes"] = None


class AddNoteField(VersionChange):
    """Add note parameter to DagRun Model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(DagRun).field("note").didnt_exist,
        schema(TriggerDAGRunPayload).field("note").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_note_field(response: ResponseInfo) -> None:
        """Remove note field for older API versions."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("note", None)


class AddTaskInstanceStartDateField(VersionChange):
    """Add `start_date` field to TIRunContext."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("start_date").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_start_date_field(response: ResponseInfo) -> None:
        """Remove start_date field for older API versions."""
        response.body.pop("start_date", None)


class AddDagEndpoint(VersionChange):
    """Add the `/dags/{dag_id}` endpoint."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (endpoint("/dags/{dag_id}", ["GET"]).didnt_exist,)


class AddRunAfterField(VersionChange):
    """Add run_after parameter to TriggerDAGRunPayload Model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TriggerDAGRunPayload).field("run_after").didnt_exist,
    )
