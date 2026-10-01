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

import copy
import hashlib
import json
import sys
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

import pytest
from jsonschema import Draft7Validator

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensor, DateTimeSensorAsync
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, Asset, ExceptionRetryPolicy, Param, TaskGroup, task as sdk_task, task_group
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.operator_resources import Resources
from airflow.serialization import dag_version_diff
from airflow.serialization.dag_version_diff import (
    _DIFF_V1_DAG_FIELD_CATEGORIES,
    _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS,
    _DIFF_V1_PUBLIC_ROOT_FIELDS,
    _DIFF_V1_PUBLIC_TASK_FIELDS,
    _DIFF_V1_PUBLIC_TASK_GROUP_FIELDS,
    _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS,
    _DIFF_V1_TASK_GROUP_METADATA_FIELDS,
    MAX_ALLOWED_CHANGES,
    SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS,
    build_serialized_dag_diff,
    build_unavailable_dag_diff,
)
from airflow.serialization.json_schema import load_dag_schema_dict
from airflow.serialization.serialized_objects import (
    _DAG_CALLBACK_FIELDS,
    _OPERATOR_TIMEDELTA_FIELDS,
    BaseSerialization,
    DagSerialization,
    OperatorSerialization,
)


def _build_payload(
    *,
    tasks: list[dict],
    tags: list[str] | None = None,
    schedule: str = "daily",
    dependencies: list[dict] | None = None,
) -> dict:
    return {
        "__version": 3,
        "dag": {
            "dag_id": "example",
            "schedule": schedule,
            "tags": tags or [],
            "tasks": [
                {
                    "__type": "airflow.providers.standard.operators.empty.EmptyOperator",
                    "__var": task,
                }
                for task in tasks
            ],
            "dag_dependencies": dependencies or [],
        },
    }


def _serialize_dag(dag: DAG) -> dict[str, Any]:
    return json.loads(json.dumps(DagSerialization.to_dict(dag)))


def _get_task(payload: dict[str, Any]) -> dict[str, Any]:
    return payload["dag"]["tasks"][0]["__var"]


def _build_field_sweep_payload() -> dict[str, Any]:
    """Serialize one Dag covering the operator shapes that emit distinct task field sets."""
    # The drift guard below is only as complete as the shapes built here: a new serializer field
    # emitted solely by a shape this sweep does not construct still lands in custom_fields
    # unnoticed, so a new operator shape needs a task here, not just a new allowlist entry.
    with DAG("field_sweep", schedule=None) as dag:
        EmptyOperator(task_id="empty")
        BranchPythonOperator(task_id="branch", python_callable=_return_private_before)
        PythonOperator(task_id="python", python_callable=_return_private_before, op_kwargs={"a": 1})
        DateTimeSensor(task_id="rescheduling", target_time="{{ ts }}", mode="reschedule")
        DateTimeSensorAsync(task_id="deferring", target_time="{{ ts }}")
        BashOperator(
            task_id="configured",
            bash_command="echo unchanged",
            run_as_user="service",
            email="owner@example.org",
            retry_policy=ExceptionRetryPolicy(rules=[]),
            executor_config={"key": "value"},
            resources={"cpus": 1},
            outlets=[Asset("s3://sweep")],
        )
        BashOperator(task_id="setup", bash_command="echo unchanged").as_setup()
        BashOperator(task_id="teardown", bash_command="echo unchanged").as_teardown()
        BashOperator.partial(task_id="mapped", run_as_user="service").expand(bash_command=["one", "two"])
        sdk_task(task_id="decorated")(_return_private_before).expand(private_argument=["one", "two"])
        sdk_task.stub(task_id="stubbed")(_declare_private_argument)("private_value")
        with TaskGroup("group"):
            BashOperator(task_id="grouped", bash_command="echo unchanged")
    return _serialize_dag(dag)


def _collect_emitted_task_fields(payload: dict[str, Any]) -> tuple[set[str], set[str], set[str]]:
    emitted: set[str] = set()
    emitted_partial: set[str] = set()
    template_fields: set[str] = set()
    for entry in payload["dag"]["tasks"]:
        task_fields = entry["__var"]
        partial_kwargs = task_fields.get("partial_kwargs", {})
        emitted |= task_fields.keys()
        emitted_partial |= partial_kwargs.keys()
        template_fields |= set(task_fields.get("template_fields", []))
        template_fields |= set(partial_kwargs.get("template_fields", []))
    return emitted, emitted_partial, template_fields


def test_diff_v1_task_field_policy_tracks_serializer_schema() -> None:
    schema_fields = frozenset(load_dag_schema_dict()["definitions"]["operator"]["properties"])
    emitted, emitted_partial, template_fields = _collect_emitted_task_fields(_build_field_sweep_payload())
    # definitions.operator is additionalProperties: true, so comparing the allowlist against the
    # schema alone cannot see a field the serializer writes and the schema never declares. Derive
    # those from a real payload instead of listing them, or they get buried in custom_fields unnoticed.
    serializer_only_fields = emitted - schema_fields - template_fields
    # Template field names are operator-defined and unbounded, so the allowlist cannot enumerate
    # them; naming one in a redacted path would also disclose what template_fields itself withholds.
    exempt_fields = _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS | template_fields

    assert emitted <= (_DIFF_V1_PUBLIC_TASK_FIELDS - {"__type"}) | exempt_fields
    assert emitted_partial <= _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS | exempt_fields
    assert schema_fields == (
        (_DIFF_V1_PUBLIC_TASK_FIELDS - {"__type"} - serializer_only_fields)
        | _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS
    )
    assert _DIFF_V1_PUBLIC_TASK_FIELDS - schema_fields == {"__type"} | serializer_only_fields
    assert not (_DIFF_V1_PUBLIC_TASK_FIELDS & _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS)


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_names_sensor_reschedule_mode_changes(include_values):
    payloads = []
    for mode in ("poke", "reschedule"):
        with DAG("sensor_mode", schedule=None) as dag:
            DateTimeSensor(task_id="private_task", target_time="{{ ts }}", mode=mode)
        payloads.append(_serialize_dag(dag))
    hydrated = [
        DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["private_task"].reschedule
        for payload in payloads
    ]
    assert hydrated == [False, True]

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    task_id = "private_task" if include_values else "*"
    assert [change["path"] for change in result["changes"]] == [f"/dag/tasks/{task_id}/reschedule"]
    assert result["changes"][0]["impact"] == "execution"


def _return_private_before(private_argument=None):
    return private_argument


def _return_private_after(private_argument=None):
    return private_argument


def _declare_private_argument(private_argument: str): ...


def _build_serializer_only_field_payload(field: str, is_mapped: bool, *, after: bool) -> dict[str, Any]:
    with DAG("serializer_only_fields", schedule=None) as dag:
        if field == "python_callable_name":
            python_callable = _return_private_after if after else _return_private_before
            if is_mapped:
                PythonOperator.partial(task_id="private_task", python_callable=python_callable).expand(
                    op_kwargs=[{"private_argument": "private_value"}]
                )
            else:
                PythonOperator(task_id="private_task", python_callable=python_callable)
        elif field == "expand_input":
            BashOperator.partial(task_id="private_task").expand(
                bash_command=["private_before", "private_after"] if after else ["private_before"]
            )
        elif field == "op_kwargs_expand_input":
            sdk_task(task_id="private_task")(_return_private_before).expand(
                private_argument=["private_before", "private_after"] if after else ["private_before"]
            )
        else:
            values: dict[str, tuple[Any, Any]] = {
                "resources": ({"cpus": 1}, {"cpus": 2}),
                "run_as_user": ("private_before", "private_after"),
                "email": ("private_before@example.org", "private_after@example.org"),
                "has_retry_policy": (None, ExceptionRetryPolicy(rules=[])),
            }
            constructor_field = "retry_policy" if field == "has_retry_policy" else field
            kwargs = {} if field == "_operator_name" else {constructor_field: values[field][after]}
            operator = (
                BashOperator.partial(task_id="private_task", **kwargs).expand(bash_command=["echo unchanged"])
                if is_mapped
                else BashOperator(task_id="private_task", bash_command="echo unchanged", **kwargs)
            )
            if field == "_operator_name":
                setattr(
                    operator,
                    "_operator_name" if is_mapped else "custom_operator_name",
                    "private_after" if after else "private_before",
                )
    return _serialize_dag(dag)


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "is_mapped"),
    [
        ("python_callable_name", False),
        ("python_callable_name", True),
        ("expand_input", True),
        ("op_kwargs_expand_input", True),
        ("resources", False),
        ("resources", True),
        ("run_as_user", False),
        ("run_as_user", True),
        ("email", False),
        ("email", True),
        ("_operator_name", False),
        ("_operator_name", True),
        ("has_retry_policy", False),
        ("has_retry_policy", True),
    ],
)
def test_build_diff_exposes_serializer_only_field_names_without_private_values(
    field, is_mapped, include_values
):
    base = _build_serializer_only_field_payload(field, is_mapped, after=False)
    target = _build_serializer_only_field_payload(field, is_mapped, after=True)
    for payload in (base, target):
        emitted_fields = _get_task(payload)
        redacted_fields = _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS | set(
            emitted_fields.get("template_fields", [])
        )
        assert emitted_fields.keys() <= (_DIFF_V1_PUBLIC_TASK_FIELDS - {"__type"}) | redacted_fields
        assert emitted_fields.get("partial_kwargs", {}).keys() <= (
            _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS | redacted_fields
        )
    task_fields = _get_task(target)
    in_partial_kwargs = is_mapped and field in {
        "python_callable_name",
        "resources",
        "run_as_user",
        "email",
        "has_retry_policy",
    }
    stored_fields = task_fields["partial_kwargs"] if in_partial_kwargs else task_fields
    assert field in stored_fields
    originals = copy.deepcopy((base, target))

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    task_id = "private_task" if include_values else "*"
    field_path = f"partial_kwargs/{field}" if in_partial_kwargs else field
    assert change["path"] == f"/dag/tasks/{task_id}/{field_path}"
    expected_category = "metadata" if field == "_operator_name" else "task"
    assert change["category"] == expected_category
    assert change["impact"] == ("metadata" if field == "_operator_name" else "execution")
    if include_values:
        assert change["after_value"] == stored_fields[field]
        assert change["before_digest"] != change["after_digest"]
    else:
        assert "private_" not in json.dumps(result)
        assert not {"before_value", "after_value", "before_digest", "after_digest"} & change.keys()
    assert (base, target) == originals


def test_diff_v1_task_group_field_policy_tracks_serializer_schema() -> None:
    schema_fields = set(load_dag_schema_dict()["definitions"]["task_group"]["properties"])

    # A mapped group serializes expand_input, which the task_group definition does not declare.
    assert schema_fields == _DIFF_V1_PUBLIC_TASK_GROUP_FIELDS - {"expand_input"}
    assert _DIFF_V1_TASK_GROUP_METADATA_FIELDS <= _DIFF_V1_PUBLIC_TASK_GROUP_FIELDS


def test_diff_v1_dag_field_policy_tracks_serializer_schema() -> None:
    schema_fields = set(load_dag_schema_dict()["definitions"]["dag"]["properties"])

    assert schema_fields == _DIFF_V1_DAG_FIELD_CATEGORIES.keys()


def test_diff_v1_root_field_policy_tracks_serializer_schema() -> None:
    schema_fields = set(load_dag_schema_dict()["allOf"][0]["properties"])

    # __version and client_defaults are folded in by canonicalization, which adds provenance.
    assert (schema_fields - {"__version", "client_defaults"}) | {"provenance"} == _DIFF_V1_PUBLIC_ROOT_FIELDS


def test_dag_callback_fields_track_serializer_schema() -> None:
    schema_fields = load_dag_schema_dict()["definitions"]["dag"]["properties"]

    assert {
        field for field in schema_fields if field.startswith("has_on_") and field.endswith("_callback")
    } == _DAG_CALLBACK_FIELDS


def test_operator_timedelta_fields_track_serializer_schema() -> None:
    schema_fields = load_dag_schema_dict()["definitions"]["operator"]["properties"]

    assert {
        field for field, schema in schema_fields.items() if schema.get("$ref") == "#/definitions/timedelta"
    } == _OPERATOR_TIMEDELTA_FIELDS


def test_json_value_encoding_tracks_serializer() -> None:
    value = {"nested": [None, True, 7, 1.5, "plain", {"__type": "dict", "__var": {"cpu": 1}}]}
    assert dag_version_diff._encode_json_value(value) == BaseSerialization.serialize(value)


def test_supported_schema_versions_track_serializer() -> None:
    # A serializer bump without a matching conversion in _canonicalize_payload_v1 would otherwise
    # turn every stored Dag in a deployment into unsupported_serialized_dag_schema_version.
    supported_by_serializer = frozenset(range(1, DagSerialization.SERIALIZER_VERSION + 1))

    assert supported_by_serializer == SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS


def test_task_field_categories_are_reachable() -> None:
    # _get_category only reaches these after a non-public field has been aggregated under
    # custom_fields, so a name outside the allowlist would be dead.
    classified = (
        dag_version_diff._DIFF_V1_TASK_ASSET_FIELDS
        | dag_version_diff._DIFF_V1_TASK_PARAM_FIELDS
        | dag_version_diff._DIFF_V1_TASK_DEPENDENCY_FIELDS
        | dag_version_diff._DIFF_V1_TASK_METADATA_FIELDS
    )

    assert classified <= _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS


@pytest.mark.parametrize("field", sorted(dag_version_diff._DIFF_V1_LEGACY_DAG_FIELD_CATEGORIES))
def test_legacy_dag_field_categories_are_reachable(field) -> None:
    # The v1 conversion rewrites some retired fields away, and a category for a field that never
    # survives canonicalization classifies nothing.
    payload = _build_v1_payload()
    payload["dag"][field] = "@daily"

    canonical = dag_version_diff._canonicalize_payload_v1(payload)

    assert field in canonical["dag"]


def _build_params_payload(params: dict) -> dict:
    with DAG("params_example", schedule=None, params=params) as dag:
        BashOperator(task_id="extract", bash_command="echo hello")
    return _serialize_dag(dag)


def _get_param_container(payload: dict[str, Any], location: str) -> dict[str, Any]:
    return payload["dag"] if location == "dag" else _get_task(payload)


def _get_hydrated_params(payload: dict[str, Any], location: str):
    dag = DagSerialization.from_dict(copy.deepcopy(payload))
    return dag.params if location == "dag" else dag.task_dict["extract"].params


def _rewrite_params(payload: dict, rewrite) -> dict:
    payload = copy.deepcopy(payload)
    for data in [payload["dag"], *(task["__var"] for task in payload["dag"]["tasks"])]:
        if "params" in data:
            data["params"] = rewrite(data["params"])
    return payload


def _as_legacy_class(params: list) -> list:
    return [[name, {**param, "__class": "airflow.models.param.Param"}] for name, param in params]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("rewrite", "shape"),
    [
        (dict, "mapping stored by 2.9.2 and earlier"),
        (_as_legacy_class, "pre-SDK Param class identifier"),
    ],
)
def test_build_diff_normalizes_equivalent_param_shapes(rewrite, shape, include_values):
    current = _build_params_payload({"x": 1, "y": 2})
    stored = _rewrite_params(current, rewrite)
    hydrated = [
        list(DagSerialization.from_dict(copy.deepcopy(payload)).params.items())
        for payload in (stored, current)
    ]
    assert [name for name, _ in hydrated[0]] == [name for name, _ in hydrated[1]], shape
    assert [value.dump() for _, value in hydrated[0]] == [value.dump() for _, value in hydrated[1]], shape

    result = build_serialized_dag_diff(base_data=stored, target_data=current, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("location", ["dag", "task"])
@pytest.mark.parametrize(
    "shape",
    [
        "primitive",
        "minimal_param",
        "missing_source",
        "missing_description",
        "missing_schema",
        "plain_schema",
        "plain_default",
    ],
)
def test_diff_normalizes_equivalent_param_contents(include_values, location, shape):
    value = {"nested": [1, {"value": True}]} if shape == "plain_default" else "same"
    schema = {"type": "string"} if shape == "plain_schema" else {}
    current = _build_params_payload({"x": Param(value, schema=schema)})
    _get_param_container(current, location)["params"] = copy.deepcopy(current["dag"]["params"])
    stored = copy.deepcopy(current)
    container = _get_param_container(stored, location)
    param = container["params"][0][1]
    if shape == "primitive":
        container["params"] = {"x": value}
    elif shape == "minimal_param":
        container["params"] = {"x": {"__class": "airflow.models.param.Param", "default": value}}
    elif shape.startswith("missing_"):
        param.pop(shape.removeprefix("missing_"))
    elif shape == "plain_schema":
        param["schema"] = schema
    else:
        param["default"] = value
    DagSerialization.validate_schema(current)
    # Hydration still accepts the legacy mappings excluded by the current schema.
    if shape not in {"primitive", "minimal_param"}:
        DagSerialization.validate_schema(stored)
    assert (
        _get_hydrated_params(stored, location).get_param("x").dump()
        == _get_hydrated_params(current, location).get_param("x").dump()
    )

    result = build_serialized_dag_diff(base_data=stored, target_data=current, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_still_reports_renamed_params() -> None:
    result = build_serialized_dag_diff(
        base_data=_build_params_payload({"x": 1}),
        target_data=_build_params_payload({"renamed": 1}),
    )

    assert [change["path"] for change in result["changes"]] == [
        "/dag/params",
        "/dag/tasks/*/params",
    ]


@pytest.mark.parametrize("include_values", [False, True])
def test_diff_preserves_param_definition_order(include_values):
    base = _build_params_payload({"first": 1, "second": 2})
    target = _build_params_payload({"second": 2, "first": 1})
    for location in ("dag", "task"):
        assert list(_get_hydrated_params(base, location)) == ["first", "second"]
        assert list(_get_hydrated_params(target, location)) == ["second", "first"]

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    task_id = "extract" if include_values else "*"
    assert [change["path"] for change in result["changes"]] == ["/dag/params", f"/dag/tasks/{task_id}/params"]
    if include_values:
        for change in result["changes"]:
            assert [pair[0] for pair in change["before_value"]] == ["first", "second"]
            assert [pair[0] for pair in change["after_value"]] == ["second", "first"]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("location", ["dag", "task"])
@pytest.mark.parametrize(
    ("field", "before", "after"),
    [
        ("default", "old", "new"),
        ("schema", {"minLength": 1}, {"minLength": 2}),
        ("source", "dag", "task"),
    ],
)
def test_diff_preserves_param_value_schema_and_source_changes(include_values, location, field, before, after):
    base = _build_params_payload({"x": "same"})
    target = copy.deepcopy(base)
    for payload, value in ((base, before), (target, after)):
        _get_param_container(payload, location)["params"][0][1][field] = value
    assert (
        _get_hydrated_params(base, location).get_param("x").dump()
        != _get_hydrated_params(target, location).get_param("x").dump()
    )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    task_id = "extract" if include_values else "*"
    change = result["changes"][0]
    assert change["path"] == ("/dag/params" if location == "dag" else f"/dag/tasks/{task_id}/params")
    if include_values:
        assert change["before_digest"] != change["after_digest"]


@pytest.mark.parametrize("include_values", [False, True])
def test_diff_preserves_literal_encoding_keys_in_param_defaults(include_values):
    base = _build_params_payload({"x": {"nested": {"__type": "dict", "__var": {"value": "same"}}}})
    target = _build_params_payload({"x": {"nested": {"value": "same"}}})
    assert _get_hydrated_params(base, "dag")["x"] != _get_hydrated_params(target, "dag")["x"]

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    task_id = "extract" if include_values else "*"
    assert [change["path"] for change in result["changes"]] == ["/dag/params", f"/dag/tasks/{task_id}/params"]
    if include_values:
        assert all(change["before_digest"] != change["after_digest"] for change in result["changes"])


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("mapped", [False, True])
@pytest.mark.parametrize("change", ["class", "description"])
def test_diff_preserves_templated_params(include_values, mapped, change):
    with DAG("diff_regression", schedule=None, params={"x": "same"}) as dag:
        if mapped:
            BashOperator.partial(task_id="extract").expand(bash_command=["echo hello"])
        else:
            BashOperator(task_id="extract", bash_command="echo hello")
    base = _serialize_dag(dag)
    task = _get_task(base)
    task["template_fields"].append("params")
    operator_schema = {"$ref": "#/definitions/operator", "definitions": load_dag_schema_dict()["definitions"]}
    schema_defaults = DagSerialization.get_schema_defaults("operator")
    for field in operator_schema["definitions"]["operator"]["required"]:
        if field not in task:
            task[field] = schema_defaults[field]
    target = copy.deepcopy(base)
    param = _get_task(target)["params"][0][1]
    if change == "class":
        param["__class"] = "airflow.models.param.Param"
    else:
        param.pop("description")
    originals = copy.deepcopy((base, target))
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        Draft7Validator(operator_schema).validate(_get_task(payload))
        hydrated_params = _get_hydrated_params(payload, "task")
        assert isinstance(hydrated_params, list)
        assert hydrated_params == _get_task(payload)["params"]
    assert _get_task(base)["params"] != _get_task(target)["params"]

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    task_id = "extract" if include_values else "*"
    assert change["path"] == f"/dag/tasks/{task_id}/params"
    assert change["category"] == "param"
    assert change["impact"] == "execution"
    if include_values:
        assert change["before_value"] == _get_task(base)["params"]
        assert change["after_value"] == _get_task(target)["params"]
        assert change["before_digest"] != change["after_digest"]
    else:
        assert "before_value" not in change
        assert "after_value" not in change
    assert (base, target) == originals


def _build_dated_payloads(field: str, task_date: datetime | None) -> list[dict]:
    payloads = []
    for day in (2, 1):
        dag_kwargs: dict[str, Any] = {field: datetime(2024, 1, day, tzinfo=timezone.utc)}
        task_kwargs: dict[str, Any] = {field: task_date} if task_date else {}
        with DAG("dates_example", schedule=None, **dag_kwargs) as dag:
            BashOperator(task_id="extract", bash_command="echo hello", **task_kwargs)
        payloads.append(json.loads(json.dumps(DagSerialization.to_dict(dag))))
    return payloads


def _hydrated_task_dates(payloads: list[dict], field: str) -> list[Any]:
    return [
        getattr(DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"], field)
        for payload in payloads
    ]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "pinned_day", "stored_dates"),
    [("start_date", 2, [False, True]), ("end_date", 1, [True, False])],
)
def test_build_diff_ignores_task_dates_matching_the_dag(include_values, field, pinned_day, stored_dates):
    pinned = datetime(2024, 1, pinned_day, tzinfo=timezone.utc)
    payloads = _build_dated_payloads(field, pinned)
    assert [field in payload["dag"]["tasks"][0]["__var"] for payload in payloads] == stored_dates
    assert set(_hydrated_task_dates(payloads, field)) == {pinned}

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    assert [change["path"] for change in result["changes"]] == [f"/dag/{field}"]


@pytest.mark.parametrize("field", ["start_date", "end_date"])
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_reports_inherited_task_date_changes(field, include_values):
    payloads = _build_dated_payloads(field, task_date=None)
    # Neither payload stores the task date, but each task still inherits its own Dag's.
    assert not any(field in payload["dag"]["tasks"][0]["__var"] for payload in payloads)
    base_date, target_date = _hydrated_task_dates(payloads, field)
    assert base_date != target_date

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    assert [change["path"] for change in result["changes"]] == [
        f"/dag/{field}",
        f"/dag/tasks/{'extract' if include_values else '*'}/{field}",
    ]


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_reports_values_status_from_every_entry_point(include_values):
    base = _build_payload(tasks=[{"task_id": "extract", "retries": 1}])
    target = _build_payload(tasks=[{"task_id": "extract", "retries": 2}])
    expected = "available" if include_values else "unavailable"

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)
    unavailable = build_unavailable_dag_diff(
        base_data=base, target_data=None, reason="serialized_dag_missing"
    )

    assert result["values"] == {"status": expected}
    assert unavailable["values"] == {"status": "unavailable"}


def test_build_diff_pins_diff_schema_version_and_top_level_keys() -> None:
    base = _build_payload(tasks=[{"task_id": "extract", "retries": 1}])
    target = _build_payload(tasks=[{"task_id": "extract", "retries": 2}])

    observed = build_serialized_dag_diff(base_data=base, target_data=target)
    unavailable = build_unavailable_dag_diff(
        base_data=base, target_data=None, reason="serialized_dag_missing"
    )

    assert observed == {
        "diff_schema_version": 1,
        "serialized_dag_schema_versions": {"base": 3, "target": 3},
        "mode": "observed_state",
        "changes": [
            {
                "path": "/dag/tasks/*/retries",
                "operation": "changed",
                "category": "task",
                "impact": "execution",
                "occurrence_count": 1,
            }
        ],
        "truncated": False,
        "values": {"status": "unavailable"},
    }
    assert unavailable == {
        "diff_schema_version": 1,
        "serialized_dag_schema_versions": {"base": 3, "target": None},
        "mode": "unavailable",
        "unavailable_reason": "serialized_dag_missing",
        "changes": [],
        "truncated": False,
        "values": {"status": "unavailable"},
    }


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_marks_values_unavailable_when_comparison_fails(include_values):
    base = _build_payload(tasks=[])
    target = copy.deepcopy(base)
    target["dag"]["tasks"] = "not a list"

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "unavailable"
    assert result["values"] == {"status": "unavailable"}


def test_build_diff_rejects_unsupported_client_defaults_sections(caplog):
    base = _build_payload(tasks=[{"task_id": "extract"}])
    target = copy.deepcopy(base)
    base["client_defaults"] = {"tasks": {}, "dags": {"catchup": True}}
    target["client_defaults"] = {"tasks": {}, "dags": {"catchup": False}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["mode"] == "unavailable"
    assert result["unavailable_reason"] == "serialized_dag_canonicalization_failed"
    assert result["changes"] == []
    assert {
        "event": "Serialized Dag diff canonicalization failed",
        "error_type": "ValueError",
    } in caplog


@pytest.mark.parametrize("max_changes", [1, 9, 10, 11])
def test_build_diff_bound_withholds_records_not_public_paths(max_changes):
    def payload(value):
        return _build_payload(
            tasks=[
                {
                    "task_id": f"task{index}",
                    "executor_config": {"__type": "dict", "__var": {f"k{key}": value for key in range(20)}},
                }
                for index in range(10)
            ]
        )

    base, target = payload("old"), payload("new")
    results = {
        include_values: build_serialized_dag_diff(
            base_data=copy.deepcopy(base),
            target_data=copy.deepcopy(target),
            include_values=include_values,
            max_changes=max_changes,
        )
        for include_values in (False, True)
    }

    redacted, valued = results[False], results[True]
    included = min(10, max_changes)
    assert len(valued["changes"]) == included
    assert valued["truncated"] is (max_changes < 10)
    assert {change["path"] for change in valued["changes"]} == {
        f"/dag/tasks/task{index}/executor_config" for index in range(included)
    }
    # The sole public path is disclosed by the first change, so no bound can withhold a path
    # from the caller or cost an occurrence.
    assert redacted["truncated"] is False
    assert redacted["changes"] == [
        {
            "path": "/dag/tasks/*/executor_config",
            "operation": "changed",
            "category": "task",
            "impact": "execution",
            "occurrence_count": 10,
        }
    ]
    assert all(change["occurrence_count"] == 1 for change in valued["changes"])
    assert all("after_value" in change for change in valued["changes"])


@pytest.mark.parametrize("max_changes", [2, 3])
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_excludes_new_group_beyond_change_limit(max_changes, include_values):
    base = _build_payload(tasks=[{"task_id": name, "retries": 0} for name in ("a", "b", "c")])
    target = _build_payload(
        tasks=[
            {"task_id": "a", "retries": 1},
            {"task_id": "b", "retries": 1},
            {"task_id": "c", "queue": "new"},
        ]
    )

    result = build_serialized_dag_diff(
        base_data=base, target_data=target, max_changes=max_changes, include_values=include_values
    )

    if include_values:
        assert result["truncated"] is (max_changes == 2)
        expected_paths = ["/dag/tasks/a/retries", "/dag/tasks/b/retries", "/dag/tasks/c/queue"]
        assert [change["path"] for change in result["changes"]] == expected_paths[:max_changes]
    else:
        assert result["truncated"] is False
        assert [(change["path"], change["occurrence_count"]) for change in result["changes"]] == [
            ("/dag/tasks/*/retries", 2),
            ("/dag/tasks/*/queue", 1),
        ]


@pytest.mark.parametrize("max_changes", [3, 2])
@pytest.mark.parametrize("repeated_path", [False, True])
def test_build_diff_truncation_boundary_is_exact_in_both_modes(repeated_path, max_changes):
    """One change over the bound truncates only when it would disclose an unseen path."""
    if repeated_path:
        base_tasks = [{"task_id": name, "owner": "alice"} for name in ("a", "b", "c")]
        target_tasks = [{"task_id": name, "owner": "bob"} for name in ("a", "b", "c")]
        redacted_paths = ["/dag/tasks/*/owner"]
    else:
        base_tasks = [{"task_id": "a", "doc_md": "old", "owner": "alice", "queue": "old"}]
        target_tasks = [{"task_id": "a", "doc_md": "new", "owner": "bob", "queue": "new"}]
        redacted_paths = ["/dag/tasks/*/doc_md", "/dag/tasks/*/owner", "/dag/tasks/*/queue"]

    redacted, valued = [
        build_serialized_dag_diff(
            base_data=_build_payload(tasks=copy.deepcopy(base_tasks)),
            target_data=_build_payload(tasks=copy.deepcopy(target_tasks)),
            include_values=include_values,
            max_changes=max_changes,
        )
        for include_values in (False, True)
    ]

    assert len(valued["changes"]) == max_changes
    assert valued["truncated"] is (max_changes < 3)
    assert [change["path"] for change in redacted["changes"]] == redacted_paths[: len(redacted["changes"])]
    if repeated_path:
        assert redacted["truncated"] is False
        assert [change["occurrence_count"] for change in redacted["changes"]] == [3]
    else:
        assert redacted["truncated"] is (max_changes < 3)
        assert [change["occurrence_count"] for change in redacted["changes"]] == [1] * max_changes


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_keeps_operations_separate_when_grouping(include_values):
    base = _build_payload(
        tasks=[
            {"task_id": "a", "doc_md": "before"},
            {"task_id": "b"},
            {"task_id": "c", "doc_md": "before"},
            {"task_id": "d", "doc_md": "before"},
        ]
    )
    target = _build_payload(
        tasks=[
            {"task_id": "a"},
            {"task_id": "b", "doc_md": "after"},
            {"task_id": "c", "doc_md": "after"},
            {"task_id": "d", "doc_md": "different"},
        ]
    )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["truncated"] is False
    assert [(change["operation"], change["occurrence_count"]) for change in result["changes"]] == (
        [("removed", 1), ("added", 1), ("changed", 1), ("changed", 1)]
        if include_values
        else [("removed", 1), ("added", 1), ("changed", 2)]
    )
    assert all(change["category"] == change["impact"] == "metadata" for change in result["changes"])
    if not include_values:
        assert {change["path"] for change in result["changes"]} == {"/dag/tasks/*/doc_md"}
        assert all(value not in json.dumps(result) for value in ("before", "after", "different"))
    for payload in (base, target):
        payload["dag"]["tasks"].reverse()
        payload["dag"] = dict(reversed(payload["dag"].items()))
    assert result == build_serialized_dag_diff(
        base_data=base, target_data=target, include_values=include_values
    )


@pytest.mark.parametrize(("max_changes", "valued_truncated"), [(500, True), (1201, False)])
def test_build_diff_groups_repeated_changes_in_large_dag(max_changes, valued_truncated):
    payloads = []
    for day, retries in ((1, 0), (2, 1)):
        with DAG(
            "grouped_diff", schedule=None, start_date=datetime(2025, 1, day, tzinfo=timezone.utc)
        ) as dag:
            for index in range(600):
                BashOperator(task_id=f"task_{index:04d}", bash_command="echo hello", retries=retries)
        payloads.append(_serialize_dag(dag))

    redacted, valued = [
        build_serialized_dag_diff(
            base_data=payloads[0],
            target_data=payloads[1],
            include_values=include_values,
            max_changes=max_changes,
        )
        for include_values in (False, True)
    ]

    assert valued["truncated"] is valued_truncated
    # All three public paths appear within the first few changes, so the redacted diff stays
    # complete and exact at a bound the authorized diff exhausts.
    assert redacted["truncated"] is False
    assert [(change["path"], change["occurrence_count"]) for change in redacted["changes"]] == [
        ("/dag/start_date", 1),
        ("/dag/tasks/*/retries", 600),
        ("/dag/tasks/*/start_date", 600),
    ]
    assert len(valued["changes"]) == len({change["path"] for change in valued["changes"]}) == max_changes
    assert sum(change["occurrence_count"] for change in redacted["changes"]) == 1201


@pytest.mark.parametrize("max_changes", [100, 200])
def test_build_diff_counts_every_occurrence_at_a_disclosed_path(max_changes):
    """A Dag-level edit fanning out to every task stays complete and exactly counted."""
    payloads = []
    for owner in ("alice", "bob"):
        with DAG(
            "fan_out",
            schedule=None,
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            default_args={"owner": owner},
        ) as dag:
            for index in range(120):
                EmptyOperator(task_id=f"task_{index:04d}")
        payloads.append(_serialize_dag(dag))

    redacted = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], max_changes=max_changes
    )

    assert redacted["truncated"] is False
    assert [(change["path"], change["occurrence_count"]) for change in redacted["changes"]] == [
        ("/dag/default_args/owner", 1),
        ("/dag/tasks/*/owner", 120),
    ]


def test_build_diff_reports_no_execution_impact_for_a_default_args_owner_edit() -> None:
    """The inherited task change and the default_args key it came from agree on impact."""
    payloads = []
    for owner in ("alice", "bob"):
        with DAG(
            "owner_only",
            schedule=None,
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            default_args={"owner": owner},
        ) as dag:
            EmptyOperator(task_id="extract")
        payloads.append(_serialize_dag(dag))

    result = build_serialized_dag_diff(base_data=payloads[0], target_data=payloads[1])

    assert [(change["path"], change["category"], change["impact"]) for change in result["changes"]] == [
        ("/dag/default_args/owner", "metadata", "metadata"),
        ("/dag/tasks/*/owner", "metadata", "metadata"),
    ]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("operation", ["added", "removed"])
def test_build_diff_classifies_the_first_default_args_entry_per_key(operation, include_values):
    """A Dag omits default_args entirely until it has one, so the boundary must not skip per-key."""
    payloads = []
    for default_args in (None, {"owner": "bob"}):
        keywords = {} if default_args is None else {"default_args": default_args}
        with DAG("first_entry", schedule=None, **keywords) as dag:
            EmptyOperator(task_id="extract")
        payloads.append(_serialize_dag(dag))
    assert "default_args" not in payloads[0]["dag"], "a Dag without default_args omits the key"
    base, target = payloads if operation == "added" else payloads[::-1]

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    default_args_changes = [
        (change["path"], change["operation"], change["category"], change["impact"])
        for change in result["changes"]
        if "default_args" in change["path"]
    ]
    assert default_args_changes == [("/dag/default_args/owner", operation, "metadata", "metadata")]


def test_build_diff_reports_nothing_when_neither_version_sets_default_args() -> None:
    payloads = []
    for task_id in ("extract", "extract"):
        with DAG("no_default_args", schedule=None) as dag:
            EmptyOperator(task_id=task_id)
        payloads.append(_serialize_dag(dag))

    result = build_serialized_dag_diff(base_data=payloads[0], target_data=payloads[1])

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize(
    ("max_changes", "expected_counts", "truncated"),
    [(1, [1], True), (3, [1, 1, 1], True), (5, [1, 1, 4, 1], False), (500, [1, 1, 4, 1], False)],
)
def test_build_diff_redacted_records_mirror_authorized_paths(max_changes, expected_counts, truncated):
    """Redacted records are the authorized paths mapped through the public-path projection."""
    base = _build_payload(
        tasks=[{"task_id": name, "owner": "alice"} for name in ("a", "b", "c", "d")], tags=["old"]
    )
    base["dag"]["tasks"][0]["__var"]["queue"] = "old"
    target = _build_payload(
        tasks=[{"task_id": name, "owner": "bob"} for name in ("a", "b", "c", "d")], tags=["new"]
    )
    target["dag"]["tasks"][0]["__var"]["queue"] = "new"

    projection = dag_version_diff._get_public_path
    with mock.patch.object(dag_version_diff, "_get_public_path", wraps=projection, spec=True) as spy:
        valued = build_serialized_dag_diff(
            base_data=copy.deepcopy(base),
            target_data=copy.deepcopy(target),
            include_values=True,
            max_changes=max_changes,
        )
    redacted = build_serialized_dag_diff(
        base_data=copy.deepcopy(base), target_data=copy.deepcopy(target), max_changes=max_changes
    )

    # add() projects each change it sees, so the leading calls carry the raw authorized paths.
    raw_paths = [call.args[0] for call in spy.call_args_list][: len(valued["changes"])]
    assert [(change["path"], change["operation"]) for change in redacted["changes"]] == list(
        dict.fromkeys(
            (dag_version_diff._format_path(projection(raw_path)), change["operation"])
            for raw_path, change in zip(raw_paths, valued["changes"], strict=True)
        )
    )
    assert [change["occurrence_count"] for change in redacted["changes"]] == expected_counts
    assert redacted["truncated"] is truncated


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "before", "after", "category", "impact"),
    [
        ("start_date", 1704067200, 1704153600, "schedule", "execution"),
        ("end_date", 1704153600, 1704240000, "schedule", "execution"),
        ("timezone", "UTC", "Europe/Paris", "schedule", "execution"),
        ("_concurrency", 1, 2, "schedule", "execution"),
        ("max_consecutive_failed_dag_runs", 1, 2, "schedule", "execution"),
        ("is_paused_upon_creation", False, True, "schedule", "execution"),
        ("render_template_as_native_obj", False, True, "task", "execution"),
        ("disable_bundle_versioning", False, True, "task", "execution"),
        ("rerun_with_latest_version", False, True, "task", "execution"),
        # Only a value that is not a readable dict envelope still classifies as a whole; the
        # per-key classification of a real default_args mapping is covered separately.
        (
            "default_args",
            {"owner": "first"},
            {"owner": "second"},
            "task",
            "execution",
        ),
        ("dag_id", "old", "new", "metadata", "metadata"),
        ("dag_display_name", "Old name", "New name", "metadata", "metadata"),
        ("owner_links", {"owner": "old"}, {"owner": "new"}, "metadata", "metadata"),
        (
            "edge_info",
            {"extract": {"load": {"label": "old"}}},
            {"extract": {"load": {"label": "new"}}},
            "metadata",
            "metadata",
        ),
        ("fileloc", "/old.py", "/new.py", "provenance", "provenance"),
        ("relative_fileloc", "old.py", "new.py", "provenance", "provenance"),
        ("bundle_name", "old", "new", "provenance", "provenance"),
        ("_processor_dags_folder", "/old", "/new", "provenance", "provenance"),
        (
            "access_control",
            {"private-role": ["can_read"]},
            {"private-role": ["can_edit"]},
            "authorization",
            "authorization",
        ),
        ("custom_callback_setting", "old", "new", "unknown", "unknown"),
        # A differently cased schema name is a different key, not the schema field it resembles.
        ("TAGS", ["old"], ["new"], "unknown", "unknown"),
    ],
)
def test_build_diff_classifies_dag_fields(include_values, field, before, after, category, impact):
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"][field] = before
    target["dag"][field] = after

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["category"] == category
    assert change["impact"] == impact
    if not include_values:
        assert change == {
            "path": f"/dag/{field}",
            "operation": "changed",
            "category": category,
            "impact": impact,
            "occurrence_count": 1,
        }


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("key", "before", "after", "category", "impact"),
    [
        ("owner", "alice", "bob", "metadata", "metadata"),
        ("retries", 3, 5, "task", "execution"),
        ("queue", "old", "new", "task", "execution"),
        ("has_on_failure_callback", False, True, "callback", "execution"),
        ("outlets", [], ["asset"], "asset", "execution"),
        ("downstream_task_ids", [], ["load"], "dependency", "execution"),
    ],
)
def test_build_diff_classifies_default_args_per_key(include_values, key, before, after, category, impact):
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["default_args"] = {"__type": "dict", "__var": {key: before}}
    target["dag"]["default_args"] = {"__type": "dict", "__var": {key: after}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    # An allowlisted key is a fixed operator field name, so both modes report the same path.
    assert change["path"] == f"/dag/default_args/{key}"
    assert (change["operation"], change["category"], change["impact"]) == ("changed", category, impact)


def _build_task_group_payload(*, field: str, value: str, depth: int) -> tuple[dict, list[str]]:
    group_ids = []
    with DAG("task_group_diff", schedule=None) as dag, ExitStack() as stack:
        group = dag.task_group
        for index in range(depth):
            group = stack.enter_context(TaskGroup(group_id=f"secret_group_{index}"))
            group_id = group.group_id
            assert group_id is not None
            group_ids.append(group_id)
        setattr(group, field, value)
        BashOperator(task_id="secret_task", bash_command="echo hello")
    return DagSerialization.to_dict(dag), group_ids


def _build_group_path(group_ids: list[str], *, include_values: bool) -> str:
    return "/dag/task_group" + "".join(
        f"/children/{group_id if include_values else '*'}/1" for group_id in group_ids
    )


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("depth", "json_roundtrip"), [(0, False), (1, False), (1, True), (2, False), (2, True)]
)
@pytest.mark.parametrize("field", ["group_display_name", "tooltip", "doc_md", "ui_color", "ui_fgcolor"])
def test_build_diff_classifies_task_group_display_fields(include_values, depth, json_roundtrip, field):
    base, group_ids = _build_task_group_payload(field=field, value="old", depth=depth)
    target, _ = _build_task_group_payload(field=field, value="new", depth=depth)
    if json_roundtrip:
        base, target = json.loads(json.dumps([base, target]))
    originals = copy.deepcopy((base, target))

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == f"{_build_group_path(group_ids, include_values=include_values)}/{field}"
    assert change["operation"] == "changed"
    assert change["category"] == change["impact"] == "metadata"
    if include_values:
        assert (change["before_value"], change["after_value"]) == ("old", "new")
    else:
        assert "secret_" not in json.dumps(result)
    assert (base, target) == originals


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_build_diff_classifies_mapped_group_inputs_as_execution(include_values, nested):
    @task_group(group_id="secret_inner")
    def mapped_group(command):
        BashOperator(task_id="secret_task", bash_command=command)

    payloads = []
    for commands in (["first"], ["first", "second"]):
        with DAG("task_group_diff", schedule=None) as dag, ExitStack() as stack:
            if nested:
                stack.enter_context(TaskGroup(group_id="secret_outer"))
            mapped_group.expand(command=commands)
        payloads.append(DagSerialization.to_dict(dag))

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    group_changes = [change for change in result["changes"] if change["path"].startswith("/dag/task_group/")]
    assert len(group_changes) == 1
    change = group_changes[0]
    group_ids = ["secret_outer", "secret_outer.secret_inner"] if nested else ["secret_inner"]
    path = _build_group_path(group_ids, include_values=include_values)
    task_id = f"{group_ids[-1]}.secret_task" if include_values else "*"
    assert {change["path"] for change in result["changes"]} == {
        f"{path}/expand_input",
        f"/dag/tasks/{task_id}/custom_fields",
    }
    assert all(change["impact"] == "execution" for change in result["changes"])
    assert change["path"] == f"{path}/expand_input"
    assert change["category"] == "task"
    if not include_values:
        assert "secret_" not in json.dumps(result)


@pytest.mark.parametrize("operation", ["added", "removed"])
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_redacts_task_group_membership_changes(operation, include_values):
    payloads = []
    for include_group in (False, True):
        with DAG("task_group_diff", schedule=None) as dag:
            if include_group:
                TaskGroup(group_id="secret_group")
        payloads.append(DagSerialization.to_dict(dag))
    if operation == "removed":
        payloads.reverse()

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    group_id = "secret_group" if include_values else "*"
    assert change["path"] == f"/dag/task_group/children/{group_id}"
    assert change["operation"] == operation
    assert change["category"] == "task"
    assert change["impact"] == "execution"
    if not include_values:
        assert "secret_group" not in json.dumps(result)


def test_build_diff_bounds_nested_group_changes():
    base, group_ids = _build_task_group_payload(field="tooltip", value="old", depth=2)
    target, _ = _build_task_group_payload(field="tooltip", value="new", depth=2)
    group = target["dag"]["task_group"]
    for group_id in group_ids:
        group = group["children"][group_id][1]
    group["ui_color"] = "changed"

    result = build_serialized_dag_diff(base_data=base, target_data=target, max_changes=1)

    assert len(result["changes"]) == 1
    assert result["changes"][0]["path"] == f"{_build_group_path(group_ids, include_values=False)}/tooltip"
    assert result["truncated"] is True


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_collapses_unknown_nested_group_fields(include_values):
    base, group_ids = _build_task_group_payload(field="tooltip", value="same", depth=2)
    target = copy.deepcopy(base)
    for payload, value in ((base, "old"), (target, "new")):
        group = payload["dag"]["task_group"]
        for group_id in group_ids:
            group = group["children"][group_id][1]
        group["secret_custom_field"] = value
        group["another_secret_field"] = value

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    group_path = _build_group_path(group_ids, include_values=include_values)
    assert [change["path"] for change in result["changes"]] == [f"{group_path}/custom_fields"]
    change = result["changes"][0]
    assert (change["operation"], change["category"], change["impact"]) == (
        "changed",
        "task",
        "execution",
    )
    if include_values:
        assert change["before_value"] == {
            "secret_custom_field": "old",
            "another_secret_field": "old",
        }
        assert change["after_value"] == {
            "secret_custom_field": "new",
            "another_secret_field": "new",
        }
    else:
        assert "secret_" not in json.dumps(result)


def test_public_group_path_truncates_non_public_group_fields() -> None:
    # No walk reaches this: _collect_public_field_changes aggregates non-public group keys into
    # custom_fields first, so only the masking itself can be pinned against a future caller.
    assert dag_version_diff._get_public_path(
        ("dag", "task_group", "children", "secret_group", "1", "secret_field", "secret_inner_key")
    ) == ("dag", "task_group", "children", "*", "1", "custom_fields")


def test_build_diff_reports_known_nested_group_fields_beside_collapsed_ones():
    base, group_ids = _build_task_group_payload(field="tooltip", value="old", depth=2)
    target, _ = _build_task_group_payload(field="tooltip", value="new", depth=2)
    for payload, value in ((base, "old"), (target, "new")):
        group = payload["dag"]["task_group"]
        for group_id in group_ids:
            group = group["children"][group_id][1]
        group["secret_custom_field"] = value
        group["another_secret_field"] = value

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    group_path = _build_group_path(group_ids, include_values=False)
    assert [change["path"] for change in result["changes"]] == [
        f"{group_path}/tooltip",
        f"{group_path}/custom_fields",
    ]
    assert [change["category"] for change in result["changes"]] == ["metadata", "task"]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("max_changes", "expected_counts", "redacted_truncated"),
    [
        (1, [("tooltip", 1)], True),
        (3, [("tooltip", 2), ("custom_fields", 2)], False),
        (4, [("tooltip", 2), ("custom_fields", 2)], False),
    ],
)
def test_build_diff_groups_nested_task_group_changes(
    include_values, max_changes, expected_counts, redacted_truncated
):
    with DAG("grouped_diff", schedule=None) as dag:
        with TaskGroup("secret_outer"):
            for index in range(2):
                TaskGroup(f"secret_inner_{index}", tooltip="before")
    base = _serialize_dag(dag)
    target = copy.deepcopy(base)
    for payload, value in ((base, "before"), (target, "after")):
        children = payload["dag"]["task_group"]["children"]["secret_outer"][1]["children"]
        for _, group in children.values():
            group.update(tooltip=value, secret_field=value, another_secret_field=value)

    result = build_serialized_dag_diff(
        base_data=base, target_data=target, include_values=include_values, max_changes=max_changes
    )

    if include_values:
        assert result["truncated"] is (max_changes < 4)
        assert sum(change["occurrence_count"] for change in result["changes"]) == max_changes
        expected_paths = [
            f"/dag/task_group/children/secret_outer/1/children/secret_outer.secret_inner_{index}/1/{field}"
            for index in range(2)
            for field in ("tooltip", "custom_fields")
        ]
        assert [change["path"] for change in result["changes"]] == expected_paths[:max_changes]
        assert all(change["occurrence_count"] == 1 for change in result["changes"])
    else:
        assert result["truncated"] is redacted_truncated
        assert [(change["path"], change["occurrence_count"]) for change in result["changes"]] == [
            (f"/dag/task_group/children/*/1/children/*/1/{field}", count) for field, count in expected_counts
        ]
        assert "secret_" not in json.dumps(result)
    for payload in (base, target):
        outer = payload["dag"]["task_group"]["children"]["secret_outer"][1]
        outer["children"] = dict(reversed(outer["children"].items()))
    assert result == build_serialized_dag_diff(
        base_data=base, target_data=target, include_values=include_values, max_changes=max_changes
    )


def test_build_diff_is_deterministic_and_normalizes_order() -> None:
    base = _build_payload(
        tasks=[{"task_id": "extract", "retries": 1}, {"task_id": "load", "retries": 1}],
        tags=["one", "two"],
        dependencies=[
            {
                "dependency_type": "task",
                "dependency_id": "extract-load",
                "source": "extract",
                "target": "load",
                "label": "extract-load",
            }
        ],
    )
    target = _build_payload(
        tasks=[{"task_id": "load", "retries": 1}, {"task_id": "extract", "retries": 1}],
        tags=["two", "one"],
        dependencies=[
            {
                "label": "extract-load",
                "target": "load",
                "source": "extract",
                "dependency_id": "extract-load",
                "dependency_type": "task",
            }
        ],
    )

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["mode"] == "observed_state"
    assert result["serialized_dag_schema_versions"] == {"base": 3, "target": 3}
    assert result["changes"] == []
    assert result["truncated"] is False


def test_build_diff_reports_categories_digests_and_values() -> None:
    base = _build_payload(tasks=[{"task_id": "extract", "retries": 1}], tags=["old"])
    target = _build_payload(
        tasks=[
            {"task_id": "extract", "retries": 2},
            {"task_id": "load", "retries": 1},
        ],
        tags=["new"],
        schedule="hourly",
    )

    result = build_serialized_dag_diff(
        base_data=base,
        target_data=target,
        base_provenance={"bundle_version": "one"},
        target_provenance={"bundle_version": "two"},
        include_values=True,
    )

    changes = {change["path"]: change for change in result["changes"]}
    assert changes["/dag/tasks/extract/retries"]["category"] == "task"
    assert changes["/dag/tasks/extract/retries"]["impact"] == "execution"
    assert changes["/dag/tasks/extract/retries"]["before_value"] == 1
    assert changes["/dag/tasks/extract/retries"]["after_value"] == 2
    assert changes["/dag/tasks/extract/retries"]["before_digest"].startswith("sha256:")
    assert changes["/dag/tasks/load"]["operation"] == "added"
    assert changes["/dag/schedule"]["category"] == "schedule"
    assert changes["/dag/tags/old"]["operation"] == "removed"
    assert changes["/dag/tags/new"]["operation"] == "added"
    assert changes["/dag/tags/old"]["category"] == "metadata"
    assert changes["/provenance/bundle_version"]["category"] == "provenance"
    assert changes["/provenance/bundle_version"]["impact"] == "provenance"


def test_build_diff_digests_escaped_surrogates() -> None:
    base = _build_payload(tasks=[{"task_id": "extract", "doc_md": "café"}])
    # An escaped lone surrogate survives ensure_ascii=True storage and the compressed column.
    target = _build_payload(tasks=[{"task_id": "extract", "doc_md": "\ud800"}])
    assert json.loads(json.dumps(target)) == target

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["after_digest"] == "sha256:" + hashlib.sha256(b'"\xed\xa0\x80"').hexdigest()
    # Payloads that already digested keep their digest, so the wire format is unchanged.
    unescaped = hashlib.sha256(json.dumps("café", ensure_ascii=False).encode()).hexdigest()
    assert change["before_digest"] == f"sha256:{unescaped}"


def _get_expected_digest(canonical_json: bytes) -> str:
    return f"sha256:{hashlib.sha256(canonical_json).hexdigest()}"


def test_build_diff_pins_authorized_change_record_keys() -> None:
    base = _build_payload(tasks=[{"task_id": "extract", "retries": 1}], tags=["old"])
    target = _build_payload(tasks=[{"task_id": "extract", "retries": 2}], tags=["new"])

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["changes"] == [
        {
            "path": "/dag/tags/new",
            "operation": "added",
            "category": "metadata",
            "impact": "metadata",
            "occurrence_count": 1,
            "before_digest": None,
            "after_digest": _get_expected_digest(b'"new"'),
            "after_value": "new",
        },
        {
            "path": "/dag/tags/old",
            "operation": "removed",
            "category": "metadata",
            "impact": "metadata",
            "occurrence_count": 1,
            "before_digest": _get_expected_digest(b'"old"'),
            "after_digest": None,
            "before_value": "old",
        },
        {
            "path": "/dag/tasks/extract/retries",
            "operation": "changed",
            "category": "task",
            "impact": "execution",
            "occurrence_count": 1,
            "before_digest": _get_expected_digest(b"1"),
            "after_digest": _get_expected_digest(b"2"),
            "before_value": 1,
            "after_value": 2,
        },
    ]


def test_build_diff_bounds_changes_and_reports_truncation() -> None:
    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[{"task_id": "extract", "retries": 1}], tags=["old"]),
        target_data=_build_payload(tasks=[{"task_id": "extract", "retries": 2}], tags=["new"]),
        max_changes=1,
    )

    assert len(result["changes"]) == 1
    assert result["truncated"] is True


def test_build_diff_reports_dependency_changes() -> None:
    dependency = {
        "dependency_type": "sensor",
        "dependency_id": "upstream-task",
        "source": "upstream-dag",
        "target": "example",
        "label": "upstream-task",
    }

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[], dependencies=[dependency]),
        target_data=_build_payload(tasks=[]),
    )

    assert len(result["changes"]) == 1
    assert result["changes"][0]["category"] == "dependency"
    assert result["changes"][0]["impact"] == "execution"


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_ignores_dependency_label_renames(include_values):
    payloads = []
    for display_name in ("Before", "After"):
        with DAG("dependency_labels", schedule=None) as dag:
            TriggerDagRunOperator(task_id="fire", trigger_dag_id="downstream", task_display_name=display_name)
            ExternalTaskSensor(task_id="watch", external_dag_id="upstream", task_display_name=display_name)
        payloads.append(_serialize_dag(dag))
    base_dependencies, target_dependencies = (payload["dag"]["dag_dependencies"] for payload in payloads)
    assert [dependency["label"] for dependency in base_dependencies] == ["Before", "Before"]
    assert [dependency["label"] for dependency in target_dependencies] == ["After", "After"]
    # Every edge keeps the identity DagDependency.node_id is built from.
    assert [
        {key: value for key, value in dependency.items() if key != "label"}
        for dependency in base_dependencies
    ] == [
        {key: value for key, value in dependency.items() if key != "label"}
        for dependency in target_dependencies
    ]

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    task_ids = ["fire", "watch"] if include_values else ["*"]
    assert [
        (change["path"], change["operation"], change["category"], change["impact"])
        for change in result["changes"]
    ] == [
        (f"/dag/tasks/{task_id}/_task_display_name", "changed", "metadata", "metadata")
        for task_id in task_ids
    ]


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_ignores_downstream_id_reordering(include_values):
    """
    A reordered id list is the same edges, not a change.

    _deserialize_operator_field turns ``downstream_task_ids`` into a set, so the order a producer
    happened to store carries no meaning and must not read as a dependency change.
    """
    payloads = [
        _build_payload(tasks=[{"task_id": "head", "downstream_task_ids": ids}])
        for ids in (["a", "b"], ["b", "a"])
    ]

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_reports_a_changed_downstream_id_set(include_values):
    payloads = [
        _build_payload(tasks=[{"task_id": "head", "downstream_task_ids": ids}])
        for ids in (["a", "b"], ["b", "c"])
    ]

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    task_id = "head" if include_values else "*"
    assert [(change["path"], change["category"], change["impact"]) for change in result["changes"]] == [
        (f"/dag/tasks/{task_id}/downstream_task_ids", "dependency", "execution")
    ]


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_ignores_task_group_id_reordering(include_values):
    """A task group's id lists hydrate to sets too, and declare no template fields."""
    payloads = []
    for order in (["g2", "g3"], ["g3", "g2"]):
        payload = _build_payload(tasks=[])
        payload["dag"]["task_group"] = {
            "_group_id": None,
            "children": {"g1": ["taskgroup", {"_group_id": "g1", "downstream_group_ids": order}]},
        }
        payloads.append(payload)

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize(
    ("path", "is_group_field"),
    [
        (("dag", "task_group", "downstream_group_ids"), True),
        (("dag", "task_group", "children", "g1", "downstream_group_ids"), True),
        (("dag", "task_group", "children", "g1", "children", "g2", "upstream_task_ids"), True),
        # A group whose own id happens to be "children" is still a group.
        (("dag", "task_group", "children", "children", "downstream_group_ids"), True),
        # User data inside a mapped group's expansion is not a dependency list.
        (
            ("dag", "task_group", "children", "g1", "expand_input", "value", "__var", "downstream_task_ids"),
            False,
        ),
        (
            (
                "dag",
                "task_group",
                "children",
                "g1",
                "expand_input",
                "__var",
                "children",
                "x",
                "upstream_task_ids",
            ),
            False,
        ),
        (("dag", "tasks", "extract", "downstream_task_ids"), False),
        ((), False),
    ],
)
def test_group_dependency_path_matches_only_a_group_field(path, is_group_field):
    assert dag_version_diff._is_group_dependency_path(path) is is_group_field


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_reports_a_mapped_group_expand_input_change(include_values):
    """
    A mapped group's expand_input is user data, not a dependency list.

    Its order and multiplicity decide how many task instances run, so normalizing a group's
    dependency ids must not reach an argument that happens to share one of those names.
    """
    payloads = []
    for values in (["a", "b"], ["a", "b", "a"]):
        with DAG("mapped_group", schedule=None) as dag:

            @sdk_task
            def work():
                return 1

            @task_group
            def grp(downstream_task_ids):
                work()

            grp.expand(downstream_task_ids=values)
        payloads.append(_serialize_dag(dag))
    stored = [
        payload["dag"]["task_group"]["children"]["grp"][1]["expand_input"]["value"]["__var"][
            "downstream_task_ids"
        ]
        for payload in payloads
    ]
    assert [len(value) for value in stored] == [2, 3], "the stored expansion count changes"

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    group_id = "grp" if include_values else "*"
    assert [(change["path"], change["impact"]) for change in result["changes"]] == [
        (f"/dag/task_group/children/{group_id}/1/expand_input", "execution")
    ]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "legacy"),
    [
        ("allowed_run_types", "omitted"),
        ("allowed_run_types", "empty list"),
        ("deadline", "omitted"),
    ],
)
def test_build_diff_ignores_fields_a_legacy_producer_left_unset(field, legacy, include_values):
    """
    Neither field has a schema default, so an older producer omits what the current one nulls.

    A diff spanning that upgrade would otherwise report an added execution change on every Dag.
    """
    with DAG("legacy_unset", schedule=None) as dag:
        EmptyOperator(task_id="extract")
    current = _serialize_dag(dag)
    assert current["dag"][field] is None, "the current serializer writes an explicit null"
    stored = copy.deepcopy(current)
    if legacy == "omitted":
        del stored["dag"][field]
    else:
        stored["dag"][field] = []
    hydrated = [
        getattr(DagSerialization.from_dict(copy.deepcopy(payload)), field) for payload in (stored, current)
    ]
    assert hydrated[0] == hydrated[1] is None

    result = build_serialized_dag_diff(base_data=stored, target_data=current, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_classifies_fail_fast_as_schedule() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["fail_fast"] = False
    target["dag"]["fail_fast"] = True

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/fail_fast"
    assert change["operation"] == "changed"
    assert change["category"] == "schedule"
    assert change["impact"] == "execution"


@pytest.mark.parametrize(
    ("base_data", "target_data", "reason"),
    [
        (None, _build_payload(tasks=[]), "serialized_dag_missing"),
        (
            {"__version": 99, "dag": {}},
            _build_payload(tasks=[]),
            "unsupported_serialized_dag_schema_version:99",
        ),
        ({"__version": 3, "dag": []}, _build_payload(tasks=[]), "serialized_dag_canonicalization_failed"),
        (
            {
                "__version": 1,
                "dag": {
                    "dag_id": "example",
                    "tasks": [],
                    "task_group": {},
                    "schedule_interval": 1,
                },
            },
            _build_payload(tasks=[]),
            "serialized_dag_canonicalization_failed",
        ),
        (
            {
                "__version": 1,
                "dag": {
                    "dag_id": "example",
                    "tasks": [],
                    "task_group": {},
                    "schedule_interval": {"__type": "timedelta", "__var": 10**20},
                },
            },
            _build_payload(tasks=[]),
            "serialized_dag_canonicalization_failed",
        ),
    ],
)
def test_build_diff_returns_unavailable_for_unsafe_inputs(base_data, target_data, reason, caplog) -> None:
    result = build_serialized_dag_diff(base_data=base_data, target_data=target_data)

    assert result["mode"] == "unavailable"
    assert result["changes"] == []
    assert result["unavailable_reason"] == reason
    if reason == "serialized_dag_canonicalization_failed":
        assert {
            "event": "Serialized Dag diff canonicalization failed",
            "base_schema_version": base_data["__version"],
            "target_schema_version": target_data["__version"],
        } in caplog


@pytest.mark.parametrize(
    "base_data",
    [
        pytest.param({"dag": {}}, id="absent-version"),
        pytest.param({"__version": True, "dag": {}}, id="boolean-version-is-not-version-1"),
        pytest.param({"__version": "3", "dag": {}}, id="string-version"),
        pytest.param({"__version": 3.0, "dag": {}}, id="float-version"),
        pytest.param(["not", "a", "mapping"], id="non-mapping-payload"),
    ],
)
def test_build_diff_returns_unavailable_without_a_usable_schema_version(base_data) -> None:
    result = build_serialized_dag_diff(base_data=base_data, target_data=_build_payload(tasks=[]))

    assert result["mode"] == "unavailable"
    assert result["unavailable_reason"] == "serialized_dag_schema_version_missing"
    assert result["serialized_dag_schema_versions"] == {"base": None, "target": 3}
    assert result["changes"] == []
    assert result["values"] == {"status": "unavailable"}


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("location", ["dag", "task", "provenance"])
def test_build_diff_discards_changes_when_json_serialization_fails(location, include_values, caplog):
    base = _build_payload(tasks=[{"task_id": "extract"}])
    target = copy.deepcopy(base)
    base["dag"]["catchup"] = True
    target["dag"]["catchup"] = False
    base_provenance = {}
    target_provenance = {}
    if location == "provenance":
        before, after = base_provenance, target_provenance
    elif location == "task":
        before, after = base["dag"]["tasks"][0]["__var"], target["dag"]["tasks"][0]["__var"]
    else:
        before, after = base["dag"], target["dag"]
    before["opaque"] = None
    after["opaque"] = datetime(2026, 1, 1, tzinfo=timezone.utc)

    result = build_serialized_dag_diff(
        base_data=base,
        target_data=target,
        base_provenance=base_provenance,
        target_provenance=target_provenance,
        include_values=include_values,
    )

    assert result["mode"] == "unavailable"
    assert result["unavailable_reason"] == "serialized_dag_json_encoding_failed"
    assert result["changes"] == []
    assert result["truncated"] is False
    assert {
        "event": "Serialized Dag diff JSON encoding failed",
        "base_schema_version": 3,
        "target_schema_version": 3,
    } in caplog


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("failed_payload", ["base", "target"])
@mock.patch.object(dag_version_diff, "_canonicalize_payload_v1", autospec=True)
def test_build_diff_returns_unavailable_on_canonicalization_recursion(
    canonicalize, failed_payload, include_values, caplog
):
    error = RecursionError("secret payload")
    canonicalize.side_effect = [error] if failed_payload == "base" else [{}, error]

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[]),
        target_data=_build_payload(tasks=[]),
        include_values=include_values,
    )

    assert result["mode"] == "unavailable"
    assert result["unavailable_reason"] == "serialized_dag_canonicalization_failed"
    assert result["changes"] == []
    assert result["truncated"] is False
    assert result["values"]["status"] == "unavailable"
    assert {"event": "Serialized Dag diff canonicalization failed", "error_type": "RecursionError"} in caplog


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("after_first_change", [False, True])
@mock.patch.object(dag_version_diff, "_collect_changes", autospec=True)
def test_build_diff_discards_changes_on_collection_recursion(
    collect, after_first_change, include_values, caplog
):
    def fail_collection(before, after, *, path, collector):
        if after_first_change:
            collector.add(path=("dag", "catchup"), operation="changed", before=False, after=True)
        raise RecursionError("secret payload")

    collect.side_effect = fail_collection

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[]),
        target_data=_build_payload(tasks=[]),
        include_values=include_values,
    )

    assert result["mode"] == "unavailable"
    assert result["unavailable_reason"] == "serialized_dag_recursion_limit_exceeded"
    assert result["changes"] == []
    assert result["truncated"] is False
    assert result["values"]["status"] == "unavailable"
    assert "Serialized Dag diff recursion limit exceeded" in caplog


@pytest.mark.parametrize("error_type", [AttributeError, KeyError, RuntimeError, TypeError, ValueError])
@mock.patch.object(dag_version_diff, "_get_category", autospec=True)
def test_build_diff_propagates_comparison_bugs(mock_get_category, error_type):
    mock_get_category.side_effect = error_type("comparison bug")
    base = _build_payload(tasks=[])
    target = copy.deepcopy(base)
    target["dag"]["catchup"] = False

    with pytest.raises(error_type, match="comparison bug"):
        build_serialized_dag_diff(base_data=base, target_data=target)


@pytest.mark.parametrize(
    ("max_changes", "expected_message"),
    [
        (0, "max_changes must be a positive integer"),
        (1.5, "max_changes must be a positive integer"),
        (1.0, "max_changes must be a positive integer"),
        (float("nan"), "max_changes must be a positive integer"),
        (float("inf"), "max_changes must be a positive integer"),
        (True, "max_changes must be a positive integer"),
        ("1", "max_changes must be a positive integer"),
        (None, "max_changes must be a positive integer"),
        ([], "max_changes must be a positive integer"),
        (MAX_ALLOWED_CHANGES + 1, f"max_changes must not exceed {MAX_ALLOWED_CHANGES}"),
    ],
)
def test_build_diff_rejects_invalid_change_bounds(max_changes, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        build_serialized_dag_diff(
            base_data=_build_payload(tasks=[]), target_data=_build_payload(tasks=[]), max_changes=max_changes
        )


def test_build_diff_redacts_sensitive_data_without_values() -> None:
    dependency = {
        "dependency_type": "sensor",
        "dependency_id": "secret-dependency",
        "source": "secret-upstream",
        "target": "example",
        "label": "secret-label",
    }
    result = build_serialized_dag_diff(
        base_data=_build_payload(
            tasks=[{"task_id": "secret-task", "retries": 1}],
            tags=["secret-old-tag"],
            dependencies=[dependency],
        ),
        target_data=_build_payload(
            tasks=[
                {"task_id": "secret-task", "retries": 2},
                {"task_id": "secret-new-task", "retries": 1},
            ],
            tags=["secret-new-tag"],
        ),
    )

    assert result["mode"] == "observed_state"
    assert any(change["path"] == "/dag/tasks/*/retries" for change in result["changes"])
    assert any(change["path"] == "/dag/tasks/*" for change in result["changes"])
    assert any(change["path"] == "/dag/tags/*" for change in result["changes"])
    assert any(change["path"] == "/dag/dag_dependencies/*" for change in result["changes"])
    assert all(
        "before_digest" not in change
        and "after_digest" not in change
        and "before_value" not in change
        and "after_value" not in change
        for change in result["changes"]
    )
    encoded_result = json.dumps(result)
    for sensitive_value in (
        "secret-task",
        "secret-new-task",
        "secret-old-tag",
        "secret-new-tag",
        "secret-dependency",
        "secret-upstream",
        "secret-label",
    ):
        assert sensitive_value not in encoded_result


def test_build_diff_collapses_custom_task_fields_without_values() -> None:
    custom_field = "secret_callback_field"
    base_task = {
        "task_id": "extract",
        "template_fields": [custom_field],
        custom_field: "old-secret",
    }
    target_task = {
        "task_id": "extract",
        "template_fields": [custom_field],
        custom_field: "new-secret",
    }

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[base_task]),
        target_data=_build_payload(tasks=[target_task]),
    )

    assert result["changes"] == [
        {
            "path": "/dag/tasks/*/custom_fields",
            "operation": "changed",
            "category": "task",
            "impact": "execution",
            "occurrence_count": 1,
        }
    ]
    encoded_result = json.dumps(result)
    assert custom_field not in encoded_result
    assert "old-secret" not in encoded_result
    assert "new-secret" not in encoded_result


def test_build_diff_includes_custom_task_fields_with_values() -> None:
    custom_field = "secret_callback_field"
    base_task = {
        "task_id": "extract",
        "template_fields": [custom_field],
        custom_field: "old-secret",
    }
    target_task = {
        "task_id": "extract",
        "template_fields": [custom_field],
        custom_field: "new-secret",
    }

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[base_task]),
        target_data=_build_payload(tasks=[target_task]),
        include_values=True,
    )

    # Authorization decides what a change carries, not which changes exist, so custom fields stay
    # aggregated into the one record the redacted walk produces and the values ride on it.
    change = next(change for change in result["changes"] if change["path"].endswith("custom_fields"))
    assert change["path"] == "/dag/tasks/extract/custom_fields"
    assert change["category"] == "task"
    assert change["impact"] == "execution"
    assert change["before_value"] == {custom_field: "old-secret"}
    assert change["after_value"] == {custom_field: "new-secret"}


@pytest.mark.parametrize(
    (
        "base_data",
        "target_data",
        "hidden_identifier",
        "expected_path",
        "expected_operation",
        "expected_category",
        "expected_impact",
    ),
    [
        pytest.param(
            _build_payload(tasks=[{"task_id": "deadline", "retries": 1}]),
            _build_payload(tasks=[{"task_id": "deadline", "retries": 2}]),
            "deadline",
            "/dag/tasks/*/retries",
            "changed",
            "task",
            "execution",
            id="task-id",
        ),
        pytest.param(
            _build_payload(tasks=[]),
            _build_payload(tasks=[], tags=["deadline"]),
            "deadline",
            "/dag/tags/*",
            "added",
            "metadata",
            "metadata",
            id="tag",
        ),
        pytest.param(
            _build_payload(tasks=[]),
            _build_payload(
                tasks=[],
                dependencies=[
                    {
                        "dependency_type": "sensor",
                        "dependency_id": "upstream-task",
                        "source": "upstream-dag",
                        "target": "example",
                        "label": "secret_callback",
                    }
                ],
            ),
            "secret_callback",
            "/dag/dag_dependencies/*",
            "added",
            "dependency",
            "execution",
            id="dependency",
        ),
    ],
)
def test_build_diff_classification_ignores_redacted_identifiers(
    base_data,
    target_data,
    hidden_identifier,
    expected_path,
    expected_operation,
    expected_category,
    expected_impact,
) -> None:
    result = build_serialized_dag_diff(base_data=base_data, target_data=target_data)

    assert result["changes"] == [
        {
            "path": expected_path,
            "operation": expected_operation,
            "category": expected_category,
            "impact": expected_impact,
            "occurrence_count": 1,
        }
    ]
    assert hidden_identifier not in json.dumps(result)


def test_build_diff_preserves_known_task_field_classification_without_values() -> None:
    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[{"task_id": "extract", "outlets": []}]),
        target_data=_build_payload(tasks=[{"task_id": "extract", "outlets": ["asset"]}]),
    )

    assert result["changes"] == [
        {
            "path": "/dag/tasks/*/outlets",
            "operation": "changed",
            "category": "asset",
            "impact": "execution",
            "occurrence_count": 1,
        }
    ]


def test_build_diff_collapses_arbitrary_mappings_without_values() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["default_args"] = {"secret-argument": "old-secret"}
    target["dag"]["default_args"] = {"secret-argument": "new-secret"}

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["changes"] == [
        {
            "path": "/dag/default_args",
            "operation": "changed",
            "category": "task",
            "impact": "execution",
            "occurrence_count": 1,
        }
    ]


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_never_names_a_user_chosen_default_args_key(include_values):
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["default_args"] = {"__type": "dict", "__var": {"secret_argument": "old"}}
    target["dag"]["default_args"] = {"__type": "dict", "__var": {"secret_argument": "new"}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/default_args/custom_fields"
    assert (change["operation"], change["category"], change["impact"]) == (
        "changed",
        "task",
        "execution",
    )
    if not include_values:
        assert "secret_argument" not in json.dumps(result)


def test_build_diff_keeps_default_args_conservative_for_a_mixed_edit() -> None:
    """A metadata key and an unrecognised one are separate records, so neither impact is lost."""
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["default_args"] = {"__type": "dict", "__var": {"owner": "alice", "secret": "old"}}
    target["dag"]["default_args"] = {"__type": "dict", "__var": {"owner": "bob", "secret": "new"}}

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert [(change["path"], change["category"], change["impact"]) for change in result["changes"]] == [
        ("/dag/default_args/owner", "metadata", "metadata"),
        ("/dag/default_args/custom_fields", "task", "execution"),
    ]
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_collapses_unknown_root_sections(include_values):
    base = _build_payload(tasks=[])
    target = copy.deepcopy(base)
    base["secret_root_section"] = {"secret-argument": "old-secret"}
    target["secret_root_section"] = {"secret-argument": "new-secret"}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == ("/secret_root_section" if include_values else "/custom_fields")
    assert (change["operation"], change["category"], change["impact"]) == (
        "changed",
        "unknown",
        "unknown",
    )
    if not include_values:
        assert "secret" not in json.dumps(result)


def test_build_diff_reports_unkeyed_lists_as_one_stable_change() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["custom_list"] = [{"name": "old"}]
    target["dag"]["custom_list"] = [{"name": "new"}]

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/custom_list"
    assert change["operation"] == "changed"
    assert change["category"] == "unknown"
    assert change["impact"] == "unknown"
    assert "before_digest" not in change
    assert "after_digest" not in change


def test_build_diff_reports_deadline_lists_as_one_stable_change() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["deadline"] = [{"name": "old", "interval": 60}]
    target["dag"]["deadline"] = [{"name": "new", "interval": 60}]

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/deadline"
    assert change["operation"] == "changed"
    assert change["category"] == "deadline"
    assert change["impact"] == "execution"


def test_build_diff_preserves_order_sensitive_string_lists() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["template_searchpath"] = ["first", "second"]
    target["dag"]["template_searchpath"] = ["second", "first"]

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/template_searchpath"
    assert change["operation"] == "changed"


@pytest.mark.parametrize(
    ("base_task", "target_task"),
    [
        ({"task_id": "extract", "retries": 2}, {"task_id": "extract"}),
        (
            {"task_id": "extract", "_is_mapped": True, "retries": 2, "partial_kwargs": {"retries": 2}},
            {"task_id": "extract", "_is_mapped": True, "partial_kwargs": {}},
        ),
    ],
)
def test_build_diff_normalizes_v3_client_defaults(base_task, target_task) -> None:
    base = _build_payload(tasks=[base_task])
    base["__version"] = 2
    target = _build_payload(tasks=[target_task])
    target["client_defaults"] = {"tasks": {"retries": 2}}

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["serialized_dag_schema_versions"] == {"base": 2, "target": 3}
    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("schema_version", [2, 3])
@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "value"), [("retries", 0), ("depends_on_past", False), ("ui_fgcolor", "#000")]
)
def test_build_diff_normalizes_schema_defaults(schema_version, include_values, field, value) -> None:
    base = _build_payload(tasks=[{"task_id": "extract", field: value}])
    base["__version"] = schema_version
    target = _build_payload(tasks=[{"task_id": "extract"}])

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("is_mapped", [False, True])
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_normalizes_explicit_schema_default_overrides(is_mapped, include_values) -> None:
    base_task: dict[str, Any] = {"task_id": "extract"}
    target_task: dict[str, Any] = {"task_id": "extract", "retries": 0}
    if is_mapped:
        base_task.update(_is_mapped=True, partial_kwargs={})
        target_task = {"task_id": "extract", "_is_mapped": True, "partial_kwargs": {"retries": 0}}
    base = _build_payload(tasks=[base_task])
    target = _build_payload(tasks=[target_task])
    target["client_defaults"] = {"tasks": {"retries": 3}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
def test_diff_selects_mapping_from_the_raw_operator(include_values):
    with DAG("diff_regression", schedule=None) as dag:
        BashOperator.partial(task_id="extract").expand(bash_command=["first", "second"])
    target = _serialize_dag(dag)
    operator_schema = {"$ref": "#/definitions/operator", "definitions": load_dag_schema_dict()["definitions"]}
    task = _get_task(target)
    schema_defaults = DagSerialization.get_schema_defaults("operator")
    for field in operator_schema["definitions"]["operator"]["required"]:
        if field not in task:
            task[field] = schema_defaults[field]
    defaults = target.setdefault("client_defaults", {}).setdefault("tasks", {})
    mapping_fields = ("_is_mapped", "partial_kwargs", "expand_input")
    defaults.update({field: copy.deepcopy(task[field]) for field in mapping_fields})
    base = copy.deepcopy(target)
    for field in mapping_fields:
        _get_task(base).pop(field)
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        Draft7Validator(operator_schema).validate(_get_task(payload))
    restored = [
        DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"] for payload in (base, target)
    ]
    assert restored[0].is_mapped is False
    assert restored[1].is_mapped is True

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    task_id = "extract" if include_values else "*"
    change = next(
        change for change in result["changes"] if change["path"] == f"/dag/tasks/{task_id}/_is_mapped"
    )
    assert change["impact"] == "execution"
    if include_values:
        assert change["before_value"] is False
        assert change["after_value"] is True


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_ignores_shadowed_mapped_client_defaults(include_values) -> None:
    task = {"task_id": "extract", "_is_mapped": True, "partial_kwargs": {"retries": 5}}
    base = _build_payload(tasks=[task])
    target = _build_payload(tasks=[task])
    target["client_defaults"] = {"tasks": {"retries": 3}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_reports_effective_mapped_default_changes() -> None:
    task = {"task_id": "extract", "_is_mapped": True, "partial_kwargs": {}}
    base = _build_payload(tasks=[task])
    base["client_defaults"] = {"tasks": {"retries": 3}}
    target = _build_payload(tasks=[task])
    target["client_defaults"] = {"tasks": {"retries": 5}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/tasks/extract/partial_kwargs/retries"
    assert change["operation"] == "changed"
    assert change["before_value"] == 3
    assert change["after_value"] == 5


def _build_mapped_precedence_payload(
    *, field: str, top_level_value: int | str, client_default: int | str, partial_kwargs: dict | None
) -> dict:
    with DAG("mapped_precedence", schedule=None) as dag:
        BashOperator.partial(task_id="mapped").expand(bash_command=["first", "second"])
    payload = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    task = payload["dag"]["tasks"][0]["__var"]
    task.pop("partial_kwargs", None)
    task[field] = top_level_value
    if partial_kwargs is not None:
        task["partial_kwargs"] = partial_kwargs
    payload["client_defaults"] = {"tasks": {field: client_default}}
    return payload


def _resolve_runtime_partial_value(payload: dict, field: str) -> int | str:
    hydrated = DagSerialization.from_dict(copy.deepcopy(payload))
    return hydrated.task_dict["mapped"].partial_kwargs[field]


@pytest.mark.parametrize(
    ("field", "before", "after", "client_default"),
    [("retries", 2, 3, 5), ("pool", "base_pool", "target_pool", "client_pool")],
)
def test_build_diff_reports_top_level_shadowing_without_partial_kwargs(field, before, after, client_default):
    base = _build_mapped_precedence_payload(
        field=field, top_level_value=before, client_default=client_default, partial_kwargs=None
    )
    target = _build_mapped_precedence_payload(
        field=field, top_level_value=after, client_default=client_default, partial_kwargs=None
    )
    # populate_operator only folds client defaults into partial_kwargs for a payload that carries
    # the key, so without it the top-level values survive and the two versions really do differ.
    assert (_resolve_runtime_partial_value(base, field), _resolve_runtime_partial_value(target, field)) == (
        before,
        after,
    )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["mode"] == "observed_state"
    assert [
        (change["path"], change["before_value"], change["after_value"]) for change in result["changes"]
    ] == [(f"/dag/tasks/mapped/partial_kwargs/{field}", before, after)]


@pytest.mark.parametrize(
    ("field", "before", "after", "client_default"),
    [("retries", 2, 3, 5), ("pool", "base_pool", "target_pool", "client_pool")],
)
def test_build_diff_ignores_top_level_shadowing_behind_empty_partial_kwargs(
    field, before, after, client_default
):
    base = _build_mapped_precedence_payload(
        field=field, top_level_value=before, client_default=client_default, partial_kwargs={}
    )
    target = _build_mapped_precedence_payload(
        field=field, top_level_value=after, client_default=client_default, partial_kwargs={}
    )
    # The present key makes populate_operator apply the client default instead, shadowing both
    # top-level values, so the versions resolve identically and the diff must stay silent.
    assert (_resolve_runtime_partial_value(base, field), _resolve_runtime_partial_value(target, field)) == (
        client_default,
        client_default,
    )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(("field", "default_type"), [("retry_delay", timedelta), ("start_date", datetime)])
def test_diff_distinguishes_implicit_typed_defaults_from_template_numbers(
    include_values, field, default_type
):
    with DAG("diff_regression", schedule=None, start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)) as dag:
        BashOperator(task_id="extract", bash_command="echo hello")
    base = _serialize_dag(dag)
    _get_task(base)["template_fields"].append(field)
    _get_task(base).pop(field, None)
    base.get("client_defaults", {}).get("tasks", {}).pop(field, None)
    target = copy.deepcopy(base)
    value = (
        base["dag"][field]
        if field == "start_date"
        else DagSerialization.get_schema_defaults("operator")[field]
    )
    _get_task(target)[field] = value
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
    restored = [
        DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"] for payload in (base, target)
    ]
    assert isinstance(getattr(restored[0], field), default_type)
    assert type(getattr(restored[1], field)) is float

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    task_id = "extract" if include_values else "*"
    assert change["path"] == f"/dag/tasks/{task_id}/{field}"
    assert change["impact"] == "execution"
    if include_values:
        assert change["before_digest"] != change["after_digest"]


@pytest.mark.parametrize("field", sorted(_OPERATOR_TIMEDELTA_FIELDS))
@pytest.mark.parametrize("duration_source", ["encoded_partial", "plain_partial", "client_default"])
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_distinguishes_mapped_template_numbers_from_timedeltas(
    field, duration_source, include_values
):
    with DAG("example", schedule=None) as dag:
        BashOperator.partial(task_id="extract").expand(bash_command=["echo hello"])
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    base.get("client_defaults", {}).get("tasks", {}).pop(field, None)
    base_task = base["dag"]["tasks"][0]["__var"]
    base_task["partial_kwargs"].pop(field, None)
    base_task["template_fields"].append(field)
    base_task[field] = 60.0
    target = copy.deepcopy(base)
    target_task = target["dag"]["tasks"][0]["__var"]
    target_task.pop(field)
    if duration_source == "client_default":
        target.setdefault("client_defaults", {}).setdefault("tasks", {})[field] = 60.0
    else:
        target_task["partial_kwargs"][field] = (
            {"__type": "timedelta", "__var": 60.0} if duration_source == "encoded_partial" else 60.0
        )
    stored_payloads = copy.deepcopy((base, target))
    for payload, expected in ((base, 60.0), (target, timedelta(seconds=60))):
        DagSerialization.validate_schema(payload)
        actual = getattr(DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"], field)
        assert actual == expected
        assert type(actual) is type(expected)

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    task_id = "extract" if include_values else "*"
    assert change["path"] == f"/dag/tasks/{task_id}/partial_kwargs/{field}"
    assert change["operation"] == "changed"
    if include_values:
        assert change["before_value"] == 60.0
        assert change["after_value"] == {"__type": "timedelta", "__var": 60.0}
    assert (base, target) == stored_payloads


@pytest.mark.parametrize("field", sorted(_OPERATOR_TIMEDELTA_FIELDS))
@pytest.mark.parametrize("duration_source", ["encoded_partial", "plain_partial", "client_default"])
def test_build_diff_normalizes_mapped_timedelta_defaults(field, duration_source) -> None:
    with DAG("example", schedule=None) as dag:
        BashOperator.partial(task_id="extract").expand(bash_command=["echo hello"])
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    base.get("client_defaults", {}).get("tasks", {}).pop(field, None)
    base_task = base["dag"]["tasks"][0]["__var"]
    base_task["template_fields"].append(field)
    base_task[field] = 60.0
    base_task["partial_kwargs"][field] = {"__type": "timedelta", "__var": 60.0}
    target = copy.deepcopy(base)
    target_task = target["dag"]["tasks"][0]["__var"]
    target_task[field] = 120.0
    if duration_source == "client_default":
        target_task["partial_kwargs"].pop(field)
        target.setdefault("client_defaults", {}).setdefault("tasks", {})[field] = 60.0
    elif duration_source == "plain_partial":
        target_task["partial_kwargs"][field] = 60.0
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        assert getattr(
            DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"], field
        ) == timedelta(seconds=60)

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize(
    "field",
    sorted(_OPERATOR_TIMEDELTA_FIELDS & DagSerialization.get_schema_defaults("operator").keys()),
)
@pytest.mark.parametrize("stores_partial_kwargs", [True, False])
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_normalizes_mapped_timedelta_schema_defaults(field, stores_partial_kwargs, include_values):
    default = DagSerialization.get_schema_defaults("operator")[field]
    with DAG("example", schedule=None) as dag:
        BashOperator.partial(task_id="extract").expand(bash_command=["echo hello"])
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    base.get("client_defaults", {}).get("tasks", {}).pop(field, None)
    base_task = base["dag"]["tasks"][0]["__var"]
    base_task["partial_kwargs"].pop(field, None)
    if not stores_partial_kwargs:
        base_task.pop("partial_kwargs")
    target = copy.deepcopy(base)
    target["dag"]["tasks"][0]["__var"].setdefault("partial_kwargs", {})[field] = default
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        assert getattr(
            DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"], field
        ) == timedelta(seconds=default)

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
def test_diff_preserves_mapped_resource_types(include_values):
    resources = Resources(cpus=1, ram=512, disk=512, gpus=0)
    payloads = []
    for value in (resources, resources.to_dict()):
        with DAG("diff_regression", schedule=None) as dag:
            task = BashOperator.partial(task_id="extract", resources=value).expand(
                bash_command=["echo hello"]
            )
        payloads.append(_serialize_dag(dag))
        if isinstance(value, Resources):
            assert isinstance(task.unmap({"bash_command": "echo hello"}).resources, Resources)
        else:
            with pytest.raises(TypeError):
                task.unmap({"bash_command": "echo hello"})

    restored = [
        DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"] for payload in payloads
    ]
    assert isinstance(restored[0].resources, Resources)
    assert isinstance(restored[1].resources, dict)

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    task_id = "extract" if include_values else "*"
    assert change["path"] == f"/dag/tasks/{task_id}/partial_kwargs/resources"
    assert change["impact"] == "execution"
    if include_values:
        assert change["before_digest"] != change["after_digest"]
    else:
        assert "before_value" not in change
        assert "after_value" not in change


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("encoded_location", ["partial_kwargs", "top_level"])
@pytest.mark.parametrize(
    "executor_config",
    [
        {"queue": "same"},
        {"nested": {"limits": [{"cpu": 2}]}},
        {"nested": {"__type": "timedelta", "__var": 60.0}},
    ],
)
def test_build_diff_normalizes_mapped_plain_mappings(executor_config, encoded_location, include_values):
    with DAG("example", schedule=None) as dag:
        BashOperator.partial(task_id="extract", executor_config=executor_config).expand(
            bash_command=["echo hello"]
        )
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    if encoded_location == "top_level":
        encoded_task = base["dag"]["tasks"][0]["__var"]
        encoded_task["executor_config"] = encoded_task["partial_kwargs"].pop("executor_config")
    target = copy.deepcopy(base)
    target["dag"]["tasks"][0]["__var"]["partial_kwargs"]["executor_config"] = executor_config
    stored_payloads = copy.deepcopy((base, target))
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        assert (
            DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"].executor_config
            == executor_config
        )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []
    assert (base, target) == stored_payloads


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_preserves_literal_mapping_client_defaults(include_values):
    executor_config = {"__type": "dict", "__var": {"queue": "same"}}
    with DAG("example", schedule=None) as dag:
        BashOperator.partial(task_id="extract").expand(bash_command=["echo hello"])
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    base["dag"]["tasks"][0]["__var"]["partial_kwargs"].pop("executor_config", None)
    base["client_defaults"] = {"tasks": {"executor_config": executor_config}}
    target = copy.deepcopy(base)
    target["dag"]["tasks"][0]["__var"]["partial_kwargs"]["executor_config"] = OperatorSerialization.serialize(
        executor_config
    )
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        assert (
            DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"].executor_config
            == executor_config
        )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "value", "is_template"),
    [
        ("resources", {"cpus": 1}, False),
        ("execution_timeout", timedelta(seconds=60), False),
        ("executor_config", {"pod": {"cpu": 1}}, True),
    ],
)
def test_build_diff_normalizes_encoded_outer_mapped_fields(field, value, is_template, include_values):
    with DAG("example", schedule=None) as dag:
        BashOperator.partial(task_id="extract").expand(bash_command=["echo hello"])
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    base.get("client_defaults", {}).get("tasks", {}).pop(field, None)
    encoded_value = json.loads(json.dumps(BaseSerialization.serialize(value)))
    expected = encoded_value if is_template else value
    base_task = base["dag"]["tasks"][0]["__var"]
    if is_template:
        base_task["template_fields"].append(field)
    base_task["partial_kwargs"][field] = BaseSerialization.serialize(expected)
    target = copy.deepcopy(base)
    target_task = target["dag"]["tasks"][0]["__var"]
    target_task["partial_kwargs"].pop(field)
    target_task[field] = encoded_value
    for payload in (base, target):
        DagSerialization.validate_schema(payload)
        assert (
            getattr(DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"], field)
            == expected
        )

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize(
    "base_data",
    [
        {"__version": 3, "dag": {"tasks": []}, "client_defaults": []},
        {"__version": 3, "dag": {"tasks": []}, "client_defaults": {"tasks": []}},
        {"__version": 3, "dag": {"tasks": {}}, "client_defaults": {"tasks": {}}},
        {"__version": 3, "dag": {"tasks": [{}]}, "client_defaults": {"tasks": {}}},
        _build_payload(tasks=[{"task_id": "extract", "_is_mapped": True, "partial_kwargs": []}]),
    ],
)
def test_build_diff_rejects_malformed_client_defaults(base_data) -> None:
    result = build_serialized_dag_diff(base_data=base_data, target_data=_build_payload(tasks=[]))

    assert result["mode"] == "unavailable"
    assert result["changes"] == []
    assert result["unavailable_reason"] == "serialized_dag_canonicalization_failed"


def test_build_diff_ignores_exact_duplicate_dependencies() -> None:
    dependency = {
        "dependency_type": "task",
        "dependency_id": "extract-load",
        "source": "extract",
        "target": "load",
        "label": "extract-load",
    }

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[], dependencies=[dependency, dependency]),
        target_data=_build_payload(tasks=[], dependencies=[dependency]),
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_collapses_dependencies_differing_only_by_label() -> None:
    def build_dependency(label: str) -> dict:
        return {
            "dependency_type": "trigger",
            "dependency_id": "fire",
            "source": "example",
            "target": "downstream",
            "label": label,
        }

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[], dependencies=[build_dependency("Old"), build_dependency("New")]),
        target_data=_build_payload(tasks=[], dependencies=[build_dependency("Old")]),
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_dependency_keys_do_not_collide_on_delimiters() -> None:
    first_dependency = {
        "dependency_type": "trigger",
        "dependency_id": "id",
        "source": "a",
        "target": "b|c",
        "label": "d",
    }
    second_dependency = {
        "dependency_type": "trigger",
        "dependency_id": "id",
        "source": "a|b",
        "target": "c",
        "label": "d",
    }

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[], dependencies=[first_dependency, second_dependency]),
        target_data=_build_payload(tasks=[], dependencies=[second_dependency, first_dependency]),
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_detects_json_type_change() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["flag"] = True
    target["dag"]["flag"] = 1

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/flag"
    assert change["operation"] == "changed"


@pytest.mark.parametrize("include_values", [False, True])
def test_diff_preserves_task_template_named_type(include_values):
    payloads = []
    for value in ("old", "new"):
        with DAG("diff_regression", schedule=None) as dag:
            task = BashOperator(task_id="extract", bash_command="echo hello")
            task.template_fields = (*task.template_fields, "__type")
            setattr(task, "__type", value)
        payload = _serialize_dag(dag)
        assert (
            getattr(DagSerialization.from_dict(copy.deepcopy(payload)).task_dict["extract"], "__type")
            == value
        )
        payloads.append(payload)

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    task_id = "extract" if include_values else "*"
    change = result["changes"][0]
    assert change["path"] == f"/dag/tasks/{task_id}/custom_fields"
    assert change["impact"] == "execution"
    if include_values:
        assert change["before_value"]["__type"] == "old"
        assert change["after_value"]["__type"] == "new"
    else:
        assert "before_value" not in change
        assert "after_value" not in change


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("provenance_changed", [False, True])
def test_diff_ignores_wrapper_types_missing_on_both_sides(include_values, provenance_changed):
    base = _build_params_payload({})
    base["dag"]["tasks"][0].pop("__type")
    base["dag"].pop("task_group")
    target = copy.deepcopy(base)
    DagSerialization.validate_schema(base)
    assert DagSerialization.from_dict(copy.deepcopy(base)).task_dict == {}
    target_version = "new" if provenance_changed else "old"

    result = build_serialized_dag_diff(
        base_data=base,
        target_data=target,
        base_provenance={"bundle_version": "old"},
        target_provenance={"bundle_version": target_version},
        include_values=include_values,
        max_changes=1,
    )

    assert result["mode"] == "observed_state"
    assert [change["path"] for change in result["changes"]] == (
        ["/provenance/bundle_version"] if provenance_changed else []
    )
    assert result["truncated"] is False
    if provenance_changed and include_values:
        assert result["changes"][0]["before_value"] == "old"
        assert result["changes"][0]["after_value"] == "new"


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("operation", ["added", "removed"])
def test_diff_preserves_one_sided_wrapper_types(include_values, operation):
    base = _build_params_payload({})
    base["dag"].pop("task_group")
    target = copy.deepcopy(base)
    (base if operation == "added" else target)["dag"]["tasks"][0].pop("__type")

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    task_id = "extract" if include_values else "*"
    assert change["path"] == f"/dag/tasks/{task_id}/__type"
    assert change["operation"] == operation
    if include_values:
        present_side = "after" if operation == "added" else "before"
        missing_side = "before" if operation == "added" else "after"
        assert change[f"{present_side}_value"] == "operator"
        assert change[f"{missing_side}_digest"] is None
        assert f"{missing_side}_value" not in change


def test_build_diff_classifies_callback_as_execution() -> None:
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["on_failure_callback"] = "old"
    target["dag"]["on_failure_callback"] = "new"

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/on_failure_callback"
    assert change["category"] == "callback"
    assert change["impact"] == "execution"


def _build_v1_payload() -> dict:
    return {
        "__version": 1,
        "dag": {
            "_dag_id": "example",
            "fileloc": "/dags/example.py",
            "timezone": "UTC",
            "task_group": {},
            "tasks": [
                {
                    "__type": "operator",
                    "__var": {
                        "task_id": "extract",
                        "_task_type": "EmptyOperator",
                        "_task_module": "airflow.providers.standard.operators.empty",
                        "template_fields": [],
                        "ui_color": "#e8f7e4",
                    },
                }
            ],
            "dag_dependencies": [],
        },
    }


def _build_equivalent_v3_payload() -> dict:
    return {
        "__version": 3,
        "dag": {
            "dag_id": "example",
            "fileloc": "/dags/example.py",
            "timezone": "UTC",
            "timetable": {
                "__type": "airflow.timetables.simple.NullTimetable",
                "__var": {},
            },
            "task_group": {"group_display_name": ""},
            "tasks": [
                {
                    "__type": "operator",
                    "__var": {
                        "task_id": "extract",
                        "task_type": "EmptyOperator",
                        "_task_module": "airflow.providers.standard.operators.empty",
                        "ui_color": "#e8f7e4",
                    },
                }
            ],
            "dag_dependencies": [],
        },
    }


def test_build_diff_normalizes_valid_v1_payload() -> None:
    result = build_serialized_dag_diff(
        base_data=_build_v1_payload(), target_data=_build_equivalent_v3_payload()
    )

    assert result["mode"] == "observed_state"
    assert result["serialized_dag_schema_versions"] == {"base": 1, "target": 3}
    assert result["changes"] == []


@pytest.mark.parametrize(
    ("legacy_fields", "current_fields", "attribute"),
    [
        ({"_downstream_task_ids": ["load"]}, {"downstream_task_ids": ["load"]}, "downstream_task_ids"),
        (
            {"_downstream_task_ids": ["load"], "downstream_task_ids": []},
            {"downstream_task_ids": ["load"]},
            "downstream_task_ids",
        ),
        ({"on_failure_callback": True}, {"has_on_failure_callback": True}, "has_on_failure_callback"),
        (
            {"on_failure_callback": False, "has_on_failure_callback": True},
            {"has_on_failure_callback": False},
            "has_on_failure_callback",
        ),
        ({"task_display_name": "display"}, {"_task_display_name": "display"}, "_task_display_name"),
    ],
)
def test_build_diff_matches_legacy_operator_aliases(legacy_fields, current_fields, attribute):
    task = {"task_id": "extract", "task_type": "BaseOperator", "_task_module": "airflow.sdk.bases.operator"}
    legacy_task = {**task, **legacy_fields}
    current_task = {**task, **current_fields}
    assert getattr(OperatorSerialization.deserialize_operator(legacy_task), attribute) == getattr(
        OperatorSerialization.deserialize_operator(current_task), attribute
    )
    base = _build_payload(tasks=[legacy_task])
    base["__version"] = 2

    result = build_serialized_dag_diff(base_data=base, target_data=_build_payload(tasks=[current_task]))

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("is_mapped", [False, True])
@pytest.mark.parametrize(("legacy", "current"), [(True, 2.0), (False, 0.0), (2, 2.0)])
def test_build_diff_normalizes_retry_backoff(is_mapped, legacy, current):
    tasks = []
    for value in (legacy, current):
        task = {"task_id": "extract", "retry_exponential_backoff": value}
        if is_mapped:
            task = {
                "task_id": "extract",
                "_is_mapped": True,
                "partial_kwargs": {"retry_exponential_backoff": value},
            }
        tasks.append(task)

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[tasks[0]]), target_data=_build_payload(tasks=[tasks[1]])
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_reports_a_templated_retry_backoff_change(include_values):
    """
    A templated backoff must not be normalized away.

    populate_operator skips deserialization for a name in ``template_fields``, so a stored ``True``
    and a stored ``2.0`` reach the scheduler as different factors and must not compare equal here.
    """

    def payload(value):
        return _build_payload(
            tasks=[
                {
                    "task_id": "extract",
                    "template_fields": ["retry_exponential_backoff"],
                    "retry_exponential_backoff": value,
                }
            ]
        )

    result = build_serialized_dag_diff(
        base_data=payload(True), target_data=payload(2.0), include_values=include_values
    )

    assert [(change["path"], change["category"]) for change in result["changes"]] == [
        ("/dag/tasks/extract/retry_exponential_backoff", "task")
        if include_values
        else ("/dag/tasks/*/retry_exponential_backoff", "task")
    ]


@pytest.mark.parametrize("is_mapped", [False, True])
@pytest.mark.parametrize("malformed", [None, "abc", {"__type": "dict", "__var": {}}])
def test_build_diff_degrades_only_malformed_retry_backoff(is_mapped, malformed):
    def build(value):
        task: dict[str, Any] = {"task_id": "extract", "_is_mapped": is_mapped}
        fields = task.setdefault("partial_kwargs", {}) if is_mapped else task
        fields["retry_exponential_backoff"] = value
        return _build_payload(tasks=[task, {"task_id": "load", "retries": 1}])

    target = build(malformed)
    target["dag"]["tasks"][1]["__var"]["retries"] = 2

    result = build_serialized_dag_diff(base_data=build(True), target_data=target, include_values=True)

    assert result["mode"] == "observed_state"
    changes = {change["path"]: change for change in result["changes"]}
    assert changes["/dag/tasks/load/retries"]["after_value"] == 2
    backoff_path = (
        "/dag/tasks/extract/partial_kwargs/retry_exponential_backoff"
        if is_mapped
        else "/dag/tasks/extract/retry_exponential_backoff"
    )
    assert changes[backoff_path]["before_value"] == 2.0
    assert changes[backoff_path]["after_value"] == malformed


@pytest.mark.parametrize("is_mapped", [False, True])
def test_build_diff_reports_effective_retry_backoff_changes(is_mapped):
    task: dict[str, Any] = {"task_id": "extract", "_is_mapped": is_mapped}
    base = _build_payload(tasks=[task])
    target = copy.deepcopy(base)
    target_fields = target["dag"]["tasks"][0]["__var"]
    if is_mapped:
        target_fields = target_fields.setdefault("partial_kwargs", {})
    target_fields["retry_exponential_backoff"] = True

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

    assert len(result["changes"]) == 1
    assert result["changes"][0]["before_value"] == 0.0
    assert result["changes"][0]["after_value"] == 2.0


def test_build_diff_preserves_shadowed_mapped_backoff():
    task = {"task_id": "extract", "_is_mapped": True, "partial_kwargs": {"retry_exponential_backoff": True}}
    base = _build_payload(tasks=[task])
    target = _build_payload(tasks=[task])
    base["client_defaults"] = {"tasks": {"retry_exponential_backoff": 3.0}}
    target["client_defaults"] = {"tasks": {"retry_exponential_backoff": 4.0}}
    target = copy.deepcopy(target)
    target["dag"]["tasks"][0]["__var"]["partial_kwargs"]["retry_exponential_backoff"] = 2.0

    assert build_serialized_dag_diff(base_data=base, target_data=target)["changes"] == []


@pytest.mark.parametrize("field", ["python_callable_name", "label"])
def test_build_diff_retains_definition_metadata(field):
    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[{"task_id": "extract", field: "old"}]),
        target_data=_build_payload(tasks=[{"task_id": "extract", field: "new"}]),
        include_values=True,
    )

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    if field == "python_callable_name":
        assert change["path"] == f"/dag/tasks/extract/{field}"
        assert change["before_value"] == "old"
        assert change["after_value"] == "new"
    else:
        assert change["path"] == "/dag/tasks/extract/custom_fields"
        assert change["before_value"] == {field: "old"}
        assert change["after_value"] == {field: "new"}


@pytest.mark.parametrize(
    ("field", "default"),
    [("fail_fast", False), ("render_template_as_native_obj", False), ("rerun_with_latest_version", None)],
)
def test_build_diff_normalizes_dag_defaults(field, default):
    base = _build_payload(tasks=[])
    base["__version"] = 2
    base["dag"][field] = default
    target = _build_payload(tasks=[])

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []
    target["dag"][field] = True
    result = build_serialized_dag_diff(base_data=base, target_data=target)
    assert [change["path"] for change in result["changes"]] == [f"/dag/{field}"]


@pytest.mark.parametrize("field", sorted(_DAG_CALLBACK_FIELDS))
def test_build_diff_preserves_dag_callback_presence(field):
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    target["dag"][field] = False

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["changes"] == [
        {
            "path": f"/dag/{field}",
            "operation": "added",
            "category": "callback",
            "impact": "execution",
            "occurrence_count": 1,
        }
    ]


@pytest.mark.parametrize("field", sorted(_DAG_CALLBACK_FIELDS))
@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_normalizes_present_dag_callbacks(field, include_values):
    with DAG("example", schedule=None) as dag:
        BashOperator(task_id="extract", bash_command="echo hello")
    base = json.loads(json.dumps(DagSerialization.to_dict(dag)))
    target = copy.deepcopy(base)
    base["dag"][field] = False
    target["dag"][field] = True
    for payload in (base, target):
        assert getattr(DagSerialization.from_dict(copy.deepcopy(payload)), field) is True

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def _handle_failure(context):
    pass


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("is_mapped", [False, True])
@pytest.mark.parametrize(
    ("field", "before", "after", "category", "impact"),
    [
        ("owner", "old", "new", "metadata", "metadata"),
        ("doc", "old", "new", "metadata", "metadata"),
        ("doc_md", "old", "new", "metadata", "metadata"),
        ("doc_rst", "old", "new", "metadata", "metadata"),
        ("doc_json", "{}", '{"new": true}', "metadata", "metadata"),
        ("doc_yaml", "old: true", "new: true", "metadata", "metadata"),
        ("ui_color", "#000000", "#ffffff", "metadata", "metadata"),
        ("ui_fgcolor", "#000000", "#ffffff", "metadata", "metadata"),
        ("task_display_name", "old", "new", "metadata", "metadata"),
        ("on_failure_callback", None, _handle_failure, "callback", "execution"),
        ("outlets", [Asset("s3://bucket/old")], [Asset("s3://bucket/new")], "asset", "execution"),
    ],
)
def test_build_diff_classifies_serialized_task_fields(
    is_mapped, include_values, field, before, after, category, impact
):
    payloads = []
    for value in (before, after):
        kwargs = {} if field in {"ui_color", "ui_fgcolor"} else {field: value}
        with DAG("classification") as dag:
            if is_mapped:
                task = BashOperator.partial(task_id="task", **kwargs).expand(bash_command=["echo hello"])
            else:
                task = BashOperator(task_id="task", bash_command="echo hello", **kwargs)
            if field in {"ui_color", "ui_fgcolor"}:
                setattr(task, field, value)
        payloads.append(DagSerialization.to_dict(dag))

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    changes = [change for change in result["changes"] if change["path"].startswith("/dag/tasks/")]
    assert len(changes) == 1
    assert changes[0]["category"] == category
    assert changes[0]["impact"] == impact
    if include_values:
        assert "before_value" in changes[0] or "after_value" in changes[0]
    else:
        assert "before_value" not in changes[0]
        assert "after_value" not in changes[0]


def _build_operator_flag_payload(shape: str) -> dict[str, Any]:
    with DAG("operator_flags", schedule=None) as dag:
        if shape == "branch":
            BranchPythonOperator(task_id="secret_task", python_callable=_return_private_before)
        elif shape == "empty":
            EmptyOperator(task_id="secret_task")
        elif shape == "stub":
            sdk_task.stub(task_id="secret_task")(_declare_private_argument)("private_value")
        else:
            PythonOperator(task_id="secret_task", python_callable=_return_private_before)
    return _serialize_dag(dag)


class _TemplatedPartialOperator(BaseOperator):
    """An unmapped operator whose own execution input happens to be called partial_kwargs."""

    template_fields = ("partial_kwargs",)

    def __init__(self, *, partial_kwargs, **kwargs):
        super().__init__(**kwargs)
        self.partial_kwargs = partial_kwargs

    def execute(self, context):
        return self.partial_kwargs["owner"]


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_keeps_partial_kwargs_opaque_across_a_mapping_change(include_values):
    """One mapped side is not enough: the unmapped side's keys are still its own input."""
    payloads = []
    for is_mapped, owner in ((False, "first"), (True, "second")):
        task: dict[str, Any] = {"task_id": "extract", "partial_kwargs": {"owner": owner}}
        if is_mapped:
            task["_is_mapped"] = True
        payloads.append(_build_payload(tasks=[task]))

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    task_id = "extract" if include_values else "*"
    assert not [change for change in result["changes"] if "partial_kwargs" in change["path"]], (
        "an unmapped side's keys must not be named"
    )
    assert (f"/dag/tasks/{task_id}/custom_fields", "task", "execution") in [
        (change["path"], change["category"], change["impact"]) for change in result["changes"]
    ]


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_keeps_an_unmapped_templated_partial_kwargs_opaque(include_values):
    """
    partial_kwargs is only mapped configuration when the task is actually mapped.

    Reading an unmapped operator's templated dictionary as operator fields classified its
    execution input by whichever field name a key collided with, and named a key its author
    chose rather than leaving it inside the withheld value.
    """
    payloads = []
    for owner in ("first", "second"):
        with DAG("templated_partial", schedule=None) as dag:
            _TemplatedPartialOperator(task_id="extract", partial_kwargs={"owner": owner})
        payloads.append(_serialize_dag(dag))
    assert _get_task(payloads[0])["partial_kwargs"] == {"owner": "first"}
    assert not _get_task(payloads[0]).get("_is_mapped", False), "the task is not mapped"

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    task_id = "extract" if include_values else "*"
    assert [(change["path"], change["category"], change["impact"]) for change in result["changes"]] == [
        (f"/dag/tasks/{task_id}/custom_fields", "task", "execution")
    ]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "expected_category", "expected_impact"),
    [("pool", "task", "execution"), ("owner", "metadata", "metadata")],
)
def test_build_diff_classifies_a_mapped_task_partial_kwarg_per_field(
    field, expected_category, expected_impact, include_values
):
    payloads = []
    for value in ("before", "after"):
        with DAG("mapped_partial", schedule=None) as dag:
            BashOperator.partial(task_id="extract", **{field: value}).expand(bash_command=["a", "b"])
        payloads.append(_serialize_dag(dag))
    assert _get_task(payloads[0])["_is_mapped"] is True

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    task_id = "extract" if include_values else "*"
    assert [(change["path"], change["category"], change["impact"]) for change in result["changes"]] == [
        (f"/dag/tasks/{task_id}/partial_kwargs/{field}", expected_category, expected_impact)
    ]


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize(
    ("field", "shape"),
    [("_can_skip_downstream", "branch"), ("_is_empty", "empty"), ("is_stub", "stub")],
)
def test_build_diff_names_operator_flag_fields(field, shape, include_values):
    base = _build_operator_flag_payload(shape)
    target = _build_operator_flag_payload("python")
    assert field in _get_task(base)
    assert field not in _get_task(target)

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    field_path = f"/dag/tasks/{'secret_task' if include_values else '*'}/{field}"
    assert field_path in [change["path"] for change in result["changes"]]
    change = next(change for change in result["changes"] if change["path"] == field_path)
    assert (change["operation"], change["category"], change["impact"]) == (
        "removed",
        "task",
        "execution",
    )
    if not include_values:
        assert "secret_" not in json.dumps(result)


@pytest.mark.parametrize("field", ["_arg_bindings", "secret_callback_argument"])
@pytest.mark.parametrize("is_mapped", [False, True])
def test_build_diff_redacts_argument_bindings_and_partial_arguments(field, is_mapped):
    tasks = []
    for value in ("old-secret", "new-secret"):
        fields = {field: [{"argument": value}]}
        task = {"task_id": "secret-task", **fields}
        if is_mapped:
            task = {"task_id": "secret-task", "_is_mapped": True, "partial_kwargs": fields}
        tasks.append(task)

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=[tasks[0]]), target_data=_build_payload(tasks=[tasks[1]])
    )

    path = "/dag/tasks/*/partial_kwargs/custom_fields" if is_mapped else "/dag/tasks/*/custom_fields"
    assert result["changes"] == [
        {
            "path": path,
            "operation": "changed",
            "category": "task",
            "impact": "execution",
            "occurrence_count": 1,
        }
    ]
    assert all(
        value not in json.dumps(result) for value in (field, "secret-task", "old-secret", "new-secret")
    )


@pytest.mark.parametrize(("field", "value"), [("tags", "tag"), ("allowed_run_types", "manual")])
def test_build_diff_normalizes_duplicate_set_entries(field, value):
    with DAG("duplicate_sets") as dag:
        BashOperator(task_id="task", bash_command="echo hello")
    base = DagSerialization.to_dict(dag)
    base["dag"][field] = [value, value]
    target = copy.deepcopy(base)
    target["dag"][field] = [value]
    DagSerialization.validate_schema(base)
    assert getattr(DagSerialization.from_dict(copy.deepcopy(base)), field) == getattr(
        DagSerialization.from_dict(copy.deepcopy(target)), field
    )

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("single_task", [False, True])
@mock.patch.object(dag_version_diff, "_is_json_equal", wraps=dag_version_diff._is_json_equal, spec=True)
def test_build_diff_stops_comparing_after_truncation(compare, include_values, single_task):
    if single_task:
        base_tasks = [{"task_id": "task", "doc_md": "old", "owner": "old", "queue": "old"}]
        target_tasks = [{"task_id": "task", "doc_md": "first", "owner": "second", "queue": "unvisited"}]
    else:
        base_tasks = [{"task_id": f"task{index}", "doc_md": "old"} for index in range(3)]
        target_tasks = [
            {"task_id": f"task{index}", "doc_md": value}
            for index, value in enumerate(("first", "second", "unvisited"))
        ]

    result = build_serialized_dag_diff(
        base_data=_build_payload(tasks=base_tasks),
        target_data=_build_payload(tasks=target_tasks),
        max_changes=1,
        include_values=include_values,
    )

    assert len(result["changes"]) == 1
    compare.assert_any_call("old", "second")
    # A redacted diff repeating one already disclosed path drops nothing, so it keeps walking
    # to count the remaining occurrences instead of reporting truncation.
    repeats_one_public_path = not single_task and not include_values
    assert result["truncated"] is not repeats_one_public_path
    assert result["changes"][0]["occurrence_count"] == (3 if repeats_one_public_path else 1)
    assert (mock.call("old", "unvisited") in compare.call_args_list) is repeats_one_public_path


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_reports_a_real_deep_payload_as_canonicalization_failure(include_values):
    def payload(value):
        nested: Any = {"leaf": value}
        # Use the live limit: BaseOperator.__deepcopy__ can raise it process-wide.
        for _ in range(sys.getrecursionlimit()):
            nested = {"__type": "dict", "__var": {"nested": nested}}
        return _build_payload(tasks=[{"task_id": "extract", "executor_config": nested}])

    result = build_serialized_dag_diff(
        base_data=payload(1), target_data=payload(2), include_values=include_values
    )

    assert result["mode"] == "unavailable"
    assert result["unavailable_reason"] == "serialized_dag_canonicalization_failed"


def test_build_diff_canonicalization_failure_logs_why(caplog):
    payload = _build_payload(tasks=[])
    payload["client_defaults"] = {"dags": {"catchup": True}}

    result = build_serialized_dag_diff(base_data=payload, target_data=_build_payload(tasks=[]))

    assert result["unavailable_reason"] == "serialized_dag_canonicalization_failed"
    assert {
        "event": "Serialized Dag diff canonicalization failed",
        "error_type": "ValueError",
        "reason": "unsupported client_defaults sections: ['dags']",
    } in caplog


@pytest.mark.parametrize(
    ("max_changes", "expected_counts", "truncated"),
    [
        # Counting occurrences would drop timezone at the default bound.
        (2, {"/dag/tasks/*/retries": 600, "/dag/timezone": 1}, False),
        (500, {"/dag/tasks/*/retries": 600, "/dag/timezone": 1}, False),
        (1, {"/dag/tasks/*/retries": 600}, True),
    ],
)
def test_build_diff_bound_caps_records_not_repeats(max_changes, expected_counts, truncated):
    def payload(retries, timezone):
        built = _build_payload(tasks=[{"task_id": f"t{index}", "retries": retries} for index in range(600)])
        built["dag"]["timezone"] = timezone
        return built

    result = build_serialized_dag_diff(
        base_data=payload(1, "UTC"), target_data=payload(2, "Europe/Berlin"), max_changes=max_changes
    )

    counts = {change["path"]: change["occurrence_count"] for change in result["changes"]}
    assert counts == expected_counts
    assert result["truncated"] is truncated
