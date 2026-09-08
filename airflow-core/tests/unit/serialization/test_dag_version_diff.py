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
import json
from typing import Any
from unittest import mock

import pytest

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset, task_group
from airflow.serialization import dag_version_diff
from airflow.serialization.dag_version_diff import (
    _DIFF_V1_DAG_FIELD_CATEGORIES,
    _DIFF_V1_PUBLIC_TASK_FIELDS,
    _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS,
    build_serialized_dag_diff,
)
from airflow.serialization.json_schema import load_dag_schema_dict
from airflow.serialization.serialized_objects import (
    _DAG_CALLBACK_FIELDS,
    _OPERATOR_TIMEDELTA_FIELDS,
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


def test_diff_v1_task_field_policy_tracks_serializer_schema() -> None:
    schema_fields = frozenset(load_dag_schema_dict()["definitions"]["operator"]["properties"])

    assert schema_fields == (
        (_DIFF_V1_PUBLIC_TASK_FIELDS - {"__type"}) | _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS
    )
    assert not (_DIFF_V1_PUBLIC_TASK_FIELDS & _DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS)


def test_diff_v1_dag_field_policy_tracks_serializer_schema() -> None:
    schema_fields = set(load_dag_schema_dict()["definitions"]["dag"]["properties"])

    assert schema_fields == _DIFF_V1_DAG_FIELD_CATEGORIES.keys()


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
        }


@pytest.mark.parametrize("include_values", [False, True])
@pytest.mark.parametrize("field", ["group_display_name", "tooltip", "doc_md", "ui_color", "ui_fgcolor"])
def test_build_diff_classifies_task_group_display_fields(include_values, field):
    base = _build_payload(tasks=[])
    target = _build_payload(tasks=[])
    base["dag"]["task_group"] = {field: "old"}
    target["dag"]["task_group"] = {field: "new"}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=include_values)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == f"/dag/task_group/{field}"
    assert change["category"] == "metadata"
    assert change["impact"] == "metadata"


@pytest.mark.parametrize("include_values", [False, True])
def test_build_diff_classifies_nested_mapped_group_inputs_as_execution(include_values):
    @task_group
    def group(command):
        BashOperator(task_id="extract", bash_command=command)

    payloads = []
    for commands in (["one"], ["one", "two"]):
        with DAG("example", schedule=None) as dag:
            group.expand(command=commands)
        payloads.append(DagSerialization.to_dict(dag))

    result = build_serialized_dag_diff(
        base_data=payloads[0], target_data=payloads[1], include_values=include_values
    )

    assert result["mode"] == "observed_state"
    group_changes = [
        change for change in result["changes"] if change["path"].startswith("/dag/task_group/children")
    ]
    assert len(group_changes) == 1
    change = group_changes[0]
    assert change["category"] == "task"
    assert change["impact"] == "execution"


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
def test_build_diff_returns_unavailable_for_unsafe_inputs(base_data, target_data, reason) -> None:
    result = build_serialized_dag_diff(base_data=base_data, target_data=target_data)

    assert result["mode"] == "unavailable"
    assert result["changes"] == []
    assert result["unavailable_reason"] == reason


def test_build_diff_rejects_non_positive_change_bound() -> None:
    with pytest.raises(ValueError, match="max_changes must be a positive integer"):
        build_serialized_dag_diff(
            base_data=_build_payload(tasks=[]), target_data=_build_payload(tasks=[]), max_changes=0
        )


def test_build_diff_rejects_unbounded_change_bound() -> None:
    with pytest.raises(ValueError, match="max_changes must not exceed 5000"):
        build_serialized_dag_diff(
            base_data=_build_payload(tasks=[]), target_data=_build_payload(tasks=[]), max_changes=5001
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

    change = next(change for change in result["changes"] if change["path"].endswith(custom_field))
    assert change["path"] == f"/dag/tasks/extract/{custom_field}"
    assert change["category"] == "task"
    assert change["impact"] == "execution"
    assert change["before_value"] == "old-secret"
    assert change["after_value"] == "new-secret"


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
            "category": "param",
            "impact": "execution",
        }
    ]


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


@pytest.mark.parametrize("field", sorted(_OPERATOR_TIMEDELTA_FIELDS))
def test_build_diff_normalizes_mapped_timedelta_defaults(field) -> None:
    base = _build_payload(
        tasks=[
            {
                "task_id": "extract",
                "_is_mapped": True,
                "partial_kwargs": {field: {"__type": "timedelta", "__var": 60.0}},
            }
        ]
    )
    target = _build_payload(tasks=[{"task_id": "extract", "_is_mapped": True, "partial_kwargs": {}}])
    target["client_defaults"] = {"tasks": {field: 60.0}}

    result = build_serialized_dag_diff(base_data=base, target_data=target, include_values=True)

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
        "source": "a",
        "target": "b",
        "label": "c|d",
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
    assert result["changes"][0]["path"] == f"/dag/tasks/extract/{field}"


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
        {"path": f"/dag/{field}", "operation": "added", "category": "callback", "impact": "execution"}
    ]


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
        {"path": path, "operation": "changed", "category": "task", "impact": "execution"}
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
    assert result["truncated"] is True
    compare.assert_any_call("old", "second")
    assert mock.call("old", "unvisited") not in compare.call_args_list
