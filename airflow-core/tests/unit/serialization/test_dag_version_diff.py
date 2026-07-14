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

import json

import pytest

from airflow.serialization.dag_version_diff import build_serialized_dag_diff


def _payload(
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


def test_build_diff_is_deterministic_and_normalizes_order() -> None:
    base = _payload(
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
    target = _payload(
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
    base = _payload(tasks=[{"task_id": "extract", "retries": 1}], tags=["old"])
    target = _payload(
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
        base_data=_payload(tasks=[{"task_id": "extract", "retries": 1}], tags=["old"]),
        target_data=_payload(tasks=[{"task_id": "extract", "retries": 2}], tags=["new"]),
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
        base_data=_payload(tasks=[], dependencies=[dependency]),
        target_data=_payload(tasks=[]),
    )

    assert len(result["changes"]) == 1
    assert result["changes"][0]["category"] == "dependency"
    assert result["changes"][0]["impact"] == "execution"


def test_build_diff_classifies_fail_fast_as_schedule() -> None:
    base = _payload(tasks=[])
    target = _payload(tasks=[])
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
        (None, _payload(tasks=[]), "serialized_dag_missing"),
        ({"__version": 99, "dag": {}}, _payload(tasks=[]), "unsupported_serialized_dag_schema_version:99"),
        ({"__version": 3, "dag": []}, _payload(tasks=[]), "serialized_dag_canonicalization_failed"),
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
            _payload(tasks=[]),
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
            _payload(tasks=[]),
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
        build_serialized_dag_diff(base_data=_payload(tasks=[]), target_data=_payload(tasks=[]), max_changes=0)


def test_build_diff_rejects_unbounded_change_bound() -> None:
    with pytest.raises(ValueError, match="max_changes must not exceed 5000"):
        build_serialized_dag_diff(
            base_data=_payload(tasks=[]), target_data=_payload(tasks=[]), max_changes=5001
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
        base_data=_payload(
            tasks=[{"task_id": "secret-task", "retries": 1}],
            tags=["secret-old-tag"],
            dependencies=[dependency],
        ),
        target_data=_payload(
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


def test_build_diff_collapses_arbitrary_mappings_without_values() -> None:
    base = _payload(tasks=[])
    target = _payload(tasks=[])
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
    base = _payload(tasks=[])
    target = _payload(tasks=[])
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
    base = _payload(tasks=[])
    target = _payload(tasks=[])
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
    base = _payload(tasks=[])
    target = _payload(tasks=[])
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
            {"task_id": "extract", "retries": 2, "partial_kwargs": {"retries": 2}},
            {"task_id": "extract", "partial_kwargs": {}},
        ),
    ],
)
def test_build_diff_normalizes_v3_client_defaults(base_task, target_task) -> None:
    base = _payload(tasks=[base_task])
    base["__version"] = 2
    target = _payload(tasks=[target_task])
    target["client_defaults"] = {"tasks": {"retries": 2}}

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert result["serialized_dag_schema_versions"] == {"base": 2, "target": 3}
    assert result["mode"] == "observed_state"
    assert result["changes"] == []


@pytest.mark.parametrize(
    "base_data",
    [
        {"__version": 3, "dag": {"tasks": []}, "client_defaults": []},
        {"__version": 3, "dag": {"tasks": []}, "client_defaults": {"tasks": []}},
        {"__version": 3, "dag": {"tasks": {}}, "client_defaults": {"tasks": {}}},
        {"__version": 3, "dag": {"tasks": [{}]}, "client_defaults": {"tasks": {}}},
    ],
)
def test_build_diff_rejects_malformed_client_defaults(base_data) -> None:
    result = build_serialized_dag_diff(base_data=base_data, target_data=_payload(tasks=[]))

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
        base_data=_payload(tasks=[], dependencies=[dependency, dependency]),
        target_data=_payload(tasks=[], dependencies=[dependency]),
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
        base_data=_payload(tasks=[], dependencies=[first_dependency, second_dependency]),
        target_data=_payload(tasks=[], dependencies=[second_dependency, first_dependency]),
    )

    assert result["mode"] == "observed_state"
    assert result["changes"] == []


def test_build_diff_detects_json_type_change() -> None:
    base = _payload(tasks=[])
    target = _payload(tasks=[])
    base["dag"]["flag"] = True
    target["dag"]["flag"] = 1

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/flag"
    assert change["operation"] == "changed"


def test_build_diff_classifies_callback_as_execution() -> None:
    base = _payload(tasks=[])
    target = _payload(tasks=[])
    base["dag"]["on_failure_callback"] = "old"
    target["dag"]["on_failure_callback"] = "new"

    result = build_serialized_dag_diff(base_data=base, target_data=target)

    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["path"] == "/dag/on_failure_callback"
    assert change["category"] == "callback"
    assert change["impact"] == "execution"


def _v1_payload() -> dict:
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


def _equivalent_v3_payload() -> dict:
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
    result = build_serialized_dag_diff(base_data=_v1_payload(), target_data=_equivalent_v3_payload())

    assert result["mode"] == "observed_state"
    assert result["serialized_dag_schema_versions"] == {"base": 1, "target": 3}
    assert result["changes"] == []
