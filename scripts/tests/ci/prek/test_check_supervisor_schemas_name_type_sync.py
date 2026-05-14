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

import os
from pathlib import Path
from typing import Annotated, Literal
from unittest import mock

import pytest
from ci.prek.dump_supervisor_schemas_name_type_mismatches import (
    _members_of_discriminated_union,
    _source_location,
    _type_literal_value,
    classify_unions,
)
from pydantic import BaseModel, Field


class _GoodA(BaseModel):
    type: Literal["_GoodA"] = "_GoodA"


class _GoodB(BaseModel):
    type: Literal["_GoodB"] = "_GoodB"


class _Renamed(BaseModel):
    """``__name__`` and ``type`` literal drift -- the case the hook must catch."""

    type: Literal["RenamedOnWire"] = "RenamedOnWire"


class _NoType(BaseModel):
    payload: str = ""


class _MultiLiteral(BaseModel):
    type: Literal["a", "b"] = "a"


# ``Annotated[...]``-wrapped union, mirroring how supervisor-schema modules
# declare ``ToTask`` / ``ToSupervisor`` / ``ToTriggerRunner`` / ...
_AllGoodUnion = Annotated[_GoodA | _GoodB, Field(discriminator="type")]
_RenamedUnion = Annotated[_GoodA | _Renamed, Field(discriminator="type")]
_NoTypeUnion = Annotated[_GoodA | _NoType, Field(discriminator="type")]
_MultiLiteralUnion = Annotated[_GoodA | _MultiLiteral, Field(discriminator="type")]


class TestMembersOfDiscriminatedUnion:
    def test_unwraps_annotated_then_returns_classes(self):
        members = _members_of_discriminated_union(_AllGoodUnion)
        assert set(members) == {_GoodA, _GoodB}

    def test_filters_non_class_args(self):
        # ``get_args`` on a bare ``int | str`` returns ``(int, str)`` -- both are
        # classes, so they pass the ``isinstance(m, type)`` filter. To verify
        # the filter actually fires we feed in a ``Literal`` whose args are
        # *string values*, not classes.
        members = _members_of_discriminated_union(Literal["a", "b"])
        assert members == ()


class TestTypeLiteralValue:
    @pytest.mark.parametrize(
        "model, expected",
        [(_GoodA, "_GoodA"), (_GoodB, "_GoodB"), (_Renamed, "RenamedOnWire")],
    )
    def test_returns_single_literal_value(self, model, expected):
        assert _type_literal_value(model) == expected

    def test_returns_none_when_type_field_missing(self):
        assert _type_literal_value(_NoType) is None

    def test_returns_none_for_multi_value_literal(self):
        # The wire discriminator must round-trip to exactly one string; a
        # multi-value Literal is ambiguous and treated as a mismatch.
        assert _type_literal_value(_MultiLiteral) is None


class TestSourceLocation:
    def test_returns_relpath_lineno_for_introspectable_class(self):
        location = _source_location(_GoodA)
        path, _, lineno = location.rpartition(":")
        assert path.endswith("test_check_supervisor_schemas_name_type_sync.py")
        assert int(lineno) > 0

    def test_falls_back_when_source_unavailable(self):
        # ``int`` is a C builtin -- ``inspect.getsourcefile`` raises ``TypeError``.
        assert _source_location(int) == "<unknown>:0"


class TestClassifyUnions:
    """Drives the real classifier the hook ships with; asserts on the whole payload."""

    def test_all_matching_union(self):
        assert classify_unions({"All": _AllGoodUnion}) == {
            "mismatches": [],
            "checked": [
                {
                    "class_name": "_GoodA",
                    "literal": "_GoodA",
                    "union": "All",
                    "location": mock.ANY,
                },
                {
                    "class_name": "_GoodB",
                    "literal": "_GoodB",
                    "union": "All",
                    "location": mock.ANY,
                },
            ],
        }

    def test_renamed_class_is_flagged(self):
        assert classify_unions({"Renamed": _RenamedUnion}) == {
            "mismatches": [
                {
                    "class_name": "_Renamed",
                    "literal": "RenamedOnWire",
                    "union": "Renamed",
                    "location": mock.ANY,
                    "reason": "class __name__ != type literal value",
                },
            ],
            "checked": [
                {
                    "class_name": "_GoodA",
                    "literal": "_GoodA",
                    "union": "Renamed",
                    "location": mock.ANY,
                },
            ],
        }

    def test_missing_type_field_is_flagged(self):
        assert classify_unions({"NoType": _NoTypeUnion}) == {
            "mismatches": [
                {
                    "class_name": "_NoType",
                    "literal": "",
                    "union": "NoType",
                    "location": mock.ANY,
                    "reason": "model has no `type: Literal[...]` field",
                },
            ],
            "checked": [
                {
                    "class_name": "_GoodA",
                    "literal": "_GoodA",
                    "union": "NoType",
                    "location": mock.ANY,
                },
            ],
        }

    def test_multi_value_literal_is_flagged(self):
        assert classify_unions({"Multi": _MultiLiteralUnion}) == {
            "mismatches": [
                {
                    "class_name": "_MultiLiteral",
                    "literal": "",
                    "union": "Multi",
                    "location": mock.ANY,
                    "reason": "model has no `type: Literal[...]` field",
                },
            ],
            "checked": [
                {
                    "class_name": "_GoodA",
                    "literal": "_GoodA",
                    "union": "Multi",
                    "location": mock.ANY,
                },
            ],
        }

    def test_member_appearing_in_two_unions_is_only_classified_once(self):
        # ``_GoodA`` is shared by both unions; ``classify_unions`` must
        # dedupe so the snapshot doesn't double-count it. The union the
        # entry is attributed to is the first one that contains it.
        payload = classify_unions({"First": _AllGoodUnion, "Second": _RenamedUnion})
        good_a_entries = [c for c in payload["checked"] if c["class_name"] == "_GoodA"]
        assert len(good_a_entries) == 1
        assert good_a_entries[0]["union"] == "First"


@pytest.fixture
def hook_in_pre_commit_config():
    """Find the new hook entry in ``.pre-commit-config.yaml`` and return its dict."""
    import yaml

    cfg = yaml.safe_load((_repo_root() / ".pre-commit-config.yaml").read_text())
    for repo in cfg.get("repos", []):
        for hook in repo.get("hooks", []):
            if hook.get("id") == "check-supervisor-schemas-name-type-sync":
                return hook
    pytest.fail("hook id check-supervisor-schemas-name-type-sync not found in .pre-commit-config.yaml")


def _repo_root() -> Path:
    # scripts/tests/ci/prek/<this>.py -> repo root is four parents up
    return Path(__file__).resolve().parents[4]


class TestPreCommitWiring:
    """The hook config is what actually fires the check in CI -- keep it honest."""

    def test_entry_points_at_existing_script(self, hook_in_pre_commit_config):
        entry = hook_in_pre_commit_config["entry"]
        assert (_repo_root() / entry.removeprefix("./")).is_file()

    def test_files_pattern_matches_union_source_files(self, hook_in_pre_commit_config):
        import re

        pattern = re.compile(hook_in_pre_commit_config["files"])
        for path in (
            "task-sdk/src/airflow/sdk/execution_time/comms.py",
            "airflow-core/src/airflow/dag_processing/processor.py",
            "airflow-core/src/airflow/jobs/triggerer_job_runner.py",
        ):
            assert pattern.match(path), f"expected hook to fire for {path}"

    def test_files_pattern_ignores_unrelated_files(self, hook_in_pre_commit_config):
        import re

        pattern = re.compile(hook_in_pre_commit_config["files"])
        for path in (
            "airflow-core/src/airflow/models/dag.py",
            "task-sdk/src/airflow/sdk/definitions/dag.py",
            os.path.join("airflow-core", "src", "airflow", "jobs", "scheduler_job_runner.py"),
        ):
            assert not pattern.match(path), f"hook should not fire for {path}"
