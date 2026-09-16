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

import inspect
import json
import shlex
from functools import cached_property
from unittest.mock import Mock

import pytest

from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION, GithubEvents
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.selective_checks import SelectiveChecks
from airflow_breeze.utils.verification_plan import (
    FLAG_COMMANDS,
    NOT_RUNNABLE_LOCALLY,
    build_local_verification_plan,
    build_unit_test_items,
)

NEUTRAL_COMMIT = "938f0c1f3cc4cbe867123ee8aa9f290f9f18100a"
# Exported by SelectiveChecks but no workflow job is gated on them (build-info pass-through only).
NOT_A_CI_JOB = {
    "run_amazon_tests",
    "run_api_tests",
    "run_ol_tests",
    "run_python_scans",
    "run_javascript_scans",
}


def _selective_checks(files: tuple[str, ...], default_branch: str = "main") -> SelectiveChecks:
    return SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=tuple(),
        default_branch=default_branch,
    )


def _mock_selective_checks(**flags: object) -> Mock:
    sc = Mock(spec=SelectiveChecks)
    for flag in (*FLAG_COMMANDS, "run_unit_tests", "docs_build", "basic_checks_only"):
        setattr(sc, flag, False)
    sc.skip_providers_tests = True
    sc.skip_prek_hooks = "identity"
    sc.default_python_version = DEFAULT_PYTHON_MAJOR_MINOR_VERSION
    sc.full_tests_needed = False
    for flag, value in flags.items():
        setattr(sc, flag, value)
    return sc


@pytest.mark.parametrize("flag", sorted(FLAG_COMMANDS))
def test_each_flag_maps_to_its_commands(flag: str):
    result = build_local_verification_plan(_mock_selective_checks(**{flag: True}), (), "main")
    commands = [item["command"] for item in result["items"]]
    assert commands == [
        "SKIP=identity prek run --all-files",
        *(command for _, command, _ in FLAG_COMMANDS[flag]),
    ]


@pytest.mark.parametrize(
    ("basic_checks_only", "expected"),
    [
        (False, ("prek", "SKIP=identity prek run --all-files", "breeze")),
        (
            True,
            (
                "prek",
                "SKIP_BREEZE_PREK_HOOKS=true SKIP=identity prek run --from-ref main --to-ref HEAD",
                "host",
            ),
        ),
    ],
)
def test_prek_command_follows_basic_checks_only(basic_checks_only: bool, expected: tuple[str, str, str]):
    result = build_local_verification_plan(
        _mock_selective_checks(basic_checks_only=basic_checks_only), (), "main"
    )
    prek = result["items"][0]
    assert (prek["kind"], prek["command"], prek["runs_in"]) == expected


def test_docs_only_change_mirrors_the_ci_cell():
    files = ("airflow-core/docs/index.rst",)
    sc = _selective_checks(files)
    result = build_local_verification_plan(sc, files, "main")
    assert [item["command"] for item in result["items"]] == [
        f"SKIP={sc.skip_prek_hooks} prek run --all-files",
        "breeze build-docs apache-airflow",
    ]


def test_core_change_splits_db_and_non_db_cells():
    files = ("airflow-core/src/airflow/models/dag.py",)
    sc = _selective_checks(files)
    core_types = " ".join(c["test_types"] for c in json.loads(sc.core_test_types_list_as_strings_in_json))
    result = build_local_verification_plan(sc, files, "main")
    unit = [
        item["command"] for item in result["items"] if item["command"].startswith("breeze testing core-tests")
    ]
    assert unit == [
        f'breeze testing core-tests --run-in-parallel --run-db-tests-only --parallel-test-types "{core_types}"',
        "breeze testing core-tests --use-xdist --skip-db-tests --no-db-cleanup --backend none "
        f'--parallel-test-types "{core_types}"',
    ]


def test_every_selective_checks_run_flag_is_classified():
    run_flags = {
        name
        for name, value in inspect.getmembers(SelectiveChecks)
        if isinstance(value, cached_property)
        and (name.startswith("run_") or name in ("docs_build", "has_migrations"))
    }
    classified = set(FLAG_COMMANDS) | NOT_RUNNABLE_LOCALLY | {"run_unit_tests", "docs_build"} | NOT_A_CI_JOB
    assert run_flags - classified == set(), (
        "new SelectiveChecks flag: add it to FLAG_COMMANDS, NOT_RUNNABLE_LOCALLY or NOT_A_CI_JOB"
    )
    assert classified - run_flags == set(), "classified flag no longer exists on SelectiveChecks"


@pytest.mark.parametrize("group", ["core", "providers"])
def test_unit_test_commands_use_the_same_flags_as_the_ci_script(group: str):
    ci_script = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "testing" / "run_unit_tests.sh"
    ci_flag_sets = {
        frozenset(shlex.split(line)[3:])
        for line in ci_script.read_text().splitlines()
        if line.strip().startswith(f"breeze testing {group}-tests")
    }
    for item in build_unit_test_items(group, json.dumps([{"description": "x", "test_types": "Always"}])):
        tokens = shlex.split(item.command)[3:]
        cut = tokens.index("--parallel-test-types")
        assert frozenset(tokens[:cut] + tokens[cut + 2 :]) in ci_flag_sets, item.command


def test_empty_diff_prints_only_the_always_items():
    sc = _selective_checks(())
    commands = [item["command"] for item in build_local_verification_plan(sc, (), "main")["items"]]
    assert (
        commands[0]
        == f"SKIP_BREEZE_PREK_HOOKS=true SKIP={sc.skip_prek_hooks} prek run --from-ref main --to-ref HEAD"
    )
    assert not any(command.startswith("breeze testing") for command in commands)


def test_release_branch_drops_providers_tests():
    files = ("airflow-core/src/airflow/models/dag.py",)
    result = build_local_verification_plan(
        _selective_checks(files, default_branch="v3-1-test"), files, "v3-1-test"
    )
    commands = [item["command"] for item in result["items"]]
    assert any(command.startswith("breeze testing core-tests") for command in commands)
    assert not any(command.startswith("breeze testing providers-tests") for command in commands)


def test_prek_command_quotes_base_ref():
    result = build_local_verification_plan(
        _mock_selective_checks(basic_checks_only=True), (), "branch; echo unexpected"
    )
    assert result["items"][0]["command"].endswith(
        "prek run --from-ref 'branch; echo unexpected' --to-ref HEAD"
    )
