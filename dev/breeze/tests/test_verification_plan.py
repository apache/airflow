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
import re
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
    LeanSelectiveChecks,
    build_local_verification_plan,
    build_unit_test_items,
)

NEUTRAL_COMMIT = "938f0c1f3cc4cbe867123ee8aa9f290f9f18100a"
# Selective-checks outputs that workflow `if:` conditions read to shape the run itself, not to gate a
# job breeze verify could list.
CI_INTERNAL_GATES = {
    "ci_image_build",
    "prod_image_build",
    "full_tests_needed",
    "default_branch",
    "default_python_version",
    "latest_versions_only",
    "include_success_outputs",
    "upgrade_to_newer_dependencies",
    "kustomize_overlay_names",
    "testable_core_integrations",
    "testable_providers_integrations",
}
CONSUMED_DIRECTLY = {"run_unit_tests", "docs_build", "basic_checks_only", "skip_providers_tests"}


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
    sc.shared_distributions_as_json = "[]"
    sc.default_python_version = DEFAULT_PYTHON_MAJOR_MINOR_VERSION
    for flag, value in flags.items():
        setattr(sc, flag, value)
    return sc


@pytest.mark.parametrize("flag", sorted(FLAG_COMMANDS))
def test_each_flag_maps_to_its_commands(flag: str):
    result = build_local_verification_plan(
        _mock_selective_checks(**{flag: True}), (), "main", full_tests_needed=False
    )
    commands = [item["command"] for item in result["items"]]
    assert commands == [
        "SKIP=identity prek run --from-ref main --to-ref HEAD",
        *(command for _, command, _ in FLAG_COMMANDS[flag]),
    ]


@pytest.mark.parametrize(
    ("basic_checks_only", "full", "expected"),
    [
        (False, False, ("prek", "SKIP=identity prek run --from-ref main --to-ref HEAD", "breeze")),
        (False, True, ("prek", "SKIP=identity prek run --all-files", "breeze")),
        (
            True,
            False,
            (
                "prek",
                "SKIP_BREEZE_PREK_HOOKS=true SKIP=identity prek run --from-ref main --to-ref HEAD",
                "host",
            ),
        ),
        (
            True,
            True,
            (
                "prek",
                "SKIP_BREEZE_PREK_HOOKS=true SKIP=identity prek run --from-ref main --to-ref HEAD",
                "host",
            ),
        ),
    ],
)
def test_prek_command_follows_basic_checks_only_and_full(
    basic_checks_only: bool, full: bool, expected: tuple[str, str, str]
):
    result = build_local_verification_plan(
        _mock_selective_checks(basic_checks_only=basic_checks_only),
        (),
        "main",
        full_tests_needed=False,
        full=full,
    )
    prek = result["items"][0]
    assert (prek["kind"], prek["command"], prek["runs_in"]) == expected


def test_docs_only_change_mirrors_the_ci_cell():
    files = ("airflow-core/docs/index.rst",)
    sc = _selective_checks(files)
    result = build_local_verification_plan(sc, files, "main", full_tests_needed=False)
    assert [item["command"] for item in result["items"]] == [
        f"SKIP={sc.skip_prek_hooks} prek run --from-ref main --to-ref HEAD",
        "breeze build-docs apache-airflow",
    ]


def test_core_change_splits_db_and_non_db_cells():
    files = ("airflow-core/src/airflow/models/dag.py",)
    sc = _selective_checks(files)
    core_types = " ".join(c["test_types"] for c in json.loads(sc.core_test_types_list_as_strings_in_json))
    result = build_local_verification_plan(sc, files, "main", full_tests_needed=False)
    unit = [
        item["command"] for item in result["items"] if item["command"].startswith("breeze testing core-tests")
    ]
    assert unit == [
        f'breeze testing core-tests --run-in-parallel --run-db-tests-only --parallel-test-types "{core_types}"',
        "breeze testing core-tests --use-xdist --skip-db-tests --no-db-cleanup --backend none "
        f'--parallel-test-types "{core_types}"',
    ]


def _gate_names_in(workflow_text: str) -> set[str]:
    """Names read by `if:` conditions, including every line of a multi-line `if: >` block."""
    names: set[str] = set()
    lines = workflow_text.splitlines()
    for index, line in enumerate(lines):
        if not re.match(r"\s*if:", line):
            continue
        block = [line]
        if re.match(r"\s*if:\s*>", line):
            indent = len(line) - len(line.lstrip())
            for following in lines[index + 1 :]:
                if not following.strip() or len(following) - len(following.lstrip()) <= indent:
                    break
                block.append(following)
        for text in block:
            names.update(n.replace("-", "_") for n in re.findall(r"(?:outputs|inputs)\.([a-z0-9-]+)", text))
    return names


def _workflow_gate_names() -> set[str]:
    return set().union(
        *(
            _gate_names_in(path.read_text())
            for path in (AIRFLOW_ROOT_PATH / ".github" / "workflows").glob("*.yml")
        )
    )


def test_gate_scan_reads_every_line_of_a_long_multiline_if():
    conditions = " &&\n".join(f"      needs.build-info.outputs.gate-{n} == 'true'" for n in range(12))
    workflow = f"  job:\n    if: >\n{conditions}\n    steps: []\n  other:\n    if: inputs.single == 'true'\n"
    assert _gate_names_in(workflow) == {f"gate_{n}" for n in range(12)} | {"single"}


def test_every_workflow_gate_backed_by_selective_checks_is_classified():
    properties = {n for n, v in inspect.getmembers(SelectiveChecks) if isinstance(v, cached_property)}
    gates = _workflow_gate_names() & properties
    classified = set(FLAG_COMMANDS) | NOT_RUNNABLE_LOCALLY | CONSUMED_DIRECTLY | CI_INTERNAL_GATES
    assert gates - classified == set(), (
        "a workflow now gates a job on this selective-checks output: add it to FLAG_COMMANDS, "
        "NOT_RUNNABLE_LOCALLY or CI_INTERNAL_GATES in test_verification_plan.py"
    )
    assert classified - gates == set(), "classified name is no longer a workflow gate"


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


def test_empty_diff_prints_only_prek():
    sc = _selective_checks(())
    plan = build_local_verification_plan(sc, (), "main", full_tests_needed=False)
    commands = [item["command"] for item in plan["items"]]
    assert (
        commands[0]
        == f"SKIP_BREEZE_PREK_HOOKS=true SKIP={sc.skip_prek_hooks} prek run --from-ref main --to-ref HEAD"
    )
    assert not any(command.startswith("breeze testing") for command in commands)


def test_release_branch_drops_providers_tests():
    files = ("airflow-core/src/airflow/models/dag.py",)
    result = build_local_verification_plan(
        _selective_checks(files, default_branch="v3-1-test"), files, "v3-1-test", full_tests_needed=False
    )
    commands = [item["command"] for item in result["items"]]
    assert any(command.startswith("breeze testing core-tests") for command in commands)
    assert not any(command.startswith("breeze testing providers-tests") for command in commands)


def test_prek_command_quotes_base_ref():
    result = build_local_verification_plan(
        _mock_selective_checks(basic_checks_only=True), (), "branch; echo unexpected", full_tests_needed=False
    )
    assert result["items"][0]["command"].endswith(
        "prek run --from-ref 'branch; echo unexpected' --to-ref HEAD"
    )


def test_lean_plan_skips_the_full_suite_expansion_for_ci_tooling_changes():
    files = ("dev/breeze/src/airflow_breeze/breeze.py",)
    ci = _selective_checks(files)
    lean = LeanSelectiveChecks(
        files=files, commit_ref=NEUTRAL_COMMIT, github_event=GithubEvents.PULL_REQUEST, default_branch="main"
    )
    assert ci.full_tests_needed is True
    full_commands = [
        i["command"]
        for i in build_local_verification_plan(ci, files, "main", full_tests_needed=True)["items"]
    ]
    lean_commands = [
        i["command"]
        for i in build_local_verification_plan(lean, files, "main", full_tests_needed=True)["items"]
    ]
    assert any(c.startswith("breeze testing core-tests") for c in full_commands)
    assert not any(c.startswith("breeze testing") for c in lean_commands)
    assert "cd dev/breeze && uv run --locked pytest" in lean_commands


def test_lean_and_full_plans_agree_when_the_change_does_not_expand():
    files = ("airflow-core/src/airflow/models/dag.py",)
    lean = LeanSelectiveChecks(
        files=files, commit_ref=NEUTRAL_COMMIT, github_event=GithubEvents.PULL_REQUEST, default_branch="main"
    )
    assert (
        build_local_verification_plan(lean, files, "main", full_tests_needed=False)["items"]
        == (
            build_local_verification_plan(_selective_checks(files), files, "main", full_tests_needed=False)[
                "items"
            ]
        )
    )


def test_full_plan_adds_the_jobs_ci_runs_on_every_pr():
    files = ("airflow-core/docs/index.rst",)
    sc = _selective_checks(files)
    lean = build_local_verification_plan(sc, files, "main", full_tests_needed=False)["items"]
    full = build_local_verification_plan(sc, files, "main", full_tests_needed=False, full=True)["items"]
    assert full[0]["command"] == f"SKIP={sc.skip_prek_hooks} prek run --all-files"
    assert [i["command"] for i in full[1:]] == [
        *(i["command"] for i in lean[1:]),
        "cd dev/breeze && uv run --locked pytest",
        "for d in "
        + " ".join(sorted(json.loads(sc.shared_distributions_as_json)))
        + "; do (cd shared/$d && uv run --group dev pytest) || exit 1; done",
    ]


def test_full_plan_does_not_duplicate_breeze_tests_for_a_breeze_change():
    files = ("dev/breeze/src/airflow_breeze/breeze.py",)
    full = build_local_verification_plan(
        _selective_checks(files), files, "main", full_tests_needed=True, full=True
    )
    assert [i["command"] for i in full["items"]].count("cd dev/breeze && uv run --locked pytest") == 1


def test_manual_stage_hooks_in_the_mapping_are_the_ones_ci_runs():
    workflow = (AIRFLOW_ROOT_PATH / ".github" / "workflows" / "ci-amd.yml").read_text()
    hooks = [
        hook
        for commands in FLAG_COMMANDS.values()
        for _, command, _ in commands
        for hook in re.findall(r"prek run --stage manual (\S+)", command)
    ]
    assert hooks
    for hook in hooks:
        assert f"--stage manual {hook} " in workflow, hook
