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

import ast
import textwrap
from pathlib import Path

import pytest
from check_deferred_hook_configuration import (
    collect_errors,
    find_defer_sites,
    find_hand_built_hooks,
    find_unparseable_modules,
    find_unreadable_defer_sites,
    resolve_trigger_constructions,
)

CONFIGURED = "region_name=self.region_name, verify=self.verify, botocore_config=self.botocore_config"


@pytest.fixture
def aws_tree(tmp_path):
    """Factory fixture: write modules into an aws-package-shaped tree and return its root."""

    def _write(modules: dict[str, str]) -> Path:
        root = tmp_path / "aws"
        root.mkdir(parents=True, exist_ok=True)
        for relative_path, source in modules.items():
            path = root / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(textwrap.dedent(source))
        return root

    return _write


@pytest.fixture(autouse=True)
def empty_allowlists(monkeypatch):
    """Start every case from empty allowlists; the real entries describe the real tree, not a fixture."""
    for name in ("UNREADABLE_DEFER_SITES", "PENDING_MIGRATION", "HAND_BUILT_HOOK_EXCEPTIONS"):
        monkeypatch.setattr(f"check_deferred_hook_configuration.{name}", frozenset())


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        pytest.param("SomeTrigger(x=1)", 1, id="call"),
        pytest.param("A() if flag else B()", 2, id="conditional-both-readable"),
        pytest.param("trigger", None, id="bare-name"),
        pytest.param("self._trigger", None, id="attribute"),
        pytest.param("triggers[kind]", None, id="subscript"),
        pytest.param("A() if flag else self._trigger", None, id="conditional-one-unreadable"),
        pytest.param("TRIGGERS[kind](x=1)", None, id="unnameable-callee"),
        pytest.param("module.SomeTrigger(x=1)", 1, id="module-qualified-callee"),
    ],
)
def test_unreadable_trigger_expressions_resolve_to_none(expression, expected):
    """Anything the sweep cannot resolve must report None so the site is forced onto the allowlist."""
    constructions = resolve_trigger_constructions(ast.parse(expression, mode="eval").body)

    assert (constructions if constructions is None else len(constructions)) == expected


def test_find_defer_sites_reports_the_parameters_not_passed(aws_tree):
    root = aws_tree(
        {
            "operators/glue.py": """
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger(job_name=self.job_name, region_name=self.region_name))
        """,
        }
    )

    assert find_defer_sites(root) == [("operators/glue.py", 4, "GlueTrigger", ["verify", "botocore_config"])]


def test_find_defer_sites_is_satisfied_by_a_fully_configured_site(aws_tree):
    root = aws_tree(
        {
            "operators/glue.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger({CONFIGURED}))
        """,
        }
    )

    assert find_defer_sites(root) == [("operators/glue.py", 4, "GlueTrigger", [])]


def test_find_defer_sites_covers_both_branches_of_a_conditional(aws_tree):
    root = aws_tree(
        {
            "operators/emr.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=A({CONFIGURED}) if self.x else B(waiter_delay=1))
        """,
        }
    )

    assert [(name, missing) for _, _, name, missing in find_defer_sites(root)] == [
        ("A", []),
        ("B", ["region_name", "verify", "botocore_config"]),
    ]


def test_find_defer_sites_skips_triggers_that_take_no_configuration(aws_tree):
    root = aws_tree(
        {
            "operators/eks.py": """
        class O:
            def execute(self, context):
                self.defer(trigger=EksPodTrigger(pod_name=self.pod_name))
        """,
        }
    )

    assert find_defer_sites(root) == []


def test_walk_finds_defer_sites_outside_operators_and_sensors(aws_tree):
    """``defer`` is a BaseOperator method, so a nested subpackage must not be skipped."""
    root = aws_tree(
        {
            "nested/deeper/thing.py": """
        class O:
            def execute(self, context):
                self.defer(trigger=NestedTrigger(job_id=1))
        """,
        }
    )

    assert [source for source, _, _, _ in find_defer_sites(root)] == ["nested/deeper/thing.py"]


def test_find_unreadable_defer_sites_records_the_expression(aws_tree):
    root = aws_tree(
        {
            "operators/eks.py": """
        class O:
            def execute(self, context):
                self.defer(trigger=self._trigger)
        """,
        }
    )

    assert find_unreadable_defer_sites(root) == {("operators/eks.py", "self._trigger")}


def test_find_hand_built_hooks_accepts_the_config_spelling(aws_tree):
    """``AwsGenericHook`` names the botocore config ``config``, not ``botocore_config``."""
    root = aws_tree(
        {
            "triggers/glue.py": """
        class T:
            def logs(self):
                return AwsLogsHook(region_name=self.region_name, verify=self.verify, config=self.config)
        """,
        }
    )

    assert find_hand_built_hooks(root) == [("triggers/glue.py", 4, "AwsLogsHook", [])]


def test_find_hand_built_hooks_keys_on_the_path_not_the_basename(aws_tree):
    """Two trigger modules may share a basename, so an exception must not cover both."""
    root = aws_tree(
        {
            "triggers/eks.py": """
        class T:
            def logs(self):
                return EksHook(aws_conn_id=self.aws_conn_id)
        """,
            "triggers/nested/eks.py": """
        class T:
            def logs(self):
                return EksHook(aws_conn_id=self.aws_conn_id)
        """,
        }
    )

    assert sorted(source for source, _, _, _ in find_hand_built_hooks(root)) == [
        "triggers/eks.py",
        "triggers/nested/eks.py",
    ]


def test_collect_errors_is_empty_for_a_fully_configured_tree(aws_tree):
    root = aws_tree(
        {
            "operators/glue.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger({CONFIGURED}))
        """,
        }
    )

    assert collect_errors(root) == []


def test_collect_errors_flags_a_tree_with_no_defer_sites(aws_tree):
    root = aws_tree({"operators/glue.py": "class O:\n    pass\n"})

    assert "the check is not looking at it" in "\n".join(collect_errors(root))


def test_collect_errors_flags_an_unconfigured_defer_site(aws_tree):
    root = aws_tree(
        {
            "operators/glue.py": """
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger(job_name=self.job_name))
        """,
        }
    )

    (error,) = collect_errors(root)
    assert "defers to GlueTrigger without passing region_name, verify, botocore_config" in error


def test_collect_errors_flags_an_unacknowledged_unreadable_site(aws_tree):
    root = aws_tree(
        {
            "operators/glue.py": """
        class O:
            def execute(self, context):
                self.defer(trigger=self._trigger)
        """,
        }
    )

    errors = "\n".join(collect_errors(root))
    assert "cannot be read statically" in errors
    assert "Add it to UNREADABLE_DEFER_SITES" in errors


def test_collect_errors_flags_a_stale_unreadable_entry(aws_tree, monkeypatch):
    monkeypatch.setattr(
        "check_deferred_hook_configuration.UNREADABLE_DEFER_SITES",
        frozenset({("operators/glue.py", "self._gone")}),
    )
    root = aws_tree(
        {
            "operators/glue.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger({CONFIGURED}))
        """,
        }
    )

    assert "Drop it from UNREADABLE_DEFER_SITES" in "\n".join(collect_errors(root))


def test_collect_errors_flags_a_pending_migration_entry_that_is_now_fixed(aws_tree, monkeypatch):
    """The allowlist cannot outlive the work it tracks."""
    monkeypatch.setattr(
        "check_deferred_hook_configuration.PENDING_MIGRATION",
        frozenset({("sensors/batch.py", "BatchJobTrigger")}),
    )
    root = aws_tree(
        {
            "sensors/batch.py": f"""
        class S:
            def execute(self, context):
                self.defer(trigger=BatchJobTrigger({CONFIGURED}))
        """,
        }
    )

    (error,) = collect_errors(root)
    assert "Drop it from PENDING_MIGRATION" in error


def test_collect_errors_stays_quiet_for_a_pending_migration_entry_still_needed(aws_tree, monkeypatch):
    monkeypatch.setattr(
        "check_deferred_hook_configuration.PENDING_MIGRATION",
        frozenset({("sensors/batch.py", "BatchJobTrigger")}),
    )
    root = aws_tree(
        {
            "sensors/batch.py": """
        class S:
            def execute(self, context):
                self.defer(trigger=BatchJobTrigger(job_id=self.job_id))
        """,
        }
    )

    assert collect_errors(root) == []


def test_collect_errors_flags_an_unconfigured_hand_built_hook(aws_tree):
    root = aws_tree(
        {
            "operators/glue.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger({CONFIGURED}))
        """,
            "triggers/glue.py": """
        class T:
            def logs(self):
                return AwsLogsHook(aws_conn_id=self.aws_conn_id)
        """,
        }
    )

    (error,) = collect_errors(root)
    assert "builds AwsLogsHook without region_name, verify, botocore_config" in error


def test_collect_errors_flags_a_hand_built_exception_that_is_now_fixed(aws_tree, monkeypatch):
    monkeypatch.setattr(
        "check_deferred_hook_configuration.HAND_BUILT_HOOK_EXCEPTIONS",
        frozenset({("triggers/glue.py", "AwsLogsHook")}),
    )
    root = aws_tree(
        {
            "operators/glue.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger({CONFIGURED}))
        """,
            "triggers/glue.py": f"""
        class T:
            def logs(self):
                return AwsLogsHook({CONFIGURED})
        """,
        }
    )

    (error,) = collect_errors(root)
    assert "Drop it from HAND_BUILT_HOOK_EXCEPTIONS" in error


def test_a_module_that_cannot_be_parsed_is_reported_not_skipped(aws_tree):
    """prek does not guarantee ruff runs first, so the sweep must survive a half-edited file."""
    root = aws_tree(
        {
            "operators/broken.py": "class O:\n    def execute(self:\n",
            "operators/glue.py": f"""
        class O:
            def execute(self, context):
                self.defer(trigger=GlueTrigger({CONFIGURED}))
        """,
        }
    )

    assert find_unparseable_modules(root) == ["operators/broken.py"]
    # The readable file is still swept rather than the whole run dying on its neighbour.
    assert [name for _, _, name, _ in find_defer_sites(root)] == ["GlueTrigger"]
    (error,) = collect_errors(root)
    assert "operators/broken.py could not be parsed" in error
