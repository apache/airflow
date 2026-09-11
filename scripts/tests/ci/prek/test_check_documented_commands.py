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

import shutil
import subprocess

import pytest
from ci.prek import check_documented_commands
from ci.prek.check_documented_commands import (
    BreezeCommand,
    BreezeIntrospectionError,
    build_breeze_registry,
    build_prek_hook_registry,
    extract_documented_commands,
    find_violations,
)


def _cmd(*, group: bool = False, passthrough: bool = False, opts: set[str] | None = None) -> BreezeCommand:
    return BreezeCommand(group=group, passthrough=passthrough, opts=frozenset(opts or ()))


# A miniature stand-in for the real click tree, exercising every shape the
# validator distinguishes: groups, a plain leaf with options, and passthrough
# leaves that forward their tail (so their options are not validated).
BREEZE_REGISTRY = {
    "breeze": _cmd(group=True),
    "breeze ci": _cmd(group=True),
    "breeze ci selective-check": _cmd(opts={"--commit-ref"}),
    "breeze testing": _cmd(group=True),
    "breeze testing core-tests": _cmd(passthrough=True),
    "breeze setup": _cmd(group=True),
    "breeze setup config": _cmd(opts={"--terminal-multiplexer", "--backend"}),
    "breeze run": _cmd(passthrough=True),
}
PREK_REGISTRY = frozenset({"ruff", "ruff-format", "mypy-dev"})


@pytest.mark.parametrize(
    ("snippet", "expected"),
    [
        ("`breeze ci selective-check --commit-ref <sha>`", "breeze ci selective-check"),
        ("`breeze run pytest path/to/test.py -xvs`", "breeze run pytest"),
        ("`breeze testing core-tests --run-in-parallel`", "breeze testing core-tests"),
        ("Run `prek run ruff --from-ref main` locally", "ruff"),
        ("the ``breeze ci selective-check`` command", "breeze ci selective-check"),
    ],
    ids=["placeholder-truncated", "args-kept", "subcommand", "prek-hook-id", "rst-double-backtick"],
)
def test_extract_finds_command_in_inline_span(snippet: str, expected: str):
    candidates = extract_documented_commands(snippet)

    assert [c.text for c in candidates] == [expected]


@pytest.mark.parametrize(
    "snippet",
    [
        "`prek run --from-ref main --stage pre-commit`",
        "`prek run mypy-<project> --all-files`",
        "`git commit -m 'breeze ci selective-check'`",
        "plain prose mentioning breeze testing without backticks",
        "see `breeze documentation <../dev/breeze/doc/03_developer_tasks.rst>`_ for details",
    ],
    ids=["no-hook-id", "placeholder-hook-id", "other-family", "no-code-span", "rst-hyperlink"],
)
def test_extract_skips_unvalidatable_snippets(snippet: str):
    assert extract_documented_commands(snippet) == []


def test_extract_scans_fenced_blocks_and_skips_heredoc_bodies():
    text = "\n".join(
        [
            "```bash",
            "breeze ci selective-check --commit-ref HEAD",
            "gh pr create --body \"$(cat <<'EOF'",
            "breeze not-a-real-command inside heredoc data",
            "EOF",
            ')"',
            "prek run ruff",
            "```",
        ]
    )
    candidates = extract_documented_commands(text)

    assert [c.text for c in candidates] == ["breeze ci selective-check", "ruff"]


def test_extract_scans_rst_indented_literal_block():
    text = "\n".join(
        [
            "Run the config command:",
            "",
            ".. code-block:: bash",
            "",
            "  breeze setup config --terminal_multiplexer tmux",
            "",
            "back to prose mentioning breeze testing without backticks",
        ]
    )
    candidates = extract_documented_commands(text)

    assert [c.raw for c in candidates] == ["breeze setup config --terminal_multiplexer tmux"]


@pytest.mark.parametrize(
    ("snippet", "reason_contains"),
    [
        ("`breeze selective-checks --commit-ref <sha>`", "not a subcommand of `breeze`"),
        ("`breeze ci selective-checks`", "not a subcommand of `breeze ci`"),
        ("`breeze testing core-testz`", "not a subcommand of `breeze testing`"),
        ("`breeze setup config --terminal_multiplexer tmux`", "not an option of `breeze setup config`"),
        ("`prek run ruffff`", "not a hook id"),
    ],
    ids=["unknown-top", "unknown-subcommand", "typo-subcommand", "typo-option", "unknown-hook"],
)
def test_find_violations_flags_drift(snippet: str, reason_contains: str):
    violations = find_violations(extract_documented_commands(snippet), BREEZE_REGISTRY, PREK_REGISTRY)

    assert len(violations) == 1
    assert reason_contains in violations[0].reason


@pytest.mark.parametrize(
    "snippet",
    [
        "`breeze ci selective-check --commit-ref <sha>`",
        "`breeze run pytest path/to/test.py -xvs`",
        "`breeze run airflow dags list`",
        "`breeze testing core-tests --any-forwarded-flag`",
        "`breeze testing <test_group> --run-in-parallel`",
        "`breeze setup config --terminal-multiplexer tmux`",
        "`breeze testing --help`",
        "`prek run ruff --from-ref main`",
    ],
    ids=[
        "valid-option",
        "passthrough-args",
        "passthrough-nested-words",
        "passthrough-skips-option-check",
        "placeholder-stops-descent",
        "correct-option-spelling",
        "help-always-valid",
        "valid-hook",
    ],
)
def test_find_violations_accepts_valid_commands(snippet: str):
    assert find_violations(extract_documented_commands(snippet), BREEZE_REGISTRY, PREK_REGISTRY) == []


def test_find_violations_suggests_close_match():
    violations = find_violations(
        extract_documented_commands("`breeze ci selective-checks`"), BREEZE_REGISTRY, PREK_REGISTRY
    )

    assert "did you mean `selective-check`?" in violations[0].reason


def test_prek_registry_includes_root_and_nested_hook_ids():
    registry = build_prek_hook_registry()

    assert "ruff" in registry
    assert "generate-agent-skills" in registry
    # defined in dev/.pre-commit-config.yaml, not the root config
    assert "mypy-dev" in registry


def test_build_breeze_registry_raises_clean_error_when_introspection_fails(monkeypatch):
    failed = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="ImportError: boom")
    monkeypatch.setattr(check_documented_commands, "check_uv_version", lambda *a, **k: None)
    monkeypatch.setattr(check_documented_commands.subprocess, "run", lambda *a, **k: failed)

    with pytest.raises(BreezeIntrospectionError, match="boom"):
        build_breeze_registry()


def test_main_skips_without_failing_when_breeze_introspection_fails(monkeypatch):
    """A broken breeze env must warn and skip (0), not block the commit (1)."""

    def boom() -> dict:
        raise BreezeIntrospectionError("broken breeze env")

    monkeypatch.setattr(check_documented_commands, "build_breeze_registry", boom)

    assert check_documented_commands.main() == 0


@pytest.mark.skipif(shutil.which("uv") is None, reason="needs uv to introspect breeze")
def test_breeze_registry_matches_real_click_tree():
    registry = build_breeze_registry()

    assert "breeze ci selective-check" in registry
    assert registry["breeze ci selective-check"].group is False
    # `breeze run` forwards its tail to another program, so it is passthrough
    assert registry["breeze run"].passthrough is True
    # the #69861 bug: this was documented but never registered
    assert "breeze selective-checks" not in registry
