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

from unittest import mock

import click
import click.testing
import pytest

from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.utils.reproduce_ci import (
    ReproductionCommand,
    build_local_reproduction_commands,
    build_reproduction_command_from_context,
    print_local_reproduction,
    should_print_local_reproduction,
)


@pytest.mark.parametrize(
    ("env_vars", "expected"),
    [
        ({"CI": "true", "GITHUB_ACTIONS": "true"}, True),
        ({"CI": "true", "GITHUB_ACTIONS": "false"}, False),
        ({"CI": "false", "GITHUB_ACTIONS": "true"}, False),
        ({}, False),
    ],
)
def test_should_print_local_reproduction_only_in_github_actions(env_vars, expected, monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    assert should_print_local_reproduction() is expected


def test_build_local_reproduction_commands_builds_ci_image_locally(monkeypatch):
    monkeypatch.delenv("GITHUB_REF", raising=False)
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    monkeypatch.setenv("GITHUB_RUN_ID", "98765")
    build_params = BuildCiParams(
        github_repository="someone/airflow",
        platform="linux/arm64",
        python="3.11",
    )

    commands = build_local_reproduction_commands(
        command_params=build_params,
        main_command=ReproductionCommand(argv=["breeze", "build-docs", "--docs-only"]),
    )

    assert [command.comment for command in commands] == [
        "Check out the same commit",
        "Build the CI image locally",
        None,
    ]
    assert commands[0].argv == ["git", "checkout", "abc123"]
    assert commands[1].argv == [
        "breeze",
        "ci-image",
        "build",
        "--github-repository",
        "someone/airflow",
        "--platform",
        "linux/arm64",
        "--python",
        "3.11",
    ]


@pytest.mark.parametrize("pr_ref_kind", ["merge", "head"])
def test_build_local_reproduction_commands_fetches_pull_request_ref(pr_ref_kind, monkeypatch):
    github_ref = f"refs/pull/42/{pr_ref_kind}"
    monkeypatch.setenv("GITHUB_REF", github_ref)
    monkeypatch.setenv("GITHUB_SHA", "merge-sha")
    monkeypatch.setenv("GITHUB_RUN_ID", "98765")
    build_params = BuildCiParams(
        github_repository="someone/airflow",
        platform="linux/amd64",
        python="3.10",
    )

    commands = build_local_reproduction_commands(
        command_params=build_params,
        main_command=ReproductionCommand(argv=["breeze", "build-docs", "--docs-only"]),
    )

    assert [command.comment for command in commands] == [
        f"Fetch the same code as CI (pull request {pr_ref_kind} ref)",
        None,
        "Build the CI image locally",
        None,
    ]
    assert commands[0].argv == [
        "git",
        "fetch",
        "https://github.com/someone/airflow.git",
        github_ref,
    ]
    assert commands[1].argv == ["git", "checkout", "merge-sha"]


def test_build_local_reproduction_commands_builds_ci_image_for_default_repo(monkeypatch):
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    monkeypatch.delenv("GITHUB_REF", raising=False)
    monkeypatch.setenv("GITHUB_SHA", "def456")
    build_params = BuildCiParams(platform="linux/amd64", python="3.10")

    commands = build_local_reproduction_commands(
        command_params=build_params,
        main_command=ReproductionCommand(argv=["breeze", "build-docs", "--docs-only"]),
    )

    assert commands[1].argv == [
        "breeze",
        "ci-image",
        "build",
        "--platform",
        "linux/amd64",
        "--python",
        "3.10",
    ]


@mock.patch("airflow_breeze.utils.reproduce_ci.get_console", autospec=True)
def test_print_local_reproduction_renders_copyable_commands(mock_get_console, monkeypatch):
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    print_local_reproduction(
        [
            ReproductionCommand(argv=["git", "checkout", "abc123"], comment="Check out the same commit"),
            ReproductionCommand(
                argv=["breeze", "build-docs", "--docs-only"],
                comment="Run the same Breeze command locally",
            ),
        ]
    )

    assert mock_get_console.return_value.print.call_count == 2
    rendered_output = mock_get_console.return_value.print.call_args_list[1].args[0]
    assert "# 1. Check out the same commit" in rendered_output
    assert "git checkout abc123" in rendered_output
    assert "breeze build-docs --docs-only" in rendered_output


# ---------------------------------------------------------------------------
# Tests for build_reproduction_command_from_context
# ---------------------------------------------------------------------------


def _build_test_command(**options):
    """Build a simple click command with the given options for testing."""

    @click.command("test-cmd")
    def cmd(**kwargs):
        pass

    for _name, opt in options.items():
        cmd = opt(cmd)
    return cmd


def _invoke_and_get_context(cmd, args, env=None):
    """Invoke a click command and capture the context."""
    captured_ctx = {}

    original_invoke = cmd.invoke

    def patched_invoke(ctx):
        captured_ctx["ctx"] = ctx
        return original_invoke(ctx)

    cmd.invoke = patched_invoke
    runner = click.testing.CliRunner(env=env or {})
    result = runner.invoke(cmd, args, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return captured_ctx["ctx"]


class TestBuildReproductionCommandFromContext:
    """Tests for the generic Click context-based command renderer."""

    def test_simple_bool_flag_emitted_when_true(self):
        @click.command("my-cmd")
        @click.option("--verbose-output", is_flag=True, default=False)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--verbose-output"])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--verbose-output"]

    def test_simple_bool_flag_omitted_when_default(self):
        @click.command("my-cmd")
        @click.option("--verbose-output", is_flag=True, default=False)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd"]

    def test_flag_pair_emits_positive_side(self):
        @click.command("my-cmd")
        @click.option("--force/--no-force", default=False)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--force"])
        result = build_reproduction_command_from_context(ctx)
        assert "--force" in result.argv
        assert "--no-force" not in result.argv

    def test_flag_pair_emits_negative_side(self):
        @click.command("my-cmd")
        @click.option("--force/--no-force", default=True)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--no-force"])
        result = build_reproduction_command_from_context(ctx)
        assert "--no-force" in result.argv
        assert result.argv.count("--force") == 0

    def test_flag_pair_omitted_when_default(self):
        @click.command("my-cmd")
        @click.option("--force/--no-force", default=True)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [])
        result = build_reproduction_command_from_context(ctx)
        assert "--force" not in result.argv
        assert "--no-force" not in result.argv

    def test_flag_pair_prefers_long_form(self):
        @click.command("my-cmd")
        @click.option("-f", "--force/--no-force", default=False)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["-f"])
        result = build_reproduction_command_from_context(ctx)
        assert "--force" in result.argv
        assert "-f" not in result.argv

    def test_string_option_emitted_when_explicit(self):
        @click.command("my-cmd")
        @click.option("--backend", default="sqlite")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--backend", "postgres"])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--backend", "postgres"]

    def test_string_option_omitted_when_default(self):
        @click.command("my-cmd")
        @click.option("--backend", default="sqlite")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd"]

    def test_multiple_option_repeats_flag(self):
        @click.command("my-cmd")
        @click.option("--package-filter", multiple=True)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--package-filter", "foo", "--package-filter", "bar"])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--package-filter", "foo", "--package-filter", "bar"]

    def test_positional_arguments_appended_at_end(self):
        @click.command("my-cmd")
        @click.option("--flag", is_flag=True)
        @click.argument("files", nargs=-1)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--flag", "file1.py", "file2.py"])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--flag", "file1.py", "file2.py"]

    def test_expose_value_false_option_excluded(self):
        @click.command("my-cmd")
        @click.option("--verbose", is_flag=True, expose_value=False)
        @click.option("--backend", default="sqlite")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--verbose", "--backend", "postgres"])
        result = build_reproduction_command_from_context(ctx)
        assert "--verbose" not in result.argv
        assert "--backend" in result.argv

    def test_excluded_params_filtered_out(self):
        @click.command("my-cmd")
        @click.option("--debug-resources", is_flag=True)
        @click.option("--backend", default="sqlite")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--debug-resources", "--backend", "postgres"])
        result = build_reproduction_command_from_context(ctx)
        assert "--debug-resources" not in result.argv
        assert "--backend" in result.argv

    def test_envvar_source_included(self):
        @click.command("my-cmd")
        @click.option("--backend", default="sqlite", envvar="BACKEND")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [], env={"BACKEND": "postgres"})
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--backend", "postgres"]

    def test_envvar_same_as_default_still_included(self):
        """When envvar explicitly sets the same value as default, it should still be emitted."""

        @click.command("my-cmd")
        @click.option("--backend", default="sqlite", envvar="BACKEND")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [], env={"BACKEND": "sqlite"})
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--backend", "sqlite"]

    def test_custom_comment(self):
        @click.command("my-cmd")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [])
        result = build_reproduction_command_from_context(ctx, comment="Custom comment")
        assert result.comment == "Custom comment"

    def test_default_comment(self):
        @click.command("my-cmd")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, [])
        result = build_reproduction_command_from_context(ctx)
        assert result.comment == "Run the same Breeze command locally"

    def test_subcommand_path(self):
        @click.group()
        def grp():
            pass

        @grp.command("sub-cmd")
        @click.option("--flag", is_flag=True)
        def sub_cmd(**kwargs):
            pass

        captured_ctx = {}

        original_invoke = sub_cmd.invoke

        def patched_invoke(ctx):
            captured_ctx["ctx"] = ctx
            return original_invoke(ctx)

        sub_cmd.invoke = patched_invoke
        runner = click.testing.CliRunner()
        result = runner.invoke(grp, ["sub-cmd", "--flag"], catch_exceptions=False)
        assert result.exit_code == 0
        ctx = captured_ctx["ctx"]
        repro = build_reproduction_command_from_context(ctx)
        assert repro.argv == ["grp", "sub-cmd", "--flag"]

    def test_integer_option_converted_to_string(self):
        @click.command("my-cmd")
        @click.option("--timeout", type=int, default=60)
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["--timeout", "120"])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--timeout", "120"]

    def test_prefers_long_option_form(self):
        @click.command("my-cmd")
        @click.option("-b", "--backend", default="sqlite")
        def cmd(**kwargs):
            pass

        ctx = _invoke_and_get_context(cmd, ["-b", "postgres"])
        result = build_reproduction_command_from_context(ctx)
        assert result.argv == ["my-cmd", "--backend", "postgres"]
