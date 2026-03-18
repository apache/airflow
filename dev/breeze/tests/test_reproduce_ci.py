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

import shlex
from unittest import mock

import pytest

from airflow_breeze.commands.developer_commands import _build_docs_reproduction_command
from airflow_breeze.commands.testing_commands import (
    _build_core_or_providers_tests_reproduction_command,
    _build_task_sdk_tests_reproduction_command,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.utils.reproduce_ci import (
    ReproductionCommand,
    build_local_reproduction_commands,
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
        f"Check out the same code as CI (pull request {pr_ref_kind} ref)",
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
    assert commands[1].argv == ["git", "checkout", "FETCH_HEAD"]


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


def test_build_docs_reproduction_command_contains_expected_flags():
    command = _build_docs_reproduction_command(
        clean_build=False,
        refresh_airflow_inventories=True,
        docs_only=True,
        github_repository="apache/airflow",
        include_not_ready_providers=True,
        include_removed_providers=False,
        include_commits=False,
        one_pass_only=False,
        package_filter=("apache-airflow-providers-*",),
        distributions_list="amazon google",
        spellcheck_only=False,
        doc_packages=("docs",),
    )

    assert shlex.join(command.argv) == (
        "breeze build-docs --refresh-airflow-inventories --docs-only "
        "--include-not-ready-providers --package-filter 'apache-airflow-providers-*' "
        "--distributions-list 'amazon google'"
    )


def test_build_core_or_providers_tests_reproduction_command_contains_expected_flags():
    command = _build_core_or_providers_tests_reproduction_command(
        command_name="providers-tests",
        airflow_constraints_reference="constraints-main",
        allow_pre_releases=False,
        backend="postgres",
        collect_only=False,
        custom_db_url=None,
        clean_airflow_installation=False,
        db_reset=True,
        downgrade_sqlalchemy=False,
        downgrade_pendulum=True,
        enable_coverage=False,
        excluded_parallel_test_types="Always",
        excluded_providers="apache.beam",
        extra_pytest_args=("providers/google/tests/unit/google/cloud/operators/test_bigquery.py",),
        force_sa_warnings=True,
        forward_credentials=False,
        force_lowest_dependencies=False,
        github_repository="apache/airflow",
        install_airflow_with_constraints=False,
        keep_env_variables=False,
        mount_sources="skip",
        no_db_cleanup=False,
        parallel_test_types="Providers[google]",
        parallelism=4,
        distribution_format="wheel",
        providers_constraints_location="/tmp/providers-constraints.txt",
        providers_skip_constraints=True,
        python="3.11",
        run_db_tests_only=False,
        run_in_parallel=True,
        skip_db_tests=False,
        skip_docker_compose_down=False,
        skip_providers="apache.facebook",
        test_timeout=120,
        test_type="All",
        total_test_timeout=3600,
        upgrade_boto=False,
        upgrade_sqlalchemy=True,
        use_airflow_version="main",
        use_distributions_from_dist=True,
        use_xdist=False,
        mysql_version="8",
        postgres_version="16",
    )

    formatted = shlex.join(command.argv)
    assert formatted.startswith(
        "breeze testing providers-tests --python 3.11 --backend postgres --mount-sources skip "
        "--test-timeout 120 --test-type All --airflow-constraints-reference constraints-main"
    )
    assert "--db-reset" in formatted
    assert "--force-sa-warnings" in formatted
    assert "--run-in-parallel" in formatted
    assert "--parallel-test-types 'Providers[google]'" in formatted
    assert "--parallelism 4 --total-test-timeout 3600" in formatted
    assert "--excluded-parallel-test-types Always" in formatted
    assert "--excluded-providers apache.beam" in formatted
    assert "--providers-constraints-location /tmp/providers-constraints.txt" in formatted
    assert "--providers-skip-constraints" in formatted
    assert "--skip-providers apache.facebook" in formatted
    assert "--use-airflow-version main" in formatted
    assert "--distribution-format wheel --use-distributions-from-dist" in formatted
    assert formatted.endswith("providers/google/tests/unit/google/cloud/operators/test_bigquery.py")


def test_build_task_sdk_tests_reproduction_command_contains_supported_flags_only():
    command = _build_task_sdk_tests_reproduction_command(
        collect_only=True,
        enable_coverage=True,
        extra_pytest_args=("task-sdk/tests/execution_time/test_supervisor.py",),
        force_sa_warnings=False,
        forward_credentials=True,
        github_repository="apache/airflow",
        keep_env_variables=True,
        mount_sources="skip",
        python="3.10",
        skip_docker_compose_down=True,
        test_timeout=90,
    )

    formatted = shlex.join(command.argv)
    assert formatted == (
        "breeze testing task-sdk-tests --python 3.10 --mount-sources skip --test-timeout 90 "
        "--no-force-sa-warnings --collect-only --enable-coverage --forward-credentials "
        "--keep-env-variables --skip-docker-compose-down "
        "task-sdk/tests/execution_time/test_supervisor.py"
    )
