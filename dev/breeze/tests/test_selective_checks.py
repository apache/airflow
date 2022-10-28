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

import pytest

from airflow_breeze.global_constants import GithubEvents
from airflow_breeze.utils.selective_checks import SelectiveChecks


def assert_outputs_are_printed(expected_outputs: dict[str, str], stderr: str):
    for name, value in expected_outputs.items():
        assert f"{name}={value}" in stderr


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        (
            pytest.param(
                ("INTHEWILD.md",),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "docs-build": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "",
                },
                id="No tests on simple change",
            )
        ),
        (
            pytest.param(
                ("airflow/api/file.py",),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "docs-build": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "API Always",
                },
                id="Only API tests and DOCS should run",
            )
        ),
        (
            pytest.param(
                (
                    "airflow/api/file.py",
                    "tests/providers/postgres/file.py",
                ),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "docs-build": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "API Always Providers[amazon,common.sql,google,postgres]",
                },
                id="API and providers tests and docs should run",
            )
        ),
        (
            pytest.param(
                ("tests/providers/apache/beam/file.py",),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "docs-build": "false",
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "Always Providers[apache.beam,google]",
                },
                id="Selected Providers and docs should run",
            )
        ),
        (
            pytest.param(
                ("docs/file.rst",),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "docs-build": "true",
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "",
                },
                id="Only docs builds should run - no tests needed",
            )
        ),
        (
            pytest.param(
                (
                    "chart/aaaa.txt",
                    "tests/providers/postgres/file.py",
                ),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "Always Providers[amazon,common.sql,google,postgres]",
                },
                id="Helm tests, providers (both upstream and downstream),"
                "kubernetes tests and docs should run",
            )
        ),
        (
            pytest.param(
                (
                    "INTHEWILD.md",
                    "chart/aaaa.txt",
                    "tests/providers/http/file.py",
                ),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "Always Providers[airbyte,apache.livy,dbt.cloud,dingding,discord,http]",
                },
                id="Helm tests, http and all relevant providers, kubernetes tests and "
                "docs should run even if unimportant files were added",
            )
        ),
        (
            pytest.param(
                (
                    "INTHEWILD.md",
                    "chart/aaaa.txt",
                    "tests/system/providers/airbyte/file.py",
                ),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "Always Providers[airbyte,http]",
                },
                id="Helm tests, airbyte/http providers, kubernetes tests and "
                "docs should run even if unimportant files were added",
            )
        ),
        (
            pytest.param(
                (
                    "INTHEWILD.md",
                    "chart/aaaa.txt",
                    "tests/system/utils/file.py",
                ),
                {
                    "all-python-versions": "['3.7']",
                    "all-python-versions-list-as-string": "3.7",
                    "image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "Always Providers",
                },
                id="Helm tests, all providers as common util system file changed, kubernetes tests and "
                "docs should run even if unimportant files were added",
            )
        ),
        (
            pytest.param(
                ("setup.py",),
                {
                    "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                    "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                    "image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "upgrade-to-newer-dependencies": "true",
                    "test-types": "API Always CLI Core Integration Other Providers WWW",
                },
                id="Everything should run - including all providers and upgrading to "
                "newer requirements as setup.py changed",
            )
        ),
        (
            pytest.param(
                ("generated/provider_dependencies.json",),
                {
                    "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                    "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                    "image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "upgrade-to-newer-dependencies": "true",
                    "test-types": "API Always CLI Core Integration Other Providers WWW",
                },
                id="Everything should run and upgrading to newer requirements as dependencies change",
            )
        ),
    ],
)
def test_expected_output_pull_request_main(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref="HEAD",
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, pr_labels, default_branch, expected_outputs,",
    [
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed",),
                "main",
                {
                    "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                    "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                    "image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "API Always CLI Core Integration Other Providers WWW",
                },
                id="Everything should run including all providers when full tests are needed",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                (
                    "another label",
                    "full tests needed",
                ),
                "main",
                {
                    "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                    "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                    "image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "API Always CLI Core Integration Other Providers WWW",
                },
                id="Everything should run including full providers when full "
                "tests are needed even with different label set as well",
            )
        ),
        (
            pytest.param(
                (),
                ("full tests needed",),
                "main",
                {
                    "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                    "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                    "image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "API Always CLI Core Integration Other Providers WWW",
                },
                id="Everything should run including full providers when"
                "full tests are needed even if no files are changed",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed",),
                "v2-3-stable",
                {
                    "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                    "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                    "image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "test-types": "API Always CLI Core Other WWW",
                },
                id="Everything should run except Providers and Integration "
                "when full tests are needed for non-main branch",
            )
        ),
    ],
)
def test_expected_output_full_tests_needed(
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    default_branch: str,
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref="HEAD",
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=pr_labels,
        default_branch=default_branch,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "docs-build": "false",
                "full-tests-needed": "false",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            (
                "chart/aaaa.txt",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "needs-helm-tests": "false",
                "image-build": "true",
                "run-tests": "true",
                "docs-build": "true",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "Always",
            },
            id="No Helm tests, No providers should run if only chart/providers changed in non-main",
        ),
        pytest.param(
            (
                "airflow/cli/test.py",
                "chart/aaaa.txt",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "Always CLI",
            },
            id="Only CLI tests and Kubernetes tests should run if cli/chart files changed in non-main branch",
        ),
        pytest.param(
            (
                "airflow/file.py",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "API Always CLI Core Other WWW",
            },
            id="All tests except providers and Integration should "
            "run if core file changed in non-main branch",
        ),
    ],
)
def test_expected_output_pull_request_v2_3(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref="HEAD",
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="v2-3-stable",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "docs-build": "false",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            (
                "airflow/cli/test.py",
                "chart/aaaa.txt",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "Always CLI",
            },
            id="CLI tests and Kubernetes tests should run if cli/chart files changed",
        ),
        pytest.param(
            (
                "airflow/file.py",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.7']",
                "all-python-versions-list-as-string": "3.7",
                "image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "test-types": "API Always CLI Core Integration Other Providers WWW",
            },
            id="All tests except should run if core file changed",
        ),
    ],
)
def test_expected_output_pull_request_target(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref="HEAD",
        github_event=GithubEvents.PULL_REQUEST_TARGET,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, pr_labels, default_branch, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "main",
            {
                "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                "image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "upgrade-to-newer-dependencies": "true",
                "test-types": "API Always CLI Core Integration Other Providers WWW",
            },
            id="All tests run on push even if unimportant file changed",
        ),
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "v2-3-stable",
            {
                "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                "image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "upgrade-to-newer-dependencies": "true",
                "test-types": "API Always CLI Core Other WWW",
            },
            id="All tests except Providers Integration and Helm run on push"
            " even if unimportant file changed in non-main branch",
        ),
        pytest.param(
            ("airflow/api.py",),
            (),
            "main",
            {
                "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
                "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
                "image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "upgrade-to-newer-dependencies": "true",
                "test-types": "API Always CLI Core Integration Other Providers WWW",
            },
            id="All tests run on push if core file changed",
        ),
    ],
)
def test_expected_output_push(
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    default_branch: str,
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref="HEAD",
        github_event=GithubEvents.PUSH,
        pr_labels=pr_labels,
        default_branch=default_branch,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "github_event",
    [
        GithubEvents.PUSH,
        GithubEvents.PULL_REQUEST,
        GithubEvents.PULL_REQUEST_TARGET,
        GithubEvents.PULL_REQUEST_WORKFLOW,
        GithubEvents.SCHEDULE,
    ],
)
def test_no_commit_provided_trigger_full_build_for_any_event_type(github_event):
    stderr = SelectiveChecks(
        files=(),
        commit_ref="",
        github_event=github_event,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "all-python-versions": "['3.7', '3.8', '3.9', '3.10']",
            "all-python-versions-list-as-string": "3.7 3.8 3.9 3.10",
            "image-build": "true",
            "needs-helm-tests": "true",
            "run-tests": "true",
            "docs-build": "true",
            "upgrade-to-newer-dependencies": "true"
            if github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE]
            else "false",
            "test-types": "API Always CLI Core Integration Other Providers WWW",
        },
        str(stderr),
    )


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("airflow/models/dag.py",),
            {
                "upgrade-to-newer-dependencies": "false",
            },
            id="Regular source changed",
        ),
        pytest.param(
            ("setup.py",),
            {
                "upgrade-to-newer-dependencies": "true",
            },
            id="Setup.py changed",
        ),
        pytest.param(
            ("setup.cfg",),
            {
                "upgrade-to-newer-dependencies": "true",
            },
            id="Setup.cfg changed",
        ),
        pytest.param(
            ("airflow/providers/microsoft/azure/provider.yaml",),
            {
                "upgrade-to-newer-dependencies": "true",
            },
            id="Provider.yaml changed",
        ),
        pytest.param(
            ("generated/provider_dependencies.json",),
            {
                "upgrade-to-newer-dependencies": "true",
            },
            id="Generated provider_dependencies changed",
        ),
    ],
)
def test_upgrade_to_newer_dependencies(files: tuple[str, ...], expected_outputs: dict[str, str]):
    stderr = SelectiveChecks(
        files=files,
        commit_ref="HEAD",
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))
