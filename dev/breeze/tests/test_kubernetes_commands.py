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

from contextlib import contextmanager
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.commands import kubernetes_commands

FORK_REPOSITORY = "contributor/airflow"


class _ImmediatePool:
    """Runs each task in the calling process, so its keyword arguments are checked right away."""

    def apply_async(self, func, kwds):
        return func(**kwds)


@contextmanager
def _immediate_run_with_pool(all_params, **kwargs):
    yield _ImmediatePool(), [None] * len(all_params)


@mock.patch.object(kubernetes_commands, "run_command", autospec=True)
@mock.patch.object(kubernetes_commands, "check_if_image_exists", autospec=True, return_value=True)
def test_rebuild_k8s_image_builds_from_the_image_of_the_github_repository(mock_exists, mock_run_command):
    mock_run_command.return_value = mock.Mock(returncode=0)

    returncode, _ = kubernetes_commands._rebuild_k8s_image(
        python="3.11",
        rebuild_base_image=False,
        copy_local_sources=False,
        use_uv=True,
        github_repository="Contributor/Airflow",
        output=None,
    )

    assert returncode == 0
    mock_exists.assert_called_once_with(image=f"ghcr.io/contributor/airflow/{AIRFLOW_BRANCH}/prod/python3.11")
    command = mock_run_command.call_args.args[0]
    assert command[:4] == [
        "docker",
        "build",
        "--tag",
        f"ghcr.io/contributor/airflow/{AIRFLOW_BRANCH}/prod/python3.11-kubernetes:latest",
    ]
    assert (
        mock_run_command.call_args.kwargs["input"]
        .lstrip()
        .startswith(f"FROM ghcr.io/contributor/airflow/{AIRFLOW_BRANCH}/prod/python3.11\n")
    )


@mock.patch.object(kubernetes_commands, "run_command_with_k8s_env", autospec=True)
def test_upload_k8s_image_loads_the_image_of_the_github_repository(mock_run_command):
    mock_run_command.return_value = mock.Mock(returncode=0)

    kubernetes_commands._upload_k8s_image(
        python="3.11", kubernetes_version="v1.31.12", github_repository=FORK_REPOSITORY, output=None
    )

    assert mock_run_command.call_args.args[0][-1] == (
        f"ghcr.io/contributor/airflow/{AIRFLOW_BRANCH}/prod/python3.11-kubernetes"
    )


@pytest.mark.parametrize(
    ("command", "args", "helper"),
    [
        pytest.param(kubernetes_commands.build_k8s_image, [], "_rebuild_k8s_image", id="build-k8s-image"),
        pytest.param(
            kubernetes_commands.build_k8s_image,
            ["--run-in-parallel", "--python-versions", "3.11"],
            "_rebuild_k8s_image",
            id="build-k8s-image-parallel",
        ),
        pytest.param(kubernetes_commands.upload_k8s_image, [], "_upload_k8s_image", id="upload-k8s-image"),
        pytest.param(
            kubernetes_commands.upload_k8s_image,
            ["--run-in-parallel", "--python-versions", "3.11", "--kubernetes-versions", "v1.31.12"],
            "_upload_k8s_image",
            id="upload-k8s-image-parallel",
        ),
        pytest.param(
            kubernetes_commands.run_complete_tests, [], "_run_complete_tests", id="run-complete-tests"
        ),
        pytest.param(
            kubernetes_commands.run_complete_tests,
            ["--run-in-parallel", "--python-versions", "3.11", "--kubernetes-versions", "v1.31.12"],
            "_run_complete_tests",
            id="run-complete-tests-parallel",
        ),
    ],
)
@mock.patch.object(kubernetes_commands, "check_async_run_results", autospec=True)
@mock.patch.object(kubernetes_commands, "run_with_pool", _immediate_run_with_pool)
@mock.patch.object(kubernetes_commands, "make_sure_kubernetes_tools_are_installed", autospec=True)
@mock.patch.object(kubernetes_commands, "sync_virtualenv", autospec=True)
def test_commands_pass_the_github_repository_from_the_environment(
    mock_sync, _mock_tools, _mock_check_results, command, args, helper
):
    mock_sync.return_value = mock.Mock(returncode=0)
    with mock.patch.object(kubernetes_commands, helper, autospec=True, return_value=(0, "")) as mock_helper:
        result = CliRunner().invoke(command, args, env={"GITHUB_REPOSITORY": FORK_REPOSITORY})

    assert result.exit_code == 0, result.output
    mock_helper.assert_called_once()
    assert mock_helper.call_args.kwargs["github_repository"] == FORK_REPOSITORY


@mock.patch.object(kubernetes_commands, "run_command", autospec=True)
@mock.patch.object(kubernetes_commands, "get_kubectl_cluster_name", autospec=True, return_value="airflow")
@mock.patch.object(kubernetes_commands, "get_k8s_env", autospec=True, return_value={"SHELL": "/bin/bash"})
def test_run_tests_does_not_relock(mock_get_k8s_env, mock_get_cluster_name, mock_run_command):
    mock_run_command.return_value = mock.Mock(returncode=0)

    kubernetes_commands._run_tests(
        python="3.11",
        kubernetes_version="v1.31.12",
        output=None,
        executor="LocalExecutor",
        test_args=[],
    )

    assert mock_run_command.call_args.args[0][-1].startswith("uv run --frozen pytest ")
