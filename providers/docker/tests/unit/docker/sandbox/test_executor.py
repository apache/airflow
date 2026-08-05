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

from functools import partial
from pathlib import PurePosixPath
from unittest import mock
from uuid import uuid4

import pytest

from airflow.executors.workloads import BundleInfo, ExecuteTask, TaskInstanceDTO
from airflow.providers.common.sandbox.executor import BaseSandboxExecutor
from airflow.providers.docker.sandbox.driver import DockerSandboxDriver
from airflow.providers.docker.sandbox.exceptions import DockerSandboxConfigurationError
from airflow.providers.docker.sandbox.executor import DockerSandboxExecutor
from airflow.providers.docker.sandbox.models import DockerSandboxDriverConfig


def make_workload(executor_config: dict | None = None) -> ExecuteTask:
    return ExecuteTask(
        ti=TaskInstanceDTO(
            id=uuid4(),
            dag_version_id=uuid4(),
            task_id="task",
            dag_id="dag",
            run_id="run",
            try_number=1,
            map_index=-1,
            pool_slots=1,
            queue="default",
            priority_weight=1,
            external_executor_id=str(uuid4()),
            executor_config=executor_config,
        ),
        dag_rel_path=PurePosixPath("dag.py"),
        token="jwt",
        bundle_info=BundleInfo(name="dags-folder", version=None),
        log_path="dag/task.log",
    )


def test_executor_is_non_production_common_engine_binding() -> None:
    executor = DockerSandboxExecutor(parallelism=4)

    assert isinstance(executor, BaseSandboxExecutor)
    assert executor.driver_id == "docker-sandbox"
    assert executor.is_production is False
    assert executor.pre_assigns_external_executor_id is True
    assert executor.requires_terminal_cleanup is True


@mock.patch.object(DockerSandboxExecutor, "get_driver_factory", autospec=True)
def test_start_refuses_default_without_resolving_driver(mock_get_driver_factory) -> None:
    executor = DockerSandboxExecutor(parallelism=4)

    with pytest.raises(DockerSandboxConfigurationError, match="allow_non_production=True"):
        executor.start()

    mock_get_driver_factory.assert_not_called()


@mock.patch.object(BaseSandboxExecutor, "start", autospec=True)
def test_start_delegates_after_explicit_opt_in(mock_start, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__ALLOW_NON_PRODUCTION", "true")
    executor = DockerSandboxExecutor(parallelism=4)

    executor.start()

    mock_start.assert_called_once_with(executor)


def test_launch_config_merges_deployment_and_task_options(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TEMPLATE", "default-runtime")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_CPUS", "2")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_MEMORY", "4g")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_WORKDIR", "/workspace")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TIMEOUT_SECONDS", "600")
    executor = DockerSandboxExecutor(parallelism=4)
    workload = make_workload(
        {
            "sandbox": {
                "env": {"MODEL": "small"},
                "timeout_seconds": 90,
                "ttl_seconds": 120,
                "workdir": "/workspace/job",
            },
            "docker_sandbox": {
                "cpus": 8,
                "template": "gpu-runtime",
            },
        }
    )

    launch = executor.build_launch_config(workload)

    assert launch.provider_config == {
        "cpus": 8,
        "memory": "4g",
        "template": "gpu-runtime",
    }
    assert launch.env == {"MODEL": "small"}
    assert launch.keep is False
    assert launch.timeout_seconds == 90
    assert launch.ttl_seconds == 120
    assert launch.workdir == "/workspace/job"


def test_launch_config_defaults_ttl_to_execution_timeout(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TEMPLATE", "default-runtime")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TIMEOUT_SECONDS", "600")
    executor = DockerSandboxExecutor(parallelism=4)

    launch = executor.build_launch_config(make_workload())

    assert launch.timeout_seconds == 600
    assert launch.ttl_seconds == 600


def test_launch_config_rejects_keep_without_a_hard_ttl(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TEMPLATE", "default-runtime")
    executor = DockerSandboxExecutor(parallelism=4)

    with pytest.raises(DockerSandboxConfigurationError, match="has no hard TTL"):
        executor.build_launch_config(make_workload({"sandbox": {"keep": True}}))


@pytest.mark.parametrize(
    "option",
    [
        "cli_timeout_seconds",
        "env",
        "keep",
        "launch_acceptance_timeout",
        "network",
        "sbx_binary",
        "timeout_seconds",
        "ttl_seconds",
        "workdir",
        "workspace_root",
    ],
)
def test_provider_task_config_rejects_policy_and_portable_options(monkeypatch, option) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TEMPLATE", "default-runtime")
    executor = DockerSandboxExecutor(parallelism=4)
    workload = make_workload({"docker_sandbox": {option: "override"}})

    with pytest.raises(
        DockerSandboxConfigurationError,
        match=f"unsupported Docker Sandbox executor_config keys: {option}",
    ):
        executor.build_launch_config(workload)


def test_default_template_is_required() -> None:
    executor = DockerSandboxExecutor(parallelism=4)

    with pytest.raises(
        DockerSandboxConfigurationError,
        match=r"\[docker_sandbox] default_template must be a non-empty string",
    ):
        executor.build_launch_config(make_workload())


def test_provider_task_config_must_be_a_mapping(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TEMPLATE", "default-runtime")
    executor = DockerSandboxExecutor(parallelism=4)

    with pytest.raises(
        DockerSandboxConfigurationError,
        match=r"executor_config\['docker_sandbox'] must be a mapping",
    ):
        executor.build_launch_config(make_workload({"docker_sandbox": "template-name"}))


def test_default_cpus_must_be_an_integer(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_TEMPLATE", "default-runtime")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__DEFAULT_CPUS", "many")
    executor = DockerSandboxExecutor(parallelism=4)

    with pytest.raises(
        DockerSandboxConfigurationError,
        match=r"\[docker_sandbox] default_cpus must be an integer",
    ):
        executor.build_launch_config(make_workload())


def test_driver_factory_captures_validated_immutable_config(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__WORKSPACE_ROOT", "/var/lib/airflow/sbx")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__SBX_BINARY", "/usr/local/bin/sbx")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__LAUNCH_ACCEPTANCE_TIMEOUT", "17.5")
    monkeypatch.setenv("AIRFLOW__DOCKER_SANDBOX__CLI_TIMEOUT_SECONDS", "43")
    executor = DockerSandboxExecutor(parallelism=4)

    factory = executor.get_driver_factory()

    assert isinstance(factory, partial)
    assert factory.func is DockerSandboxDriver
    assert factory.args == (
        DockerSandboxDriverConfig(
            scratch_root="/var/lib/airflow/sbx",
            sbx_binary="/usr/local/bin/sbx",
            acceptance_timeout_seconds=17.5,
            command_timeout_seconds=43.0,
        ),
    )
    assert factory.keywords == {}


def test_driver_factory_requires_workspace_root() -> None:
    executor = DockerSandboxExecutor(parallelism=4)

    with pytest.raises(
        DockerSandboxConfigurationError,
        match=r"\[docker_sandbox] workspace_root must be a non-empty string",
    ):
        executor.get_driver_factory()
