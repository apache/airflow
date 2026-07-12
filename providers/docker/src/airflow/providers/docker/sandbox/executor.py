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
"""Bind the common sandbox executor engine to Docker Sandboxes."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.providers.common.sandbox.executor import BaseSandboxExecutor
    from airflow.providers.common.sandbox.models import SandboxLaunchConfig, coerce_sandbox_executor_config
except ModuleNotFoundError as error:
    if error.name is None or not (
        error.name == "airflow.providers.common.sandbox"
        or error.name.startswith("airflow.providers.common.sandbox.")
    ):
        raise
    raise AirflowOptionalProviderFeatureException(
        "DockerSandboxExecutor requires apache-airflow-providers-docker[common.sandbox]"
    ) from error
from airflow.providers.docker.sandbox.driver import DockerSandboxDriver
from airflow.providers.docker.sandbox.exceptions import DockerSandboxConfigurationError
from airflow.providers.docker.sandbox.models import (
    DockerSandboxDriverConfig,
    DockerSandboxLaunchConfig,
)

if TYPE_CHECKING:
    from airflow.executors.workloads import ExecuteTask
    from airflow.providers.common.sandbox.driver import SandboxDriverFactory


class DockerSandboxExecutor(BaseSandboxExecutor):
    """Run task attempts in local Docker Sandboxes for development and end-to-end tests."""

    driver_id = "docker-sandbox"
    is_production = False
    requires_terminal_cleanup = True

    _PROVIDER_CONFIG_SECTION = "docker_sandbox"

    def start(self) -> None:
        if not self.conf.getboolean(
            self._PROVIDER_CONFIG_SECTION,
            "allow_non_production",
            fallback=False,
        ):
            raise DockerSandboxConfigurationError(
                "DockerSandboxExecutor is for development and end-to-end testing only; "
                "set [docker_sandbox] allow_non_production=True to enable it explicitly"
            )
        super().start()

    def get_driver_factory(self) -> SandboxDriverFactory:
        section = self._PROVIDER_CONFIG_SECTION
        config = DockerSandboxDriverConfig(
            scratch_root=self._required_option("workspace_root"),
            sbx_binary=str(self.conf.get(section, "sbx_binary", fallback="sbx")),
            acceptance_timeout_seconds=self._positive_float(
                "launch_acceptance_timeout",
                fallback=30.0,
            ),
            command_timeout_seconds=self._positive_float(
                "cli_timeout_seconds",
                fallback=60.0,
            ),
        )
        return partial(DockerSandboxDriver, config)

    def build_launch_config(self, workload: ExecuteTask) -> SandboxLaunchConfig:
        executor_config = workload.ti.executor_config
        sandbox_override = coerce_sandbox_executor_config(executor_config)
        if sandbox_override.get("keep", False):
            raise DockerSandboxConfigurationError(
                "DockerSandboxExecutor cannot keep sandboxes because Docker Sandboxes has no hard TTL"
            )
        docker_override = self._coerce_provider_config(executor_config)
        provider = DockerSandboxLaunchConfig(
            template=docker_override.get("template", self._required_option("default_template")),
            cpus=docker_override.get("cpus", self._optional_int("default_cpus")),
            memory=docker_override.get("memory", self._optional_str("default_memory")),
        )
        timeout_seconds = sandbox_override.get(
            "timeout_seconds",
            self.conf.getint(
                self._PROVIDER_CONFIG_SECTION,
                "default_timeout_seconds",
                fallback=3600,
            ),
        )
        return SandboxLaunchConfig(
            provider_config={
                "template": provider.template,
                "cpus": provider.cpus,
                "memory": provider.memory,
            },
            env=sandbox_override.get("env", {}),
            workdir=sandbox_override.get("workdir", self._optional_str("default_workdir")),
            timeout_seconds=timeout_seconds,
            ttl_seconds=sandbox_override.get("ttl_seconds", timeout_seconds),
            keep=False,
        )

    def _required_option(self, option: str) -> str:
        value = self._optional_str(option)
        if value is None:
            raise DockerSandboxConfigurationError(f"[docker_sandbox] {option} must be a non-empty string")
        return value

    def _optional_int(self, option: str) -> int | None:
        value = self.conf.get(self._PROVIDER_CONFIG_SECTION, option, fallback=None)
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError) as error:
            raise DockerSandboxConfigurationError(f"[docker_sandbox] {option} must be an integer") from error

    def _optional_str(self, option: str) -> str | None:
        value = self.conf.get(self._PROVIDER_CONFIG_SECTION, option, fallback=None)
        if value in (None, ""):
            return None
        return str(value)

    def _positive_float(self, option: str, *, fallback: float) -> float:
        value = self.conf.get(self._PROVIDER_CONFIG_SECTION, option, fallback=str(fallback))
        try:
            parsed = float(value) if value is not None else fallback
        except (TypeError, ValueError) as error:
            raise DockerSandboxConfigurationError(
                f"[docker_sandbox] {option} must be a positive number"
            ) from error
        if parsed <= 0:
            raise DockerSandboxConfigurationError(f"[docker_sandbox] {option} must be a positive number")
        return parsed

    @staticmethod
    def _coerce_provider_config(value: dict[str, Any] | None) -> dict[str, Any]:
        if value is None:
            return {}
        override = value.get("docker_sandbox", {})
        if not isinstance(override, dict):
            raise DockerSandboxConfigurationError("executor_config['docker_sandbox'] must be a mapping")
        allowed = {"cpus", "memory", "template"}
        if unknown := sorted(set(override) - allowed):
            raise DockerSandboxConfigurationError(
                "unsupported Docker Sandbox executor_config keys: " + ", ".join(unknown)
            )
        return dict(override)
