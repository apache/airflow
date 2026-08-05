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
"""Bind the common sandbox executor engine to the Islo sandbox service."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from airflow.providers.common.sandbox.executor import BaseSandboxExecutor
from airflow.providers.common.sandbox.models import (
    SandboxLaunchConfig,
    coerce_sandbox_executor_config,
)
from airflow.providers.islo.drivers.islo import IsloSandboxDriver
from airflow.providers.islo.hooks.islo import IsloHook
from airflow.providers.islo.models import IsloSandboxConfig, coerce_islo_executor_config

if TYPE_CHECKING:
    from airflow.executors.workloads import ExecuteTask
    from airflow.providers.common.sandbox.driver import SandboxDriverFactory


class IsloExecutor(BaseSandboxExecutor):
    """Run every task attempt in an isolated, ephemeral Islo sandbox."""

    driver_id = "islo"

    def get_driver_factory(self) -> SandboxDriverFactory:
        conn_id = self.conf.get("islo", "conn_id", fallback=IsloHook.default_conn_name)
        client_config = IsloHook(str(conn_id)).get_client_config()
        return partial(IsloSandboxDriver, client_config)

    def _optional_int(self, option: str) -> int | None:
        value = self.conf.get("islo", option, fallback=None)
        if value is None or value == "":
            return None
        return int(value)

    def _optional_str(self, option: str) -> str | None:
        value = self.conf.get("islo", option, fallback=None)
        return str(value) if value not in (None, "") else None

    def build_launch_config(self, workload: ExecuteTask) -> SandboxLaunchConfig:
        executor_config = workload.ti.executor_config
        sandbox_override = coerce_sandbox_executor_config(executor_config)
        islo_override = coerce_islo_executor_config(executor_config)
        defaults = {
            "image": self._optional_str("default_image"),
            "snapshot_name": self._optional_str("default_snapshot_name"),
        }
        if any(key in islo_override for key in defaults):
            sources = {key: islo_override.get(key) for key in defaults}
        else:
            sources = defaults
        provider_config = IsloSandboxConfig(
            **sources,
            vcpus=islo_override.get("vcpus", self._optional_int("default_vcpus")),
            memory_mb=islo_override.get("memory_mb", self._optional_int("default_memory_mb")),
            disk_gb=islo_override.get("disk_gb", self._optional_int("default_disk_gb")),
            gateway_profile=self._optional_str("default_gateway_profile"),
            internet_enabled=self.conf.getboolean("islo", "internet_enabled", fallback=True),
        )
        return SandboxLaunchConfig(
            provider_config=provider_config.to_json(),
            env=sandbox_override.get("env", {}),
            workdir=sandbox_override.get("workdir", self._optional_str("default_workdir")),
            timeout_seconds=sandbox_override.get(
                "timeout_seconds",
                self.conf.getint("islo", "default_timeout_seconds", fallback=3600),
            ),
            ttl_seconds=sandbox_override.get(
                "ttl_seconds",
                self.conf.getint("islo", "default_ttl_seconds", fallback=86400),
            ),
            keep=sandbox_override.get("keep", False),
        )
