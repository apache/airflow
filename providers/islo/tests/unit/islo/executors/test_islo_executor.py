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

from pathlib import PurePosixPath
from uuid import uuid4

from airflow.executors.workloads import BundleInfo, ExecuteTask, TaskInstanceDTO
from airflow.providers.common.sandbox.executor import BaseSandboxExecutor
from airflow.providers.islo.executors.islo_executor import IsloExecutor
from airflow.providers.islo.models import IsloSandboxConfig


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


def test_islo_executor_is_thin_common_engine_binding(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__ISLO__DEFAULT_SNAPSHOT_NAME", "airflow-runtime")
    instance = IsloExecutor(parallelism=4)

    assert isinstance(instance, BaseSandboxExecutor)
    assert instance.driver_id == "islo"
    assert instance.pre_assigns_external_executor_id is True


def test_launch_config_separates_portable_and_islo_options(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__ISLO__DEFAULT_SNAPSHOT_NAME", "default-runtime")
    monkeypatch.setenv("AIRFLOW__ISLO__DEFAULT_GATEWAY_PROFILE", "airflow-restricted")
    monkeypatch.setenv("AIRFLOW__ISLO__INTERNET_ENABLED", "false")
    instance = IsloExecutor(parallelism=4)
    workload = make_workload(
        {
            "sandbox": {
                "env": {"MODEL": "small"},
                "keep": True,
                "timeout_seconds": 90,
                "ttl_seconds": 120,
                "workdir": "/workspace",
            },
            "islo": {
                "snapshot_name": "genomics-runtime",
                "vcpus": 8,
                "memory_mb": 32768,
            },
        }
    )

    launch = instance.build_launch_config(workload)
    provider = IsloSandboxConfig.from_json(launch.provider_config)

    assert launch.env == {"MODEL": "small"}
    assert launch.keep is True
    assert launch.timeout_seconds == 90
    assert launch.ttl_seconds == 120
    assert launch.workdir == "/workspace"
    assert provider.snapshot_name == "genomics-runtime"
    assert provider.vcpus == 8
    assert provider.memory_mb == 32768
    assert provider.gateway_profile == "airflow-restricted"
    assert provider.internet_enabled is False
