#
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

import contextlib
import json
import socket
import subprocess
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from airflow.sdk.execution_time.supervisor import ActivitySubprocess

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Iterator

    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
    from airflow.sdk.execution_time.comms import TaskHandlerArtifactRef
    from airflow.sdk.execution_time.coordinator import BaseCoordinator


@contextlib.contextmanager
def register_dag_bundle(name: str, path: pathlib.Path) -> Iterator[str]:
    """Register *path* as the Dag bundle *name* for the duration of the context, and yield the name."""
    bundle = {
        "name": name,
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {"path": str(path)},
    }
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps([bundle])}):
        yield name


def execute_task(
    coordinator: BaseCoordinator,
    client,
    *,
    what: TaskInstance,
    dag_rel_path: str,
    bundle_info: BundleInfo,
    task_handler_artifact: TaskHandlerArtifactRef | None = None,
) -> tuple[BaseCoordinator.ExecutionResult, list[list[str]]]:
    """Run ``execute_task`` with a mocked subprocess, and return its result and the commands it started."""
    mock_proc = MagicMock(spec=subprocess.Popen)
    mock_proc.pid = 12345
    comm_sock = MagicMock(spec=socket.socket)
    logs_sock = MagicMock(spec=socket.socket)
    popen_calls: list[list[str]] = []

    def capture_popen(cmd, **kwargs):
        popen_calls.append(cmd)
        return mock_proc

    with (
        patch(
            "airflow.sdk.coordinators._subprocess.subprocess.Popen", autospec=True, side_effect=capture_popen
        ),
        patch(
            "airflow.sdk.coordinators._subprocess._accept_connections",
            autospec=True,
            side_effect=lambda servers, drains, proc, **kw: (
                {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                {soc: b"" for soc in drains.values()},
            ),
        ),
        patch.object(ActivitySubprocess, "_register_pipe_readers", autospec=True),
        patch.object(ActivitySubprocess, "_on_child_started", autospec=True),
        patch.object(ActivitySubprocess, "wait", autospec=True, return_value=0),
        patch("psutil.Process", autospec=True),
    ):
        result = coordinator.execute_task(
            what=what,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            client=client,
            subprocess_logs_to_stdout=False,
            task_handler_artifact=task_handler_artifact,
        )
    return result, popen_calls
