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
"""Native executable runtime coordinator for DAG file processing and task execution."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.providers.sdk.executable.bundle_scanner import BundleScanner, read_source_code
from airflow.sdk.execution_time.coordinator import BaseRuntimeCoordinator

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance


class ExecutableRuntimeCoordinator(BaseRuntimeCoordinator):
    """Coordinator that launches a native executable subprocess for DAG parsing and task execution."""

    runtime_name = "executable"
    file_extension = ""

    @classmethod
    def can_handle_dag_file(cls, bundle_name: str, path: str | os.PathLike[str]) -> bool:
        """Return ``True`` when *path* is a native executable with Airflow SDK metadata."""
        with contextlib.suppress(FileNotFoundError, KeyError):
            return BundleScanner.resolve_executable(Path(path)) is not None
        return False

    @classmethod
    def get_code_from_file(cls, fileloc: str) -> str:
        """Read source code from a sidecar file alongside the executable."""
        code = read_source_code(Path(fileloc))
        if code is None:
            raise FileNotFoundError(f"No source code found for executable: {fileloc}")
        return code

    @classmethod
    def dag_parsing_runtime_cmd(
        cls,
        *,
        dag_file_path: str,
        bundle_name: str,
        bundle_path: str,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """Build the subprocess command for parsing a native executable bundle."""
        return [
            dag_file_path,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]

    @classmethod
    def task_execution_runtime_cmd(
        cls,
        *,
        what: TaskInstance,
        dag_file_path: str,
        bundle_path: str,
        bundle_info: BundleInfo,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """Build the subprocess command for executing a task in a native executable bundle."""
        if os.access(dag_file_path, os.X_OK):
            # Case 1: Pure executable DAG — the dag_file_path points directly
            # to the bundle binary.
            return [
                dag_file_path,
                f"--comm={comm_addr}",
                f"--logs={logs_addr}",
            ]

        # Case 2: Python Stub DAG — the dag_file_path is a Python file but
        # the task delegates to a native runtime.  The actual binary lives
        # in the provider's configured ``[executable] bundles_folder``.
        from airflow.providers.common.compat.sdk import conf

        bundles_folder = conf.get("executable", "bundles_folder", fallback=None)
        if not bundles_folder:
            raise ValueError(
                "The [executable] bundles_folder config must be set for Python stub DAGs "
                "that delegate to native executable task execution."
            )

        resolved = BundleScanner(Path(bundles_folder)).resolve(dag_id=what.dag_id)
        return [
            resolved.executable_path,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
