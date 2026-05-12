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
"""Native executable coordinator for DAG file processing and task execution."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from structlog import get_logger

from airflow.providers.sdk.executable.bundle_scanner import BundleScanner, read_source_code
from airflow.sdk.execution_time.coordinator import BaseCoordinator

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance

log = get_logger(__name__)


class ExecutableCoordinator(BaseCoordinator):
    """Coordinator that launches a native executable subprocess for DAG parsing and task execution."""

    sdk = "executable"
    file_extension = ""

    @classmethod
    def can_handle_dag_file(cls, bundle_name: str, path: str | os.PathLike[str]) -> bool:
        """
        Return ``True`` when *path* is a self-contained executable bundle.

        Detection is by the ``AFBNDL01`` trailer magic appended by the SDK
        packer; non-bundle files are silently rejected.
        """
        try:
            return BundleScanner.resolve_executable(Path(path)) is not None
        except OSError:
            return False

    @classmethod
    def get_code_from_file(cls, fileloc: str) -> str:
        """Read the DAG source embedded in the bundle's footer."""
        code = read_source_code(Path(fileloc))
        if code is None:
            raise FileNotFoundError(f"No source code found for executable: {fileloc}")
        return code

    @classmethod
    def dag_parsing_cmd(
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
    def task_execution_cmd(
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
        # Case 1: Pure executable DAG — dag_file_path is itself a bundle.
        # A ``.py`` extension always indicates a Python stub, so short-circuit
        # there before validating the AFBNDL01 trailer. Both checks are needed:
        # ``os.access(_, X_OK)`` alone is not reliable because bind-mounted
        # Python stubs can satisfy X_OK (e.g. when running as root inside
        # Breeze) yet are not real bundles.
        log.debug(
            "Resolving executable for task execution",
            what=what,
            dag_file_path=dag_file_path,
            bundle_path=bundle_path,
            bundle_info=bundle_info,
            comm_addr=comm_addr,
            logs_addr=logs_addr,
        )
        if (
            not dag_file_path.endswith(".py")
            and (resolved := BundleScanner.resolve_executable(Path(dag_file_path))) is not None
        ):
            return [
                resolved,
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

        executable_path = BundleScanner(Path(bundles_folder)).resolve(dag_id=what.dag_id)
        return [
            executable_path,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
