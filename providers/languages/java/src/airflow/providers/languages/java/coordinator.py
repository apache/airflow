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
"""Java locale coordinator that launches a JVM subprocess for Dag file processing and task execution."""

from __future__ import annotations

import contextlib
import os
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.providers.languages.java.bundle_scanner import BundleScanner
from airflow.sdk.execution_time.coordinator import BaseLocaleCoordinator

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance


class JavaLocaleCoordinator(BaseLocaleCoordinator):
    """Coordinator that launches a JVM subprocess for DAG parsing and task execution."""

    locale_name = "java"

    @classmethod
    def can_handle_dag_file(cls, bundle_name: str, path: str | os.PathLike[str]) -> bool:
        """Return ``True`` when *path* is a JAR with valid Airflow Java SDK manifest attributes."""
        with contextlib.suppress(FileNotFoundError, zipfile.BadZipFile, KeyError):
            return BundleScanner.resolve_jar(Path(path)) is not None
        return False

    @classmethod
    def dag_parsing_locale_cmd(
        cls,
        *,
        dag_file_path: str,
        bundle_name: str,
        bundle_path: str,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """Build the ``java`` command for parsing a JAR bundle."""
        jar_path = Path(dag_file_path)
        # Java bundles are typically thin JARs: the main JAR only contains
        # the bundle's own classes while its dependencies (the Airflow Java
        # SDK, logging libraries, etc.) are separate JARs that live alongside
        # it.  Using ``<dir>/*`` lets the JVM load every JAR in the directory.
        classpath = f"{bundle_path}/*"
        return [
            "java",
            "-classpath",
            classpath,
            BundleScanner.resolve_jar(jar_path),
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]

    @classmethod
    def task_execution_locale_cmd(
        cls,
        *,
        what: TaskInstance,
        dag_file_path: str,
        bundle_path: str,
        bundle_info: BundleInfo,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """Build the ``java`` command for executing a task in a JAR bundle."""
        if what.language is None:
            # Case 1: Pure Java Dag — the dag_file_path points directly to a
            # bundle JAR inside the Airflow Core Dag Bundle.
            jar_path = Path(dag_file_path)
            classpath = f"{bundle_path}/*"
            return [
                "java",
                "-classpath",
                classpath,
                BundleScanner.resolve_jar(jar_path),
                f"--comm={comm_addr}",
                f"--logs={logs_addr}",
            ]

        # Case 2: Python Stub Dag — the task's ``language`` field is set
        # (e.g. "java").  The actual JAR bundle lives in the provider's
        # configured ``[java] bundles_folder``, not in the Dag bundle path.
        from airflow.providers.common.compat.sdk import conf

        bundles_folder = conf.get("java", "bundles_folder", fallback=None)
        if not bundles_folder:
            raise ValueError(
                "The [java] bundles_folder config must be set for Python stub DAGs "
                "that delegate to Java task execution."
            )

        resolved = BundleScanner(Path(bundles_folder)).resolve(dag_id=what.dag_id)
        return [
            "java",
            "-classpath",
            resolved.classpath,
            resolved.main_class,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
