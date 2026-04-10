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

import email
import zipfile
from pathlib import Path

from airflow.sdk.execution_time.coordinator import BaseLocaleCoordinator


def find_main_class(jar_path: Path) -> str:
    """Read the Main-Class attribute from the JAR manifest."""
    with zipfile.ZipFile(jar_path) as zf:
        with zf.open("META-INF/MANIFEST.MF") as f:
            if main_class := email.message_from_binary_file(f).get("Main-Class"):
                return main_class
    raise FileNotFoundError(f"No Main-Class in manifest of {jar_path}")


class JavaLocaleCoordinator(BaseLocaleCoordinator):
    """Coordinator that launches a JVM subprocess for DAG parsing."""

    locale_name = "java"

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
            find_main_class(jar_path),
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
