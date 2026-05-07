#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check cross-language DAG serialization compatibility between the Python and Java SDKs.

Workflow:
  1. Run the Java SDK ``SerializationCompatibilityTest`` via Gradle to produce
     ``serialized_java.json``.
  2. Run ``serialize_python.py`` via ``uv`` to produce ``serialized_python.json``.
  3. Run ``compare.py`` to deep-diff the two outputs.

The check fails if any DAG serialises differently across the two languages.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, console

JAVA_SDK_PATH = AIRFLOW_ROOT_PATH / "java-sdk"
VALIDATION_DIR = JAVA_SDK_PATH / "validation" / "serialization"
TEST_DAGS_YAML = VALIDATION_DIR / "test_dags.yaml"
COMPARE_SCRIPT = VALIDATION_DIR / "compare.py"
SERIALIZE_PYTHON_SCRIPT = VALIDATION_DIR / "serialize_python.py"
SERIALIZED_JAVA_JSON = VALIDATION_DIR / "serialized_java.json"


def _run_java_serialization() -> bool:
    """Run the Java SDK serialization test to generate ``serialized_java.json``."""
    gradlew = JAVA_SDK_PATH / "gradlew"
    if not gradlew.exists():
        console.print(f"[red]Gradle wrapper not found at {gradlew}[/]")
        return False

    if not shutil.which("java"):
        console.print(
            "[red]'java' not found on PATH. Install a JDK (17+) or set JAVA_HOME to run this check.[/]"
        )
        return False

    console.print("[yellow]Step 1/3: Running Java SDK serialization test...[/]")
    result = subprocess.run(
        [
            str(gradlew),
            "sdk:cleanTest",
            "sdk:test",
            "--tests",
            "org.apache.airflow.sdk.execution.SerializationCompatibilityTest",
            "-x",
            "spotlessApply",
            "-x",
            "spotlessCheck",
        ],
        cwd=str(JAVA_SDK_PATH),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0:
        console.print("[red]Java SDK serialization test failed:[/]")
        if result.stdout:
            console.print(result.stdout)
        if result.stderr:
            console.print(result.stderr)
        return False

    if not SERIALIZED_JAVA_JSON.exists():
        console.print(f"[red]Expected Java output not found: {SERIALIZED_JAVA_JSON}[/]")
        return False

    return True


def _run_python_serialization(output_path: Path) -> bool:
    """Run the Python serialization script to generate ``serialized_python.json``."""
    console.print("[yellow]Step 2/3: Running Python SDK serialization...[/]")
    # TODO: Perhaps we need to run this in Breeze container instead of relying on the host environment
    result = subprocess.run(
        [
            "uv",
            "run",
            "--project",
            str(AIRFLOW_ROOT_PATH / "airflow-core"),
            "python",
            str(SERIALIZE_PYTHON_SCRIPT),
            str(TEST_DAGS_YAML),
            str(output_path),
        ],
        cwd=str(AIRFLOW_ROOT_PATH),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        console.print("[red]Python SDK serialization failed:[/]")
        if result.stdout:
            console.print(result.stdout)
        if result.stderr:
            console.print(result.stderr)
        return False
    return True


def _run_comparison(python_json: Path, java_json: Path) -> bool:
    """Run the comparison script to diff the two serialized outputs."""
    console.print("[yellow]Step 3/3: Comparing serialized outputs...[/]")
    result = subprocess.run(
        ["python", str(COMPARE_SCRIPT), str(python_json), str(java_json)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.stdout:
        console.print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            console.print(result.stderr)
        return False
    return True


def main() -> int:
    if os.environ.get("SKIP_JAVA_SDK_PREK_HOOKS"):
        console.print("[yellow]Skipping Java SDK prek hooks (SKIP_JAVA_SDK_PREK_HOOKS is set)[/]")
        return 0

    if not TEST_DAGS_YAML.exists():
        console.print(f"[red]test_dags.yaml not found: {TEST_DAGS_YAML}[/]")
        return 1

    java_json_existed = SERIALIZED_JAVA_JSON.exists()

    try:
        if not _run_java_serialization():
            return 1

        with tempfile.TemporaryDirectory(prefix="serialization-check-") as tmp:
            python_json = Path(tmp) / "serialized_python.json"

            if not _run_python_serialization(python_json):
                return 1

            if not _run_comparison(python_json, SERIALIZED_JAVA_JSON):
                console.print("[red]Serialization compatibility check FAILED[/]")
                return 1

        console.print("[green]Serialization compatibility check PASSED[/]")
        return 0
    finally:
        # Clean up the Java JSON if it was generated by this run
        if not java_json_existed and SERIALIZED_JAVA_JSON.exists():
            SERIALIZED_JAVA_JSON.unlink()


if __name__ == "__main__":
    sys.exit(main())
