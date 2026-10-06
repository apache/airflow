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

import os
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from airflow_breeze.utils.docker_command_utils import VOLUMES_FOR_SELECTED_MOUNTS
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH


@pytest.mark.integration_tests
def test_reused_ci_image_imports_changed_checkout() -> None:
    """Verify editable installs see new PR code without rebuilding the image."""
    image = os.environ.get("AIRFLOW_REUSE_TEST_IMAGE")
    if not image:
        pytest.skip("Set AIRFLOW_REUSE_TEST_IMAGE to an existing CI image built from compatible dependencies")
    packages = {
        "airflow": "airflow-core/src/airflow/__init__.py",
        "airflow.sdk": "task-sdk/src/airflow/sdk/__init__.py",
        "airflow.providers.standard": "providers/standard/src/airflow/providers/standard/__init__.py",
        "airflow_shared.logging": "shared/logging/src/airflow_shared/logging/__init__.py",
    }
    # The Docker daemon must see the checkout's host path, including in a Breeze sandbox.
    with TemporaryDirectory(prefix="image-reuse-test-", dir=AIRFLOW_ROOT_PATH / "dev") as directory:
        checkout = Path(directory)
        for folder in ("airflow-core/src", "task-sdk/src", "providers", "shared"):
            shutil.copytree(
                AIRFLOW_ROOT_PATH / folder,
                checkout / folder,
                symlinks=True,
                ignore=shutil.ignore_patterns("tests", "docs", ".venv", "__pycache__", "node_modules"),
            )
        for relative_path in packages.values():
            source = checkout / relative_path
            source.write_text(source.read_text() + "\n__airflow_reuse_test_source__ = True\n")
        command = ["docker", "run", "--rm", "--pull=never", "--entrypoint", "python"]
        for source, target in VOLUMES_FOR_SELECTED_MOUNTS:
            if (checkout / source).is_dir():
                command.extend(["--mount", f"type=bind,source={checkout / source},target={target},readonly"])
        command.extend(
            [
                image,
                "-c",
                "import importlib; "
                f"modules = [importlib.import_module(name) for name in {list(packages)!r}]; "
                "assert all(getattr(module, '__airflow_reuse_test_source__', False) for module in modules), "
                "[(module.__name__, module.__file__) for module in modules]",
            ]
        )
        result = subprocess.run(command, text=True, capture_output=True, timeout=120, check=False)
        assert result.returncode == 0, result.stdout + result.stderr
