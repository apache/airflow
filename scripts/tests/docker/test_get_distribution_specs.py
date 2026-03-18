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

import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "docker" / "get_distribution_specs.py"

CURRENT_PYTHON_MAJOR_MINOR = f"{sys.version_info.major}.{sys.version_info.minor}"


def _make_wheel(directory: Path, name: str, version: str, requires_python: str | None = None) -> Path:
    """Create a minimal .whl (zip) with METADATA."""
    safe_name = name.replace("-", "_")
    wheel_path = directory / f"{safe_name}-{version}-py3-none-any.whl"
    dist_info = f"{safe_name}-{version}.dist-info"
    metadata_lines = [
        "Metadata-Version: 2.1",
        f"Name: {name}",
        f"Version: {version}",
    ]
    if requires_python is not None:
        metadata_lines.append(f"Requires-Python: {requires_python}")
    with zipfile.ZipFile(wheel_path, "w") as zf:
        zf.writestr(f"{dist_info}/METADATA", "\n".join(metadata_lines))
        zf.writestr(f"{dist_info}/RECORD", "")
    return wheel_path


def _run_script(*wheel_paths: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), *(str(p) for p in wheel_paths)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


class TestRequiresPythonFiltering:
    def test_wheel_without_requires_python_is_included(self, tmp_path):
        whl = _make_wheel(tmp_path, "some-package", "1.0.0")
        result = _run_script(whl)
        assert result.returncode == 0
        assert f"some-package @ file://{whl}" in result.stdout

    def test_wheel_matching_current_python_is_included(self, tmp_path):
        whl = _make_wheel(tmp_path, "some-package", "1.0.0", requires_python=">=3.10")
        result = _run_script(whl)
        assert result.returncode == 0
        assert f"some-package @ file://{whl}" in result.stdout

    def test_wheel_excluding_current_python_is_skipped(self, tmp_path):
        whl = _make_wheel(
            tmp_path,
            "excluded-package",
            "2.0.0",
            requires_python=f">=3.10,!={CURRENT_PYTHON_MAJOR_MINOR}.*",
        )
        result = _run_script(whl)
        assert result.returncode == 0
        assert result.stdout == ""
        assert "Skipping" in result.stderr
        assert "excluded-package" in result.stderr

    def test_corrupt_wheel_is_included_with_warning(self, tmp_path):
        bad_whl = tmp_path / "bad_package-1.0.0-py3-none-any.whl"
        bad_whl.write_bytes(b"not a zip")
        result = _run_script(bad_whl)
        assert result.returncode == 0
        assert f"bad-package @ file://{bad_whl}" in result.stdout
        assert "Warning" in result.stderr


class TestMixedInputs:
    def test_compatible_and_incompatible_together(self, tmp_path):
        good_whl = _make_wheel(
            tmp_path, "apache-airflow-providers-standard", "1.0.0", requires_python=">=3.10"
        )
        bad_whl = _make_wheel(
            tmp_path,
            "apache-airflow-providers-google",
            "21.0.0",
            requires_python=f">=3.10,!={CURRENT_PYTHON_MAJOR_MINOR}.*",
        )
        result = _run_script(good_whl, bad_whl)
        assert result.returncode == 0
        assert "apache-airflow-providers-standard" in result.stdout
        assert "apache-airflow-providers-google" not in result.stdout
        assert "Skipping" in result.stderr

    def test_sdist_is_not_filtered(self, tmp_path):
        """Sdists cannot be inspected for Requires-Python without building, so they pass through."""
        import tarfile

        sdist = tmp_path / "apache_airflow_providers_google-21.0.0.tar.gz"
        with tarfile.open(sdist, "w:gz"):
            pass
        result = _run_script(sdist)
        assert result.returncode == 0
        assert "apache-airflow-providers-google" in result.stdout


class TestExtrasEnvVar:
    def test_extras_appended_to_spec(self, tmp_path):
        import os

        whl = _make_wheel(tmp_path, "apache-airflow", "3.0.0", requires_python=">=3.10")
        env = {**os.environ, "EXTRAS": "[celery,google]"}
        result = _run_script(whl, env=env)
        assert result.returncode == 0
        assert f"apache-airflow[celery,google] @ file://{whl}" in result.stdout


@pytest.mark.parametrize(
    ("requires_python", "should_include"),
    [
        pytest.param(">=3.10", True, id="lower-bound-satisfied"),
        pytest.param(f"!={CURRENT_PYTHON_MAJOR_MINOR}.*", False, id="wildcard-minor-excluded"),
        pytest.param(f">=3.10,!={CURRENT_PYTHON_MAJOR_MINOR}.*", False, id="range-with-exclusion"),
        pytest.param("<3.10", False, id="upper-bound-below-current"),
        pytest.param(None, True, id="no-requires-python"),
    ],
)
def test_requires_python_specifiers(tmp_path, requires_python, should_include):
    whl = _make_wheel(tmp_path, "test-package", "1.0.0", requires_python=requires_python)
    result = _run_script(whl)
    assert result.returncode == 0
    if should_include:
        assert f"test-package @ file://{whl}" in result.stdout
    else:
        assert result.stdout == ""
        assert "Skipping" in result.stderr
