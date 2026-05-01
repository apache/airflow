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

import io
import json
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import install_development_dependencies as install_dev_deps
import pytest
from install_airflow_and_providers import (
    _get_build_constraints_flags,
    resolve_build_constraints_file,
)
from run_generate_constraints import (
    ConfigParams,
    _collect_upstream_build_reqs,
    _collect_workspace_build_reqs,
    _extract_build_reqs_from_tar,
    _extract_build_reqs_from_zip,
    _extract_package_name,
    _is_exact_pin,
    _normalize_package_name,
    _parse_uv_lock,
    _resolve_build_requirements,
)

# ---------------------------------------------------------------------------
# _normalize_package_name / _extract_package_name / _is_exact_pin
# ---------------------------------------------------------------------------


class TestNormalizePackageName:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("setuptools", "setuptools"),
            ("Cython", "cython"),
            ("my_package", "my-package"),
            ("my.package", "my-package"),
            ("My--Package", "my-package"),
        ],
    )
    def test_normalization(self, raw, expected):
        assert _normalize_package_name(raw) == expected


class TestExtractPackageName:
    @pytest.mark.parametrize(
        ("req", "expected"),
        [
            ("setuptools", "setuptools"),
            ("maturin>=1.9.4,<2", "maturin"),
            ("hatchling==1.29.0", "hatchling"),
            ("Cython>=3.0", "cython"),
            ("GitPython>=3.1.30", "gitpython"),
        ],
    )
    def test_extract(self, req, expected):
        assert _extract_package_name(req) == expected


class TestIsExactPin:
    @pytest.mark.parametrize(
        ("req", "expected"),
        [
            ("setuptools==80.9.0", True),
            ("hatchling==1.29.0", True),
            ("Cython == 3.1.1", True),
            ("cython==3.1.1", True),
            ("tomli==2.4.1; python_version < '3.11'", True),
            ("setuptools>=70.0", False),
            ("maturin>=1.9.4,<2", False),
            ("setuptools!=74.0.0", False),
            ("setuptools>=77.0.0", False),
            ("cython>=3.1.2,<3.3.0", False),
            ("cffi~=1.17", False),
            ("setuptools", False),
            ("wheel", False),
        ],
    )
    def test_is_exact_pin(self, req, expected):
        assert _is_exact_pin(req) == expected


# ---------------------------------------------------------------------------
# _collect_workspace_build_reqs
# ---------------------------------------------------------------------------


class TestCollectWorkspaceBuildReqs:
    def test_collects_from_multiple_pyproject_files(self, tmp_path):
        """Multiple workspace packages with different build deps are all collected."""
        (tmp_path / "pkg_a").mkdir()
        (tmp_path / "pkg_a" / "pyproject.toml").write_text(
            '[build-system]\nrequires = ["hatchling==1.29.0"]\n'
        )
        (tmp_path / "pkg_b").mkdir()
        (tmp_path / "pkg_b" / "pyproject.toml").write_text(
            '[build-system]\nrequires = ["setuptools>=70.0"]\n'
        )

        result = _collect_workspace_build_reqs(tmp_path)
        assert "hatchling" in result
        assert "setuptools" in result
        assert result["hatchling"] == {"hatchling==1.29.0"}
        assert result["setuptools"] == {"setuptools>=70.0"}

    def test_preserves_multiple_specifiers_for_same_package(self, tmp_path):
        """When two packages require the same build dep with different specifiers,
        both requirement strings are preserved."""
        (tmp_path / "pkg_a").mkdir()
        (tmp_path / "pkg_a" / "pyproject.toml").write_text(
            '[build-system]\nrequires = ["hatchling==1.29.0"]\n'
        )
        (tmp_path / "pkg_b").mkdir()
        (tmp_path / "pkg_b" / "pyproject.toml").write_text('[build-system]\nrequires = ["hatchling>=1.20"]\n')

        result = _collect_workspace_build_reqs(tmp_path)
        assert result["hatchling"] == {"hatchling==1.29.0", "hatchling>=1.20"}

    def test_skips_excluded_dirs(self, tmp_path):
        """Directories in the skip list are not scanned."""
        venv = tmp_path / ".venv" / "lib"
        venv.mkdir(parents=True)
        (venv / "pyproject.toml").write_text('[build-system]\nrequires = ["bad-pkg"]\n')

        result = _collect_workspace_build_reqs(tmp_path)
        assert "bad-pkg" not in result

    def test_skips_transient_build_and_artifact_dirs(self, tmp_path):
        """Transient directories must not affect authoritative build constraints."""
        (tmp_path / "pkg").mkdir()
        (tmp_path / "pkg" / "pyproject.toml").write_text('[build-system]\nrequires = ["hatchling==1.29.0"]\n')
        for transient_dir in [".build/airflow_source", "dist/extracted", "files/constraints-3.12"]:
            path = tmp_path / transient_dir
            path.mkdir(parents=True)
            (path / "pyproject.toml").write_text(
                f'[build-system]\nrequires = ["polluted-{path.parts[-1]}"]\n'
            )

        result = _collect_workspace_build_reqs(tmp_path)

        assert result == {"hatchling": {"hatchling==1.29.0"}}

    def test_logs_warning_on_parse_error(self, tmp_path):
        """A malformed pyproject.toml logs a warning but doesn't crash."""
        (tmp_path / "pyproject.toml").write_text("not valid toml {{{")

        with patch("run_generate_constraints.console") as mock_console:
            result = _collect_workspace_build_reqs(tmp_path)

        assert result == {}
        mock_console.print.assert_called()
        warning_msg = mock_console.print.call_args[0][0]
        assert "Warning" in warning_msg


# ---------------------------------------------------------------------------
# _parse_uv_lock
# ---------------------------------------------------------------------------


class TestParseUvLock:
    def test_parses_packages_with_sdist_and_wheels(self, tmp_path):
        lock_content = """
[[package]]
name = "pydantic-core"
version = "2.33.2"

[package.sdist]
url = "https://files.pythonhosted.org/pydantic_core-2.33.2.tar.gz"

[[package.wheels]]
url = "https://files.pythonhosted.org/pydantic_core-2.33.2-cp312-linux_x86_64.whl"

[[package]]
name = "requests"
version = "2.31.0"

[[package.wheels]]
url = "https://files.pythonhosted.org/requests-2.31.0-py3-none-any.whl"
"""
        lock_file = tmp_path / "uv.lock"
        lock_file.write_text(lock_content)

        packages = _parse_uv_lock(lock_file)
        assert len(packages) == 2

        pydantic = next(p for p in packages if p.name == "pydantic-core")
        assert pydantic.version == "2.33.2"
        assert pydantic.sdist_url == "https://files.pythonhosted.org/pydantic_core-2.33.2.tar.gz"
        assert not pydantic.has_universal_wheel

        requests_pkg = next(p for p in packages if p.name == "requests")
        assert requests_pkg.has_universal_wheel
        assert requests_pkg.sdist_url is None


# ---------------------------------------------------------------------------
# _extract_build_reqs_from_tar / _extract_build_reqs_from_zip
# ---------------------------------------------------------------------------


def _make_tar_sdist(tmp_path: Path, reqs: list[str] | None) -> Path:
    """Create a .tar.gz sdist with optional pyproject.toml containing build reqs."""
    tar_path = tmp_path / "pkg-1.0.0.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        if reqs is not None:
            content = (
                f'[build-system]\nrequires = {json.dumps(reqs)}\nbuild-backend = "setuptools.build_meta"\n'
            )
            data = content.encode()
            info = tarfile.TarInfo(name="pkg-1.0.0/pyproject.toml")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        else:
            # Legacy sdist without pyproject.toml
            info = tarfile.TarInfo(name="pkg-1.0.0/setup.py")
            setup_data = b"from setuptools import setup\nsetup()"
            info.size = len(setup_data)
            tar.addfile(info, io.BytesIO(setup_data))
    return tar_path


def _make_zip_sdist(tmp_path: Path, reqs: list[str] | None) -> Path:
    """Create a .zip sdist with optional pyproject.toml containing build reqs."""
    zip_path = tmp_path / "pkg-1.0.0.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        if reqs is not None:
            content = (
                f'[build-system]\nrequires = {json.dumps(reqs)}\nbuild-backend = "setuptools.build_meta"\n'
            )
            zf.writestr("pkg-1.0.0/pyproject.toml", content)
        else:
            zf.writestr("pkg-1.0.0/setup.py", "from setuptools import setup\nsetup()")
    return zip_path


def _pyproject_toml(reqs: list[str]) -> bytes:
    return (
        f'[build-system]\nrequires = {json.dumps(reqs)}\nbuild-backend = "setuptools.build_meta"\n'
    ).encode()


class TestExtractBuildReqsFromTar:
    def test_extracts_requirements(self, tmp_path):
        reqs = ["setuptools>=70.0", "wheel"]
        tar_path = _make_tar_sdist(tmp_path, reqs)
        result = _extract_build_reqs_from_tar(tar_path.as_uri())
        assert result == reqs

    def test_uses_top_level_pyproject_not_first_nested_pyproject(self, tmp_path):
        """Vendored or fixture pyproject.toml files must not drive build constraints."""
        tar_path = tmp_path / "pkg-1.0.0.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            nested_data = _pyproject_toml(["wrong-backend>=1"])
            nested_info = tarfile.TarInfo(name="pkg-1.0.0/vendor/dep/pyproject.toml")
            nested_info.size = len(nested_data)
            tar.addfile(nested_info, io.BytesIO(nested_data))

            root_data = _pyproject_toml(["right-backend>=2"])
            root_info = tarfile.TarInfo(name="pkg-1.0.0/pyproject.toml")
            root_info.size = len(root_data)
            tar.addfile(root_info, io.BytesIO(root_data))

        result = _extract_build_reqs_from_tar(tar_path.as_uri())

        assert result == ["right-backend>=2"]

    def test_returns_empty_for_legacy_sdist(self, tmp_path):
        tar_path = _make_tar_sdist(tmp_path, None)
        result = _extract_build_reqs_from_tar(tar_path.as_uri())
        assert result == []


class TestExtractBuildReqsFromZip:
    def test_extracts_requirements(self, tmp_path):
        reqs = ["maturin>=1.9.4,<2"]
        zip_path = _make_zip_sdist(tmp_path, reqs)
        result = _extract_build_reqs_from_zip(zip_path.as_uri())
        assert result == reqs

    def test_uses_top_level_pyproject_not_first_nested_pyproject(self, tmp_path):
        """Vendored or fixture pyproject.toml files must not drive build constraints."""
        zip_path = tmp_path / "pkg-1.0.0.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("pkg-1.0.0/tests/fixtures/pyproject.toml", _pyproject_toml(["wrong-backend>=1"]))
            zf.writestr("pkg-1.0.0/pyproject.toml", _pyproject_toml(["right-backend>=2"]))

        result = _extract_build_reqs_from_zip(zip_path.as_uri())

        assert result == ["right-backend>=2"]

    def test_returns_empty_for_legacy_sdist(self, tmp_path):
        zip_path = _make_zip_sdist(tmp_path, None)
        result = _extract_build_reqs_from_zip(zip_path.as_uri())
        assert result == []


# ---------------------------------------------------------------------------
# _collect_upstream_build_reqs
# ---------------------------------------------------------------------------


def _make_uv_lock(tmp_path: Path, packages: list[dict]) -> Path:
    """Create a minimal uv.lock TOML file from package dicts."""
    lines = []
    for pkg in packages:
        lines.append("[[package]]")
        lines.append(f'name = "{pkg["name"]}"')
        lines.append(f'version = "{pkg["version"]}"')
        if "sdist_url" in pkg:
            lines.append("")
            lines.append("[package.sdist]")
            lines.append(f'url = "{pkg["sdist_url"]}"')
        if pkg.get("universal_wheel"):
            lines.append("")
            lines.append("[[package.wheels]]")
            lines.append(f'url = "https://example.com/{pkg["name"]}-{pkg["version"]}-py3-none-any.whl"')
        elif pkg.get("platform_wheel"):
            lines.append("")
            lines.append("[[package.wheels]]")
            lines.append(f'url = "https://example.com/{pkg["name"]}-{pkg["version"]}-cp312-linux_x86_64.whl"')
        lines.append("")
    lock_file = tmp_path / "uv.lock"
    lock_file.write_text("\n".join(lines))
    return lock_file


class TestCollectUpstreamBuildReqs:
    def test_uses_cached_results_and_skips_universal_wheels(self, tmp_path):
        """Universal wheel packages are skipped; cached entries are used."""
        lock_file = _make_uv_lock(
            tmp_path,
            [
                {"name": "requests", "version": "2.31.0", "universal_wheel": True},
                {
                    "name": "pydantic-core",
                    "version": "2.33.2",
                    "sdist_url": "https://example.com/pydantic_core-2.33.2.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )

        # Pre-populate cache for pydantic-core
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(json.dumps({"pydantic-core==2.33.2": ["maturin>=1.9.4,<2"]}))

        result = _collect_upstream_build_reqs(lock_file, cache_path)
        assert "maturin" in result
        assert result["maturin"] == {"maturin>=1.9.4,<2"}
        # requests has a universal wheel, so no build deps from it
        assert "requests" not in result

    def test_stale_cache_entries_excluded_from_output(self, tmp_path):
        """Cache entries for packages no longer in uv.lock are not included in output."""
        lock_file = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "pydantic-core",
                    "version": "2.33.2",
                    "sdist_url": "https://example.com/pydantic_core-2.33.2.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )

        # Cache has a stale entry for old-package that's no longer in uv.lock
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "pydantic-core==2.33.2": ["maturin>=1.9.4,<2"],
                    "old-package==1.0.0": ["flit-core>=3.0"],
                }
            )
        )

        result = _collect_upstream_build_reqs(lock_file, cache_path)
        assert "maturin" in result
        # old-package's build dep should NOT appear
        assert "flit-core" not in result

    def test_scan_error_not_cached(self, tmp_path):
        """When scanning fails, the error is NOT written to cache, and RuntimeError is raised."""
        lock_file = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "bad-package",
                    "version": "1.0.0",
                    "sdist_url": "https://example.com/bad-1.0.0.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )
        cache_path = tmp_path / "cache.json"

        with patch(
            "run_generate_constraints._stream_build_reqs_from_sdist",
            side_effect=ConnectionError("network error"),
        ):
            with pytest.raises(RuntimeError, match="Failed to scan"):
                _collect_upstream_build_reqs(lock_file, cache_path)

        # Cache file should NOT contain the failed package
        if cache_path.exists():
            cache = json.loads(cache_path.read_text())
            assert "bad-package==1.0.0" not in cache

    def test_successful_scan_is_cached(self, tmp_path):
        """Successful scans are persisted to cache."""
        lock_file = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "some-pkg",
                    "version": "1.0.0",
                    "sdist_url": "https://example.com/some-pkg-1.0.0.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )
        cache_path = tmp_path / "cache.json"

        with patch(
            "run_generate_constraints._stream_build_reqs_from_sdist",
            return_value=["setuptools>=70.0", "wheel"],
        ):
            result = _collect_upstream_build_reqs(lock_file, cache_path)

        assert "setuptools" in result
        assert result["setuptools"] == {"setuptools>=70.0"}

        cache = json.loads(cache_path.read_text())
        assert "some-pkg==1.0.0" in cache
        assert cache["some-pkg==1.0.0"] == ["setuptools>=70.0", "wheel"]

    def test_legacy_sdist_cached_as_setuptools(self, tmp_path):
        """A successful scan that finds no pyproject.toml caches ["setuptools", "wheel"]."""
        lock_file = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "legacy-pkg",
                    "version": "1.0.0",
                    "sdist_url": "https://example.com/legacy-1.0.0.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )
        cache_path = tmp_path / "cache.json"

        with patch(
            "run_generate_constraints._stream_build_reqs_from_sdist",
            return_value=[],  # No pyproject.toml found
        ):
            result = _collect_upstream_build_reqs(lock_file, cache_path)

        cache = json.loads(cache_path.read_text())
        assert cache["legacy-pkg==1.0.0"] == ["setuptools", "wheel"]
        assert "setuptools" in result

    def test_multiple_packages_same_build_dep_different_specifiers(self, tmp_path):
        """Multiple upstream packages requiring the same build dep with different
        specifiers preserve all requirement strings."""
        lock_file = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "pkg-a",
                    "version": "1.0.0",
                    "sdist_url": "https://example.com/a-1.0.0.tar.gz",
                    "platform_wheel": True,
                },
                {
                    "name": "pkg-b",
                    "version": "2.0.0",
                    "sdist_url": "https://example.com/b-2.0.0.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "pkg-a==1.0.0": ["setuptools>=70.0"],
                    "pkg-b==2.0.0": ["setuptools>=68.0,<72.0"],
                }
            )
        )

        result = _collect_upstream_build_reqs(lock_file, cache_path)
        assert result["setuptools"] == {"setuptools>=70.0", "setuptools>=68.0,<72.0"}


# ---------------------------------------------------------------------------
# _resolve_build_requirements
# ---------------------------------------------------------------------------


def _make_success_result():
    """Create a mock subprocess result for successful uv pip compile."""

    class FakeResult:
        returncode = 0
        stdout = ""
        stderr = ""

    return FakeResult()


class TestResolveBuildRequirements:
    def test_writes_range_specifiers_and_excludes_exact_pins(self, tmp_path):
        """Exact-pin requirements are excluded; range specifiers are written to input."""
        build_reqs = {
            "hatchling": {"hatchling==1.29.0", "hatchling>=1.20"},
            "setuptools": {"setuptools>=70.0"},
            "cython": {"cython==3.1.1", "cython>=3.1.2,<3.3.0"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        captured_reqs = []

        def mock_run_command(cmd, **kwargs):
            # Read the temp requirements file before it gets deleted
            reqs_file = Path(cmd[3])  # 4th arg is the requirements file path
            captured_reqs.append(reqs_file.read_text())
            output_path.write_text("")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            _resolve_build_requirements(build_reqs, output_path, config)

        # Exact pins (hatchling==1.29.0, cython==3.1.1) should be excluded
        lines = sorted(captured_reqs[0].strip().split("\n"))
        assert lines == sorted(["cython>=3.1.2,<3.3.0", "hatchling>=1.20", "setuptools>=70.0"])

    def test_includes_required_flags(self, tmp_path):
        """uv pip compile is called with --python-version, --resolution highest,
        --upgrade, --no-python-downloads, and cwd=AIRFLOW_ROOT_PATH."""
        build_reqs = {"setuptools": {"setuptools>=70.0"}}
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        captured_kwargs = {}

        def mock_run_command(cmd, **kwargs):
            captured_kwargs.update(kwargs)
            captured_kwargs["cmd"] = cmd
            output_path.write_text("")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            with patch("run_generate_constraints.AIRFLOW_ROOT_PATH", tmp_path):
                _resolve_build_requirements(build_reqs, output_path, config)

        cmd = captured_kwargs["cmd"]
        assert "--python-version" in cmd
        assert "3.12" in cmd
        assert "--resolution" in cmd
        assert "highest" in cmd
        assert "--upgrade" in cmd
        assert "--no-python-downloads" in cmd
        assert captured_kwargs["cwd"] == tmp_path

    def test_retries_on_conflict_and_skips_problematic_package(self, tmp_path):
        """When resolver fails due to conflicting ranges, the conflicting package
        is removed and resolution is retried."""
        build_reqs = {
            "cython": {"cython>=3.0,<3.1", "cython>=3.1.2,<3.3.0"},
            "setuptools": {"setuptools>=70.0"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        call_count = 0

        def mock_run_command(cmd, **kwargs):
            nonlocal call_count
            call_count += 1

            class FailResult:
                returncode = 1
                stdout = ""
                stderr = (
                    "Because you require cython>=3.0,<3.1 and cython>=3.1.2,<3.3.0, "
                    "we can conclude that your requirements are unsatisfiable."
                )

            if call_count == 1:
                return FailResult()
            # Second call (after removing cython) succeeds
            output_path.write_text("setuptools==80.9.0\n")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            _resolve_build_requirements(build_reqs, output_path, config)

        assert call_count == 2
        assert output_path.read_text() == "setuptools==80.9.0\n"

    def test_retries_on_conflict_when_uv_stderr_wording_changes(self, tmp_path):
        """Conflict handling should not depend on one exact uv diagnostic phrase."""
        build_reqs = {
            "cython": {"cython>=3.0,<3.1", "cython>=3.1.2,<3.3.0"},
            "setuptools": {"setuptools>=70.0"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        call_count = 0

        def mock_run_command(cmd, **kwargs):
            nonlocal call_count
            call_count += 1

            class FailResult:
                returncode = 1
                stdout = ""
                stderr = (
                    "Because the project depends on cython>=3.0,<3.1 and cython>=3.1.2,<3.3.0, "
                    "the requirements are unsatisfiable."
                )

            if call_count == 1:
                return FailResult()
            output_path.write_text("setuptools==80.9.0\n")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            _resolve_build_requirements(build_reqs, output_path, config)

        assert call_count == 2
        assert output_path.read_text() == "setuptools==80.9.0\n"

    def test_retries_on_conflict_when_uv_stderr_uses_marker_format(self, tmp_path):
        """uv diagnostics can render requirement markers between the name and specifier."""
        build_reqs = {
            "cffi": {
                "cffi>=2.0.0; platform_python_implementation != 'PyPy'",
                "cffi>=1.17,<2.dev0; python_full_version < '3.14' "
                "and platform_python_implementation != 'PyPy'",
            },
            "setuptools": {"setuptools>=70.0"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        call_count = 0

        def mock_run_command(cmd, **kwargs):
            nonlocal call_count
            call_count += 1

            class FailResult:
                returncode = 1
                stdout = ""
                stderr = (
                    "Because you require cffi{platform_python_implementation != 'PyPy'}>=2.0.0 "
                    "and cffi{python_full_version < '3.14' and platform_python_implementation != "
                    "'PyPy'}>=1.17,<2.dev0, we can conclude that your requirements are unsatisfiable."
                )

            if call_count == 1:
                return FailResult()
            output_path.write_text("setuptools==80.9.0\n")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            _resolve_build_requirements(build_reqs, output_path, config)

        assert call_count == 2
        assert output_path.read_text() == "setuptools==80.9.0\n"

    def test_retries_on_conflict_when_name_uses_underscore_separator(self, tmp_path):
        """Conflict detection must survive PEP 503 separator normalization.

        ``lines_by_name`` keys are normalized (``pdm-backend``) but uv echoes
        the raw requirement string back in stderr (``pdm_backend>=...``).
        A literal regex on the normalized key would miss the underscore form.
        """
        build_reqs = {
            "pdm-backend": {"pdm_backend>=2.0,<3", "pdm_backend>=3.0"},
            "setuptools": {"setuptools>=70.0"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        call_count = 0

        def mock_run_command(cmd, **kwargs):
            nonlocal call_count
            call_count += 1

            class FailResult:
                returncode = 1
                stdout = ""
                stderr = (
                    "Because you require pdm_backend>=2.0,<3 and pdm_backend>=3.0, "
                    "we can conclude that your requirements are unsatisfiable."
                )

            if call_count == 1:
                return FailResult()
            output_path.write_text("setuptools==80.9.0\n")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            _resolve_build_requirements(build_reqs, output_path, config)

        assert call_count == 2
        assert output_path.read_text() == "setuptools==80.9.0\n"

    def test_raises_when_stderr_mentions_no_known_package(self, tmp_path):
        """When the resolver fails for a reason unrelated to any candidate
        build dep, we must NOT silently drop a random package — we must fail
        loud so a maintainer can investigate.
        """
        build_reqs = {
            "setuptools": {"setuptools>=70.0"},
            "hatchling": {"hatchling>=1.20"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        def mock_run_command(cmd, **kwargs):
            class FailResult:
                returncode = 1
                stdout = ""
                stderr = (
                    "error: Failed to download distribution: connection refused\n"
                    "Caused by: network unreachable for files.pythonhosted.org"
                )

            return FailResult()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            with pytest.raises(RuntimeError, match="uv pip compile failed"):
                _resolve_build_requirements(build_reqs, output_path, config)

    def test_conflict_resolution_follows_stderr_order(self, tmp_path):
        """When stderr names multiple candidate packages, the first one in
        stderr is removed even if another candidate appears earlier in the
        input order.
        """
        build_reqs = {
            # ``setuptools`` is inserted first, but the conflict is on ``cython``.
            "setuptools": {"setuptools>=70.0"},
            "hatchling": {"hatchling>=1.20"},
            "cython": {"cython>=3.0,<3.1", "cython>=3.1.2,<3.3.0"},
        }
        output_path = tmp_path / "build-constraints-3.12.txt"
        config = ConfigParams(
            airflow_constraints_mode="constraints-source-providers",
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
        )

        call_count = 0
        captured_reqs: list[str] = []

        def mock_run_command(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            reqs_file = Path(cmd[3])
            captured_reqs.append(reqs_file.read_text())

            class FailResult:
                returncode = 1
                stdout = ""
                stderr = (
                    "Because you require cython>=3.0,<3.1 and cython>=3.1.2,<3.3.0, "
                    "and because you require setuptools>=70.0 and setuptools<60.0, "
                    "we can conclude that your requirements are unsatisfiable."
                )

            if call_count == 1:
                return FailResult()
            output_path.write_text("setuptools==80.9.0\nhatchling==1.29.0\n")
            return _make_success_result()

        with patch("run_generate_constraints.run_command", side_effect=mock_run_command):
            _resolve_build_requirements(build_reqs, output_path, config)

        assert call_count == 2
        # The retry must contain setuptools and hatchling but NOT cython
        retry_lines = sorted(captured_reqs[1].strip().split("\n"))
        assert "setuptools>=70.0" in retry_lines
        assert "hatchling>=1.20" in retry_lines
        assert not any("cython" in line for line in retry_lines)


# ===========================================================================
# Phase 3 tests: resolve_build_constraints_file, _get_build_constraints_flags,
# install_development_dependencies command assembly
# ===========================================================================


class TestResolveBuildConstraintsFile:
    """Tests for resolve_build_constraints_file()."""

    def test_explicit_local_file(self, tmp_path):
        """Explicit local path returns the path directly."""
        bc_file = tmp_path / "bc.txt"
        bc_file.write_text("setuptools==80.0.0\n")
        result = resolve_build_constraints_file(
            build_constraints_location=str(bc_file),
            constraints_reference=None,
            airflow_package_version=None,
            default_constraints_branch="constraints-main",
            github_repository="apache/airflow",
            python_version="3.12",
        )
        assert result == bc_file

    def test_explicit_local_file_missing_exits(self, tmp_path):
        """Explicit local path that doesn't exist causes sys.exit."""
        with pytest.raises(SystemExit):
            resolve_build_constraints_file(
                build_constraints_location=str(tmp_path / "nonexistent.txt"),
                constraints_reference=None,
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )

    def test_explicit_url_download_success(self, tmp_path):
        """Explicit URL downloads to target file."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            result = resolve_build_constraints_file(
                build_constraints_location="https://example.com/bc.txt",
                constraints_reference=None,
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            assert result is not None
            mock_dl.assert_called_once()

    def test_explicit_url_download_failure_exits(self):
        """Explicit URL that fails to download causes sys.exit."""
        with (
            patch(
                "install_airflow_and_providers._download_build_constraints",
                return_value=False,
            ),
            pytest.raises(SystemExit),
        ):
            resolve_build_constraints_file(
                build_constraints_location="https://example.com/bc.txt",
                constraints_reference=None,
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )

    def test_inferred_from_constraints_reference(self):
        """Uses constraints_reference when provided."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference="constraints-3.2.0",
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            url = mock_dl.call_args[0][0]
            assert "constraints-3.2.0/build-constraints-3.12.txt" in url

    def test_inferred_from_airflow_version(self):
        """Derives constraints-{version} from airflow_package_version when no reference."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference=None,
                airflow_package_version="3.2.0",
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            url = mock_dl.call_args[0][0]
            assert "constraints-3.2.0/build-constraints-3.12.txt" in url

    def test_fallback_to_default_branch(self):
        """Falls back to default_constraints_branch when no reference or version."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference=None,
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            url = mock_dl.call_args[0][0]
            assert "constraints-main/build-constraints-3.12.txt" in url

    def test_inferred_download_failure_returns_none(self):
        """Inferred URL that 404s returns None (warn + skip)."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=False,
        ):
            result = resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference="constraints-main",
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            assert result is None

    def test_constraints_reference_takes_precedence_over_version(self):
        """Explicit constraints_reference is used even when airflow_package_version is set."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference="constraints-custom-branch",
                airflow_package_version="3.2.0",
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            url = mock_dl.call_args[0][0]
            assert "constraints-custom-branch/build-constraints-3.12.txt" in url
            assert "constraints-3.2.0" not in url


class TestGetBuildConstraintsFlags:
    """Tests for _get_build_constraints_flags()."""

    def test_none_returns_empty(self):
        assert _get_build_constraints_flags(None) == []

    def test_nonexistent_file_returns_empty(self, tmp_path):
        assert _get_build_constraints_flags(tmp_path / "nonexistent.txt") == []

    def test_empty_file_returns_empty(self, tmp_path):
        bc = tmp_path / "bc.txt"
        bc.write_text("")
        assert _get_build_constraints_flags(bc) == []

    def test_nonempty_file_returns_flags(self, tmp_path):
        bc = tmp_path / "bc.txt"
        bc.write_text("setuptools==80.0.0\n")
        result = _get_build_constraints_flags(bc)
        assert result == ["--build-constraints", str(bc)]

    def test_fallback_path_no_build_constraints(self):
        """Fallback (no-constraints) install paths should not include build constraints.

        This test documents the invariant that callers should not pass build constraints
        to fallback install commands.
        """
        # When build_constraints_file is None (download failed / not available),
        # flags must be empty
        assert _get_build_constraints_flags(None) == []

    def test_directory_returns_empty(self, tmp_path):
        """Directory path should not return build constraints flags."""
        assert _get_build_constraints_flags(tmp_path) == []


class TestResolveBuildConstraintsFileIsFile:
    """Tests for is_file() checks in resolve_build_constraints_file()."""

    def test_explicit_local_directory_exits(self, tmp_path):
        """Explicit local path that is a directory causes sys.exit."""
        with pytest.raises(SystemExit):
            resolve_build_constraints_file(
                build_constraints_location=str(tmp_path),
                constraints_reference=None,
                airflow_package_version=None,
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )

    def test_non_version_string_falls_back_to_default(self):
        """Literal 'wheel'/'sdist' does not match version regex, falls back to default branch."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference=None,
                airflow_package_version="wheel",
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            url = mock_dl.call_args[0][0]
            assert "constraints-main/build-constraints-3.12.txt" in url

    def test_prerelease_version_derives_constraints_reference(self):
        """Pre-release version like '3.2.0rc1' derives constraints-3.2.0rc1."""
        with patch(
            "install_airflow_and_providers._download_build_constraints",
            return_value=True,
        ) as mock_dl:
            resolve_build_constraints_file(
                build_constraints_location=None,
                constraints_reference=None,
                airflow_package_version="3.2.0rc1",
                default_constraints_branch="constraints-main",
                github_repository="apache/airflow",
                python_version="3.12",
            )
            url = mock_dl.call_args[0][0]
            assert "constraints-3.2.0rc1/build-constraints-3.12.txt" in url


class TestInstallDevelopmentDependenciesCommandAssembly:
    """Tests for install_development_dependencies command assembly."""

    def _prepare_airflow_root(self, tmp_path: Path) -> Path:
        airflow_root = tmp_path / "airflow"
        (airflow_root / "devel-common").mkdir(parents=True)
        (airflow_root / "generated").mkdir()
        (airflow_root / "devel-common" / "pyproject.toml").write_text(
            """[project]
dependencies = [
    "pendulum>=3",
    "pytest>=8; python_version >= '3.0'",
    "skip-me>=1; python_version < '1.0'",
]
"""
        )
        (airflow_root / "generated" / "provider_dependencies.json").write_text(
            json.dumps({"amazon": {"devel-deps": ["boto3>=1"]}})
        )
        return airflow_root

    def _run_install(
        self,
        monkeypatch: pytest.MonkeyPatch,
        airflow_root: Path,
        constraint: Path,
        build_constraints: Path,
        returncodes: tuple[int, ...] = (0,),
        github_actions: bool = False,
    ) -> list[tuple[list[str], dict[str, object]]]:
        calls: list[tuple[list[str], dict[str, object]]] = []
        returncode_iter = iter(returncodes)

        def fake_run_command(command, **kwargs):
            calls.append((command, kwargs))
            return SimpleNamespace(returncode=next(returncode_iter))

        monkeypatch.setattr(install_dev_deps, "AIRFLOW_ROOT_PATH", airflow_root)
        monkeypatch.setattr(install_dev_deps, "run_command", fake_run_command)
        callback = install_dev_deps.install_development_dependencies.callback
        assert callback
        callback(
            constraint=str(constraint),
            build_constraints=str(build_constraints),
            github_actions=github_actions,
        )
        return calls

    def test_adds_build_constraints_when_file_is_nonempty(self, tmp_path, monkeypatch):
        airflow_root = self._prepare_airflow_root(tmp_path)
        constraint = tmp_path / "constraints.txt"
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")

        calls = self._run_install(
            monkeypatch,
            airflow_root,
            constraint,
            build_constraints,
            github_actions=True,
        )

        assert len(calls) == 1
        command, kwargs = calls[0]
        assert command[:3] == ["uv", "pip", "install"]
        assert "pendulum>=3" in command
        assert "pytest>=8" in command
        assert "skip-me>=1" not in command
        assert "boto3>=1" in command
        assert command[command.index("--build-constraints") + 1] == str(build_constraints)
        assert command[-2:] == ["--constraints", str(constraint)]
        assert kwargs == {"check": False, "github_actions": True}

    @pytest.mark.parametrize("path_kind", ["missing", "empty", "directory"])
    def test_skips_build_constraints_when_path_is_not_nonempty_file(self, tmp_path, monkeypatch, path_kind):
        airflow_root = self._prepare_airflow_root(tmp_path)
        constraint = tmp_path / "constraints.txt"
        build_constraints = tmp_path / "build-constraints.txt"
        if path_kind == "empty":
            build_constraints.write_text("")
        elif path_kind == "directory":
            build_constraints.mkdir()

        calls = self._run_install(monkeypatch, airflow_root, constraint, build_constraints)

        assert len(calls) == 1
        command, _ = calls[0]
        assert "--build-constraints" not in command
        assert command[-2:] == ["--constraints", str(constraint)]

    def test_fallback_command_does_not_include_constraints(self, tmp_path, monkeypatch):
        airflow_root = self._prepare_airflow_root(tmp_path)
        constraint = tmp_path / "constraints.txt"
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")

        calls = self._run_install(
            monkeypatch,
            airflow_root,
            constraint,
            build_constraints,
            returncodes=(1, 0),
        )

        assert len(calls) == 2
        first_command, first_kwargs = calls[0]
        fallback_command, fallback_kwargs = calls[1]
        assert "--constraints" in first_command
        assert "--build-constraints" in first_command
        assert "--constraints" not in fallback_command
        assert "--build-constraints" not in fallback_command
        assert fallback_command[:3] == ["uv", "pip", "install"]
        assert first_kwargs == {"check": False, "github_actions": False}
        assert fallback_kwargs == {"check": False, "github_actions": False}
