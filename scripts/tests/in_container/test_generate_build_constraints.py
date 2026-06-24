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
import os
import shutil
import subprocess
import tarfile
import urllib.error
import zipfile
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from run_generate_constraints import (
    BUILD_CONSTRAINTS_PREFIX,
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
    _stream_build_reqs_from_sdist,
    generate_build_constraints,
    generate_constraints,
)

AIRFLOW_ROOT = Path(__file__).resolve().parents[3]


def _config(python: str = "3.12") -> ConfigParams:
    return ConfigParams(
        airflow_constraints_mode="constraints-source-providers",
        constraints_github_repository="apache/airflow",
        default_constraints_branch="main",
        github_actions=False,
        python=python,
    )


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
def test_normalize_package_name(raw, expected):
    assert _normalize_package_name(raw) == expected


@pytest.mark.parametrize(
    ("requirement", "expected"),
    [
        ("setuptools", "setuptools"),
        ("maturin>=1.9.4,<2", "maturin"),
        ("hatchling==1.29.0", "hatchling"),
        ("Cython>=3.0", "cython"),
        ("GitPython>=3.1.30", "gitpython"),
    ],
)
def test_extract_package_name(requirement, expected):
    assert _extract_package_name(requirement) == expected


@pytest.mark.parametrize(
    ("requirement", "expected"),
    [
        ("setuptools==80.9.0", True),
        ("Cython == 3.1.1", True),
        ("tomli==2.4.1; python_version < '3.11'", True),
        ("setuptools>=70.0", False),
        ("maturin>=1.9.4,<2", False),
        ("setuptools!=74.0.0", False),
        ("cython==3.*", False),
        ("cffi~=1.17", False),
        ("wheel", False),
    ],
)
def test_is_exact_pin(requirement, expected):
    assert _is_exact_pin(requirement) is expected


class TestCollectWorkspaceBuildReqs:
    def test_collects_all_specifiers_for_each_package(self, tmp_path):
        for directory, content in {
            "pkg_a": '[build-system]\nrequires = ["hatchling==1.29.0", "setuptools>=70"]\n',
            "pkg_b": '[build-system]\nrequires = ["hatchling>=1.20"]\n',
        }.items():
            package_dir = tmp_path / directory
            package_dir.mkdir()
            (package_dir / "pyproject.toml").write_text(content)

        result = _collect_workspace_build_reqs(tmp_path)

        assert result == {
            "hatchling": {"hatchling==1.29.0", "hatchling>=1.20"},
            "setuptools": {"setuptools>=70"},
        }

    @pytest.mark.parametrize(
        "directory",
        [
            ".git/worktree",
            ".venv/lib",
            ".build/airflow-source",
            "dist/extracted",
            "files/constraints-3.12",
            "node_modules/package",
            "__pycache__/package",
            ".tox/package",
            ".mypy_cache/package",
            ".ruff_cache/package",
        ],
    )
    def test_skips_transient_and_generated_directories(self, tmp_path, directory):
        authoritative = tmp_path / "package"
        authoritative.mkdir()
        (authoritative / "pyproject.toml").write_text('[build-system]\nrequires = ["hatchling==1.29.0"]\n')
        transient = tmp_path / directory
        transient.mkdir(parents=True)
        (transient / "pyproject.toml").write_text('[build-system]\nrequires = ["pollution>=1"]\n')

        result = _collect_workspace_build_reqs(tmp_path)

        assert result == {"hatchling": {"hatchling==1.29.0"}}

    @mock.patch("run_generate_constraints.console", autospec=True)
    def test_warns_and_continues_for_malformed_pyproject(self, console, tmp_path):
        (tmp_path / "pyproject.toml").write_text("not valid toml {{{")

        assert _collect_workspace_build_reqs(tmp_path) == {}
        assert "failed to parse" in console.print.call_args.args[0]


def _make_uv_lock(tmp_path: Path, packages: list[dict[str, object]]) -> Path:
    lines: list[str] = []
    for package in packages:
        lines.extend(
            [
                "[[package]]",
                f'name = "{package["name"]}"',
                f'version = "{package["version"]}"',
            ]
        )
        if sdist_url := package.get("sdist_url"):
            lines.extend(["", "[package.sdist]", f'url = "{sdist_url}"'])
        if package.get("universal_wheel"):
            lines.extend(
                [
                    "",
                    "[[package.wheels]]",
                    f'url = "https://example.invalid/{package["name"]}-py3-none-any.whl"',
                ]
            )
        elif package.get("platform_wheel"):
            lines.extend(
                [
                    "",
                    "[[package.wheels]]",
                    f'url = "https://example.invalid/{package["name"]}-cp312-linux_x86_64.whl"',
                ]
            )
        lines.append("")
    lock_path = tmp_path / "uv.lock"
    lock_path.write_text("\n".join(lines))
    return lock_path


def test_parse_uv_lock_selects_sdist_and_wheel_metadata(tmp_path):
    lock_path = _make_uv_lock(
        tmp_path,
        [
            {
                "name": "pydantic-core",
                "version": "2.33.2",
                "sdist_url": "https://example.invalid/pydantic-core.tar.gz",
                "platform_wheel": True,
            },
            {"name": "requests", "version": "2.31.0", "universal_wheel": True},
        ],
    )

    packages = _parse_uv_lock(lock_path)

    assert [
        (
            package.name,
            package.version,
            package.sdist_url,
            package.has_universal_wheel,
        )
        for package in packages
    ] == [
        (
            "pydantic-core",
            "2.33.2",
            "https://example.invalid/pydantic-core.tar.gz",
            False,
        ),
        ("requests", "2.31.0", None, True),
    ]


def _pyproject_toml(requirements: list[str]) -> bytes:
    return (
        f'[build-system]\nrequires = {json.dumps(requirements)}\nbuild-backend = "setuptools.build_meta"\n'
    ).encode()


def _make_tar_sdist(
    tmp_path: Path,
    requirements: list[str] | None,
    *,
    nested_requirements: list[str] | None = None,
) -> Path:
    archive_path = tmp_path / "package-1.0.0.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        if nested_requirements:
            nested_data = _pyproject_toml(nested_requirements)
            nested_info = tarfile.TarInfo("package-1.0.0/vendor/project/pyproject.toml")
            nested_info.size = len(nested_data)
            archive.addfile(nested_info, io.BytesIO(nested_data))
        if requirements is None:
            setup_data = b"from setuptools import setup\nsetup()\n"
            info = tarfile.TarInfo("package-1.0.0/setup.py")
            info.size = len(setup_data)
            archive.addfile(info, io.BytesIO(setup_data))
        else:
            data = _pyproject_toml(requirements)
            info = tarfile.TarInfo("package-1.0.0/pyproject.toml")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return archive_path


def _make_zip_sdist(
    tmp_path: Path,
    requirements: list[str] | None,
    *,
    nested_requirements: list[str] | None = None,
) -> Path:
    archive_path = tmp_path / "package-1.0.0.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        if nested_requirements:
            archive.writestr(
                "package-1.0.0/vendor/project/pyproject.toml",
                _pyproject_toml(nested_requirements),
            )
        if requirements is None:
            archive.writestr("package-1.0.0/setup.py", "from setuptools import setup\nsetup()\n")
        else:
            archive.writestr("package-1.0.0/pyproject.toml", _pyproject_toml(requirements))
    return archive_path


@pytest.mark.parametrize(
    ("factory", "extractor"),
    [
        (_make_tar_sdist, _extract_build_reqs_from_tar),
        (_make_zip_sdist, _extract_build_reqs_from_zip),
    ],
)
def test_extracts_only_top_level_sdist_pyproject(tmp_path, factory, extractor):
    archive_path = factory(
        tmp_path,
        ["right-backend>=2"],
        nested_requirements=["wrong-backend>=1"],
    )

    assert extractor(archive_path.as_uri()) == ["right-backend>=2"]


@pytest.mark.parametrize(
    ("factory", "extractor"),
    [
        (_make_tar_sdist, _extract_build_reqs_from_tar),
        (_make_zip_sdist, _extract_build_reqs_from_zip),
    ],
)
def test_legacy_sdist_has_no_declared_build_requirements(tmp_path, factory, extractor):
    assert extractor(factory(tmp_path, None).as_uri()) == []


class TestSdistDownloadRetry:
    @pytest.mark.parametrize(
        "error",
        [
            urllib.error.URLError("temporary DNS failure"),
            TimeoutError("timed out"),
            ConnectionError("connection reset"),
            urllib.error.HTTPError(
                "https://example.invalid/package.tar.gz",
                503,
                "unavailable",
                Message(),
                None,
            ),
        ],
    )
    @mock.patch("run_generate_constraints._extract_build_reqs_from_tar", autospec=True)
    def test_retries_transient_errors_with_exponential_backoff(self, extract, error):
        extract.side_effect = [error, error, ["setuptools>=70"]]
        sleep = mock.create_autospec(lambda seconds: None)

        result = _stream_build_reqs_from_sdist(
            "https://example.invalid/package.tar.gz",
            sleep=sleep,
        )

        assert result == ["setuptools>=70"]
        assert sleep.call_args_list == [mock.call(1), mock.call(2)]

    @mock.patch("run_generate_constraints._extract_build_reqs_from_tar", autospec=True)
    def test_raises_after_retry_attempts_are_exhausted(self, extract):
        error = urllib.error.URLError("temporary DNS failure")
        extract.side_effect = error
        sleep = mock.create_autospec(lambda seconds: None)

        with pytest.raises(urllib.error.URLError, match="temporary DNS failure"):
            _stream_build_reqs_from_sdist(
                "https://example.invalid/package.tar.gz",
                sleep=sleep,
            )

        assert extract.call_count == 3
        assert sleep.call_args_list == [mock.call(1), mock.call(2)]

    @mock.patch("run_generate_constraints._extract_build_reqs_from_tar", autospec=True)
    def test_does_not_retry_parse_errors(self, extract):
        extract.side_effect = ValueError("invalid pyproject")
        sleep = mock.create_autospec(lambda seconds: None)

        with pytest.raises(ValueError, match="invalid pyproject"):
            _stream_build_reqs_from_sdist(
                "https://example.invalid/package.tar.gz",
                sleep=sleep,
            )

        extract.assert_called_once()
        sleep.assert_not_called()

    @mock.patch(
        "run_generate_constraints._extract_build_reqs_from_zip",
        autospec=True,
        return_value=["hatchling>=1"],
    )
    def test_dispatches_zip_sdists(self, extract):
        assert _stream_build_reqs_from_sdist("https://example.invalid/package.zip") == ["hatchling>=1"]
        extract.assert_called_once()


class TestCollectUpstreamBuildReqs:
    def test_uses_cache_and_skips_universal_wheels(self, tmp_path):
        lock_path = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "platform-package",
                    "version": "1.0",
                    "sdist_url": "https://example.invalid/platform.tar.gz",
                    "platform_wheel": True,
                },
                {
                    "name": "universal-package",
                    "version": "2.0",
                    "sdist_url": "https://example.invalid/universal.tar.gz",
                    "universal_wheel": True,
                },
                {"name": "wheel-only", "version": "3.0", "platform_wheel": True},
            ],
        )
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(json.dumps({"platform-package==1.0": ["maturin>=1.9,<2"]}))

        with mock.patch("run_generate_constraints._stream_build_reqs_from_sdist", autospec=True) as scan:
            result = _collect_upstream_build_reqs(lock_path, cache_path)

        assert result == {"maturin": {"maturin>=1.9,<2"}}
        scan.assert_not_called()

    def test_excludes_stale_cache_entries(self, tmp_path):
        lock_path = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "current-package",
                    "version": "1.0",
                    "sdist_url": "https://example.invalid/current.tar.gz",
                    "platform_wheel": True,
                }
            ],
        )
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "current-package==1.0": ["setuptools>=70"],
                    "stale-package==0.1": ["flit-core>=3"],
                }
            )
        )

        result = _collect_upstream_build_reqs(lock_path, cache_path)

        assert result == {"setuptools": {"setuptools>=70"}}

    @mock.patch(
        "run_generate_constraints._stream_build_reqs_from_sdist",
        autospec=True,
        return_value=["setuptools>=70", "wheel"],
    )
    def test_caches_successful_scans(self, scan, tmp_path):
        lock_path = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "source-package",
                    "version": "1.0",
                    "sdist_url": "https://example.invalid/source.tar.gz",
                    "platform_wheel": True,
                }
            ],
        )
        cache_path = tmp_path / "cache.json"

        result = _collect_upstream_build_reqs(lock_path, cache_path)

        assert result == {
            "setuptools": {"setuptools>=70"},
            "wheel": {"wheel"},
        }
        assert json.loads(cache_path.read_text()) == {"source-package==1.0": ["setuptools>=70", "wheel"]}
        scan.assert_called_once()

    @mock.patch(
        "run_generate_constraints._stream_build_reqs_from_sdist",
        autospec=True,
        return_value=[],
    )
    def test_caches_legacy_sdists_as_setuptools_and_wheel(self, scan, tmp_path):
        lock_path = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "legacy-package",
                    "version": "1.0",
                    "sdist_url": "https://example.invalid/legacy.tar.gz",
                    "platform_wheel": True,
                }
            ],
        )
        cache_path = tmp_path / "cache.json"

        result = _collect_upstream_build_reqs(lock_path, cache_path)

        assert result == {
            "setuptools": {"setuptools"},
            "wheel": {"wheel"},
        }
        assert json.loads(cache_path.read_text()) == {"legacy-package==1.0": ["setuptools", "wheel"]}
        scan.assert_called_once()

    @mock.patch(
        "run_generate_constraints._stream_build_reqs_from_sdist",
        autospec=True,
        side_effect=ConnectionError("network failure"),
    )
    def test_does_not_cache_failed_scans(self, scan, tmp_path):
        lock_path = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "failed-package",
                    "version": "1.0",
                    "sdist_url": "https://example.invalid/failed.tar.gz",
                    "platform_wheel": True,
                }
            ],
        )
        cache_path = tmp_path / "cache.json"

        with pytest.raises(RuntimeError, match="failed-package==1.0"):
            _collect_upstream_build_reqs(lock_path, cache_path)

        assert not cache_path.exists()
        scan.assert_called_once()

    def test_preserves_multiple_ranges_for_one_build_dependency(self, tmp_path):
        lock_path = _make_uv_lock(
            tmp_path,
            [
                {
                    "name": "package-a",
                    "version": "1.0",
                    "sdist_url": "https://example.invalid/a.tar.gz",
                    "platform_wheel": True,
                },
                {
                    "name": "package-b",
                    "version": "2.0",
                    "sdist_url": "https://example.invalid/b.tar.gz",
                    "platform_wheel": True,
                },
            ],
        )
        cache_path = tmp_path / "cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "package-a==1.0": ["setuptools>=70"],
                    "package-b==2.0": ["setuptools>=68,<72"],
                }
            )
        )

        result = _collect_upstream_build_reqs(lock_path, cache_path)

        assert result == {"setuptools": {"setuptools>=70", "setuptools>=68,<72"}}

    @mock.patch("run_generate_constraints.console", autospec=True)
    def test_discards_corrupt_cache(self, console, tmp_path):
        lock_path = _make_uv_lock(tmp_path, [])
        cache_path = tmp_path / "cache.json"
        cache_path.write_text("{broken")

        assert _collect_upstream_build_reqs(lock_path, cache_path) == {}
        assert "discarding cache" in console.print.call_args.args[0]


def _success_result() -> SimpleNamespace:
    return SimpleNamespace(returncode=0, stdout="", stderr="")


class TestResolveBuildRequirements:
    @mock.patch("run_generate_constraints.run_command", autospec=True)
    def test_omits_exact_pins_and_passes_all_ranges_to_uv(self, run_command, tmp_path):
        output_path = tmp_path / "build-constraints-3.12.txt"
        captured: list[str] = []

        def compile_requirements(command, **kwargs):
            captured.append(Path(command[3]).read_text())
            output_path.write_text("hatchling==1.30.1\nsetuptools==80.9.0\n")
            return _success_result()

        run_command.side_effect = compile_requirements
        _resolve_build_requirements(
            {
                "hatchling": {"hatchling==1.29.0", "hatchling>=1.20"},
                "setuptools": {"setuptools>=70"},
                "cython": {"cython==3.1.1", "cython>=3.1.2,<3.3"},
            },
            output_path,
            _config(),
        )

        assert captured == ["cython>=3.1.2,<3.3\nhatchling>=1.20\nsetuptools>=70\n"]
        command = run_command.call_args.args[0]
        assert command[:3] == ["uv", "pip", "compile"]
        assert command[4:] == [
            "--no-config",
            "--python-version",
            "3.12",
            "--resolution",
            "highest",
            "--upgrade",
            "--no-python-downloads",
            "--no-annotate",
            "--no-header",
            "-o",
            str(output_path),
        ]
        assert run_command.call_args.kwargs["cwd"] == AIRFLOW_ROOT

    @mock.patch("run_generate_constraints.run_command", autospec=True)
    def test_writes_empty_file_when_every_requirement_is_exact(self, run_command, tmp_path):
        output_path = tmp_path / "build-constraints.txt"

        _resolve_build_requirements(
            {"hatchling": {"hatchling==1.30.1"}},
            output_path,
            _config(),
        )

        assert output_path.read_text() == ""
        run_command.assert_not_called()

    @pytest.mark.parametrize(
        "stderr",
        [
            "Because you require cython>=3.0,<3.1 and cython>=3.1.2,<3.3, requirements conflict.",
            "The project depends on cython>=3.0,<3.1 and cython>=3.1.2,<3.3.",
            (
                "Because you require cffi{platform_python_implementation != 'PyPy'}>=2.0 "
                "and cffi{python_full_version < '3.14'}>=1.17,<2.dev0, requirements conflict."
            ),
            "Because you require pdm_backend>=2,<3 and pdm_backend>=3, requirements conflict.",
        ],
    )
    @mock.patch("run_generate_constraints.run_command", autospec=True)
    def test_skips_identified_conflict_and_retries(self, run_command, stderr, tmp_path):
        if "cffi" in stderr:
            conflict_name = "cffi"
            conflicting = {
                "cffi>=2.0; platform_python_implementation != 'PyPy'",
                "cffi>=1.17,<2.dev0; python_full_version < '3.14'",
            }
        elif "pdm_backend" in stderr:
            conflict_name = "pdm-backend"
            conflicting = {"pdm_backend>=2,<3", "pdm_backend>=3"}
        else:
            conflict_name = "cython"
            conflicting = {"cython>=3.0,<3.1", "cython>=3.1.2,<3.3"}

        output_path = tmp_path / "build-constraints.txt"
        calls = 0

        def compile_requirements(command, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return SimpleNamespace(returncode=1, stdout="", stderr=stderr)
            retry_input = Path(command[3]).read_text()
            assert conflict_name not in retry_input
            output_path.write_text("setuptools==80.9.0\n")
            return _success_result()

        run_command.side_effect = compile_requirements
        _resolve_build_requirements(
            {
                conflict_name: conflicting,
                "setuptools": {"setuptools>=70"},
            },
            output_path,
            _config(),
        )

        assert calls == 2
        assert output_path.read_text() == "setuptools==80.9.0\n"

    @mock.patch("run_generate_constraints.run_command", autospec=True)
    def test_uses_first_known_package_in_stderr_order(self, run_command, tmp_path):
        output_path = tmp_path / "build-constraints.txt"
        inputs: list[str] = []

        def compile_requirements(command, **kwargs):
            inputs.append(Path(command[3]).read_text())
            if len(inputs) == 1:
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=(
                        "cython>=3.0,<3.1 conflicts with cython>=3.1.2; "
                        "setuptools>=70 conflicts with setuptools<60"
                    ),
                )
            output_path.write_text("setuptools==80.9.0\nhatchling==1.30.1\n")
            return _success_result()

        run_command.side_effect = compile_requirements
        _resolve_build_requirements(
            {
                "setuptools": {"setuptools>=70"},
                "hatchling": {"hatchling>=1.20"},
                "cython": {"cython>=3.0,<3.1", "cython>=3.1.2"},
            },
            output_path,
            _config(),
        )

        assert "cython" not in inputs[1]
        assert "setuptools>=70" in inputs[1]

    @pytest.mark.parametrize(
        "stderr",
        [
            "error: network unreachable for files.pythonhosted.org",
            "package-not-in-input>=1 failed to download",
            "failed to download setuptools>=70 from the package index",
        ],
    )
    @mock.patch("run_generate_constraints.run_command", autospec=True)
    def test_fails_loudly_when_uv_output_cannot_be_classified(self, run_command, stderr, tmp_path):
        run_command.return_value = SimpleNamespace(returncode=1, stdout="", stderr=stderr)

        with pytest.raises(RuntimeError, match="uv pip compile failed") as error:
            _resolve_build_requirements(
                {"setuptools": {"setuptools>=70"}},
                tmp_path / "build-constraints.txt",
                _config(),
            )

        assert stderr in str(error.value)

    @mock.patch("run_generate_constraints.run_command", autospec=True)
    def test_has_a_bounded_number_of_conflict_retries(self, run_command, tmp_path):
        names = [f"backend-{index}" for index in range(10)]
        build_reqs = {name: {f"{name}>=2", f"{name}<1"} for name in names}
        calls = 0

        def fail_with_next_conflict(command, **kwargs):
            nonlocal calls
            name = names[calls]
            calls += 1
            return SimpleNamespace(
                returncode=1,
                stdout="",
                stderr=f"{name}>=2 conflicts with {name}<1",
            )

        run_command.side_effect = fail_with_next_conflict

        with pytest.raises(RuntimeError, match="after 10 attempts"):
            _resolve_build_requirements(
                build_reqs,
                tmp_path / "build-constraints.txt",
                _config(),
            )

        assert calls == 10


@pytest.mark.skipif(
    shutil.which("uv") is None,
    reason="uv is required for the real resolver smoke test",
)
def test_real_uv_conflict_diagnostic_can_be_classified(tmp_path):
    output_path = tmp_path / "build-constraints.txt"

    _resolve_build_requirements(
        {
            "cython": {"cython>=3.0,<3.1", "cython>=3.1.2,<3.3"},
            "setuptools": {"setuptools>=70"},
        },
        output_path,
        _config(),
    )

    content = output_path.read_text().lower()
    assert "cython" not in content
    assert "setuptools==" in content


@mock.patch("run_generate_constraints._resolve_build_requirements", autospec=True)
@mock.patch("run_generate_constraints._collect_upstream_build_reqs", autospec=True)
@mock.patch("run_generate_constraints._collect_workspace_build_reqs", autospec=True)
def test_generate_build_constraints_merges_requirements_and_adds_header(
    collect_workspace,
    collect_upstream,
    resolve,
    tmp_path,
):
    collect_workspace.return_value = {
        "setuptools": {"setuptools>=70"},
        "hatchling": {"hatchling>=1.20"},
    }
    collect_upstream.return_value = {
        "setuptools": {"setuptools>=68,<72"},
        "maturin": {"maturin>=1.9,<2"},
    }
    config = _config()

    def write_pins(requirements, output_path, config_params):
        assert requirements == {
            "setuptools": {"setuptools>=70", "setuptools>=68,<72"},
            "hatchling": {"hatchling>=1.20"},
            "maturin": {"maturin>=1.9,<2"},
        }
        output_path.write_text("hatchling==1.30.1\nmaturin==1.9.6\nsetuptools==71.1.0\n")

    resolve.side_effect = write_pins
    with mock.patch.object(ConfigParams, "constraints_dir", tmp_path):
        generate_build_constraints(config)

    output_path = tmp_path / "build-constraints-3.12.txt"
    assert output_path.read_text() == (
        BUILD_CONSTRAINTS_PREFIX + "hatchling==1.30.1\nmaturin==1.9.6\nsetuptools==71.1.0\n"
    )
    collect_upstream.assert_called_once_with(
        uv_lock_path=AIRFLOW_ROOT / "uv.lock",
        cache_path=tmp_path / "build-deps-cache.json",
    )


@pytest.mark.parametrize(
    ("mode", "runtime_function"),
    [
        ("constraints", "generate_constraints_pypi_providers"),
        ("constraints-source-providers", "generate_constraints_source_providers"),
        ("constraints-no-providers", "generate_constraints_no_providers"),
    ],
)
def test_each_runtime_mode_also_generates_build_constraints_without_changing_runtime_output(
    tmp_path,
    mode,
    runtime_function,
):
    runtime_content = f"{mode}-runtime-output\n"

    def generate_runtime(config):
        config.current_constraints_file.write_text(runtime_content)

    def generate_build(config):
        assert config.current_constraints_file.read_text() == runtime_content
        (config.constraints_dir / f"build-constraints-{config.python}.txt").write_text("setuptools==80.9.0\n")

    with (
        mock.patch.object(ConfigParams, "constraints_dir", tmp_path),
        mock.patch(
            f"run_generate_constraints.{runtime_function}",
            autospec=True,
            side_effect=generate_runtime,
        ),
        mock.patch(
            "run_generate_constraints.generate_build_constraints",
            autospec=True,
            side_effect=generate_build,
        ),
    ):
        generate_constraints.callback(
            airflow_constraints_mode=mode,
            constraints_github_repository="apache/airflow",
            default_constraints_branch="main",
            github_actions=False,
            python="3.12",
            use_uv=True,
        )

    assert (tmp_path / f"{mode}-3.12.txt").read_text() == runtime_content
    assert (tmp_path / "build-constraints-3.12.txt").read_text() == "setuptools==80.9.0\n"


def _run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )


def _initialize_constraints_repository(tmp_path: Path) -> tuple[Path, Path]:
    files_dir = tmp_path / "files" / "constraints-3.12"
    files_dir.mkdir(parents=True)
    constraints_dir = tmp_path / "constraints"
    constraints_dir.mkdir()
    _run(["git", "init"], cwd=constraints_dir)
    _run(["git", "config", "user.email", "test@example.com"], cwd=constraints_dir)
    _run(["git", "config", "user.name", "Test User"], cwd=constraints_dir)
    (constraints_dir / "constraints-3.12.txt").write_text("# old\npackage==1\n")
    (constraints_dir / "notes.txt").write_text("keep me out of publication commits\n")
    _run(["git", "add", "."], cwd=constraints_dir)
    _run(["git", "commit", "-m", "Initial constraints"], cwd=constraints_dir)
    return files_dir, constraints_dir


def _publication_env() -> dict[str, str]:
    return {
        **os.environ,
        "GITHUB_RUN_ID": "1234",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_REPOSITORY": "apache/airflow",
        "GITHUB_SHA": "deadbeef",
    }


class TestConstraintsPublicationScripts:
    def test_stages_and_commits_new_build_constraints_only(self, tmp_path):
        files_dir, constraints_dir = _initialize_constraints_repository(tmp_path)
        (files_dir / "constraints-3.12.txt").write_text("# new\npackage==2\n")
        (files_dir / "build-constraints-3.12.txt").write_text("# generated\nsetuptools==80.9.0\n")
        (constraints_dir / "notes.txt").write_text("unrelated local edit\n")

        diff = _run(
            ["bash", str(AIRFLOW_ROOT / "scripts/ci/constraints/ci_diff_constraints.sh")],
            cwd=tmp_path,
        )
        assert "Changes detected in constraints" in diff.stdout
        assert _run(["git", "diff", "--cached", "--name-only"], cwd=constraints_dir).stdout == ""

        _run(
            ["bash", str(AIRFLOW_ROOT / "scripts/ci/constraints/ci_commit_constraints.sh")],
            cwd=tmp_path,
            env=_publication_env(),
        )

        committed_files = set(
            _run(
                ["git", "show", "--pretty=format:", "--name-only", "HEAD"],
                cwd=constraints_dir,
            ).stdout.split()
        )
        assert committed_files == {
            "build-constraints-3.12.txt",
            "constraints-3.12.txt",
        }
        assert _run(["git", "status", "--short"], cwd=constraints_dir).stdout == " M notes.txt\n"

    def test_comment_only_changes_do_not_create_publication_commit(self, tmp_path):
        files_dir, constraints_dir = _initialize_constraints_repository(tmp_path)
        (constraints_dir / "build-constraints-3.12.txt").write_text(
            "# old generated time\nsetuptools==80.9.0\n"
        )
        _run(["git", "add", "build-constraints-3.12.txt"], cwd=constraints_dir)
        _run(["git", "commit", "-m", "Add build constraints"], cwd=constraints_dir)
        before = _run(["git", "rev-parse", "HEAD"], cwd=constraints_dir).stdout

        (files_dir / "constraints-3.12.txt").write_text("# new generated time\npackage==1\n")
        (files_dir / "build-constraints-3.12.txt").write_text("# new generated time\nsetuptools==80.9.0\n")

        diff = _run(
            ["bash", str(AIRFLOW_ROOT / "scripts/ci/constraints/ci_diff_constraints.sh")],
            cwd=tmp_path,
        )
        assert "No changes in constraints" in diff.stdout
        _run(
            ["bash", str(AIRFLOW_ROOT / "scripts/ci/constraints/ci_commit_constraints.sh")],
            cwd=tmp_path,
            env=_publication_env(),
        )

        assert _run(["git", "rev-parse", "HEAD"], cwd=constraints_dir).stdout == before
