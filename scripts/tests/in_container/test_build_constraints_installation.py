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

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import install_development_dependencies as install_dev_deps
import pytest
from install_airflow_and_providers import (
    InstallationSpec,
    _get_build_constraints_flags,
    _install_airflow_and_optionally_providers_together,
    _install_airflow_ctl_with_constraints,
    _install_only_airflow_airflow_core_task_sdk_with_constraints,
    find_installation_spec,
    resolve_build_constraints_file,
)

AIRFLOW_ROOT = Path(__file__).resolve().parents[3]
COMMON_SCRIPT = AIRFLOW_ROOT / "scripts/docker/common.sh"
INSTALL_AIRFLOW_SCRIPT = AIRFLOW_ROOT / "scripts/docker/install_airflow_when_building_images.sh"
INSTALL_ADDITIONAL_SCRIPT = AIRFLOW_ROOT / "scripts/docker/install_additional_dependencies.sh"


class TestResolveBuildConstraintsFile:
    @mock.patch("install_airflow_and_providers._download_build_constraints", autospec=True)
    def test_unset_location_is_noop(self, download):
        assert resolve_build_constraints_file(None) is None
        download.assert_not_called()

    def test_returns_explicit_local_file(self, tmp_path):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")

        assert resolve_build_constraints_file(str(build_constraints)) == build_constraints

    @pytest.mark.parametrize("path_kind", ["missing", "empty", "directory"])
    def test_rejects_invalid_explicit_local_path(self, tmp_path, path_kind):
        build_constraints = tmp_path / "build-constraints.txt"
        if path_kind == "empty":
            build_constraints.write_text("")
        elif path_kind == "directory":
            build_constraints.mkdir()

        with pytest.raises(SystemExit):
            resolve_build_constraints_file(str(build_constraints))

    @mock.patch("install_airflow_and_providers._download_build_constraints", autospec=True)
    def test_downloads_explicit_url_to_deterministic_path(self, download, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))

        def write_build_constraints(url, target):
            target.write_text("setuptools==80.0.0\n")
            return True

        download.side_effect = write_build_constraints

        result = resolve_build_constraints_file("https://example.invalid/build-constraints.txt")

        assert result == tmp_path / "build-constraints.txt"
        assert result.read_text() == "setuptools==80.0.0\n"
        download.assert_called_once_with(
            "https://example.invalid/build-constraints.txt",
            tmp_path / "build-constraints.txt",
        )

    @pytest.mark.parametrize("downloaded_content", [None, ""])
    @mock.patch("install_airflow_and_providers._download_build_constraints", autospec=True)
    def test_rejects_failed_or_empty_explicit_download(
        self, download, downloaded_content, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        target = tmp_path / "build-constraints.txt"
        target.write_text("stale==1\n")

        def download_result(url, output):
            if downloaded_content is not None:
                output.write_text(downloaded_content)
                return True
            return False

        download.side_effect = download_result

        with pytest.raises(SystemExit):
            resolve_build_constraints_file("https://example.invalid/build-constraints.txt")

        assert not target.exists()


@pytest.mark.parametrize("path_kind", ["none", "missing", "empty", "directory"])
def test_get_build_constraints_flags_skips_invalid_files(tmp_path, path_kind):
    build_constraints: Path | None = tmp_path / "build-constraints.txt"
    if path_kind == "none":
        build_constraints = None
    elif path_kind == "empty":
        build_constraints.write_text("")
    elif path_kind == "directory":
        build_constraints.mkdir()

    assert _get_build_constraints_flags(build_constraints) == []


def test_get_build_constraints_flags_uses_uv_plural_flag(tmp_path):
    build_constraints = tmp_path / "build-constraints.txt"
    build_constraints.write_text("setuptools==80.0.0\n")

    assert _get_build_constraints_flags(build_constraints) == [
        "--build-constraints",
        str(build_constraints),
    ]


def _installation_spec(
    build_constraints_file: Path | None,
    *,
    airflow_constraints_location: str | None = "runtime-constraints.txt",
    airflow_ctl_constraints_location: str | None = "ctl-constraints.txt",
    provider_constraints_location: str | None = "provider-constraints.txt",
) -> InstallationSpec:
    return InstallationSpec(
        airflow_distribution="apache-airflow==3.2.0",
        airflow_core_distribution="apache-airflow-core==3.2.0",
        airflow_constraints_location=airflow_constraints_location,
        airflow_task_sdk_distribution="apache-airflow-task-sdk==1.2.0",
        airflow_ctl_distribution="apache-airflow-ctl==1.2.0",
        airflow_ctl_constraints_location=airflow_ctl_constraints_location,
        build_constraints_file=build_constraints_file,
        compile_ui_assets=False,
        mount_ui_dist=False,
        provider_distributions=["apache-airflow-providers-standard==1.0.0"],
        provider_constraints_location=provider_constraints_location,
    )


@mock.patch("install_airflow_and_providers.resolve_build_constraints_file", autospec=True)
def test_find_installation_spec_resolves_only_explicit_location(resolve, tmp_path):
    build_constraints = tmp_path / "build-constraints.txt"
    resolve.return_value = build_constraints

    result = find_installation_spec(
        airflow_constraints_mode="constraints",
        airflow_constraints_location=None,
        airflow_constraints_reference="",
        airflow_extras="",
        build_constraints_location="https://example.invalid/build-constraints.txt",
        install_airflow_with_constraints=False,
        default_constraints_branch="constraints-main",
        github_repository="apache/airflow",
        install_selected_providers="",
        distribution_format="wheel",
        providers_constraints_mode="constraints",
        providers_constraints_location=None,
        providers_constraints_reference="",
        providers_skip_constraints=True,
        python_version="3.12",
        use_airflow_version="",
        use_distributions_from_dist=False,
        mount_ui_dist=False,
    )

    assert result.build_constraints_file == build_constraints
    resolve.assert_called_once_with("https://example.invalid/build-constraints.txt")


class TestPythonInstallCommandAssembly:
    @mock.patch("install_airflow_and_providers.run_command", autospec=True)
    def test_provider_install_includes_build_constraints_without_runtime_constraints(
        self, run_command, tmp_path
    ):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        run_command.return_value = SimpleNamespace(returncode=0)

        _install_airflow_and_optionally_providers_together(
            _installation_spec(
                build_constraints,
                provider_constraints_location=None,
            ),
            github_actions=False,
        )

        command = run_command.call_args.args[0]
        assert command[command.index("--build-constraints") + 1] == str(build_constraints)
        assert "--constraint" not in command

    @mock.patch("install_airflow_and_providers.run_command", autospec=True)
    def test_provider_fallback_removes_both_constraints(self, run_command, tmp_path):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        run_command.side_effect = [
            SimpleNamespace(returncode=1),
            SimpleNamespace(returncode=0),
        ]

        _install_airflow_and_optionally_providers_together(
            _installation_spec(build_constraints),
            github_actions=False,
        )

        constrained, fallback = [call.args[0] for call in run_command.call_args_list]
        assert "--constraint" in constrained
        assert "--build-constraints" in constrained
        assert "--constraint" not in fallback
        assert "--build-constraints" not in fallback

    @mock.patch("install_airflow_and_providers.run_command", autospec=True)
    def test_ctl_install_includes_build_constraints_without_runtime_constraints(self, run_command, tmp_path):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")

        _install_airflow_ctl_with_constraints(
            _installation_spec(
                build_constraints,
                airflow_ctl_constraints_location=None,
            ),
            github_actions=False,
        )

        command = run_command.call_args.args[0]
        assert command[command.index("--build-constraints") + 1] == str(build_constraints)
        assert "--constraint" not in command

    @mock.patch("install_airflow_and_providers.run_command", autospec=True)
    def test_ctl_fallback_removes_both_constraints(self, run_command, tmp_path):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        run_command.side_effect = [
            SimpleNamespace(returncode=1),
            SimpleNamespace(returncode=0),
        ]

        _install_airflow_ctl_with_constraints(
            _installation_spec(build_constraints),
            github_actions=False,
        )

        constrained, fallback = [call.args[0] for call in run_command.call_args_list]
        assert "--constraint" in constrained
        assert "--build-constraints" in constrained
        assert "--constraint" not in fallback
        assert "--build-constraints" not in fallback

    @mock.patch("install_airflow_and_providers.run_command", autospec=True)
    def test_separate_airflow_fallback_removes_both_constraints(self, run_command, tmp_path):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        run_command.side_effect = [
            SimpleNamespace(returncode=1),
            SimpleNamespace(returncode=0),
        ]

        _install_only_airflow_airflow_core_task_sdk_with_constraints(
            _installation_spec(build_constraints),
            github_actions=False,
        )

        constrained, fallback = [call.args[0] for call in run_command.call_args_list]
        assert "--constraint" in constrained
        assert "--build-constraints" in constrained
        assert "--constraint" not in fallback
        assert "--build-constraints" not in fallback


class TestInstallDevelopmentDependencies:
    def _prepare_airflow_root(self, tmp_path):
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
        monkeypatch,
        airflow_root,
        constraint,
        build_constraints_location,
        returncodes=(0,),
    ):
        calls = []
        returncode_iter = iter(returncodes)

        def run_command(command, **kwargs):
            calls.append((command, kwargs))
            return SimpleNamespace(returncode=next(returncode_iter))

        monkeypatch.setattr(install_dev_deps, "AIRFLOW_ROOT_PATH", airflow_root)
        monkeypatch.setattr(install_dev_deps, "run_command", run_command)
        callback = install_dev_deps.install_development_dependencies.callback
        assert callback
        callback(
            constraint=str(constraint),
            build_constraints_location=build_constraints_location,
            github_actions=False,
        )
        return calls

    def test_adds_explicit_build_constraints(self, tmp_path, monkeypatch):
        airflow_root = self._prepare_airflow_root(tmp_path)
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")

        calls = self._run_install(
            monkeypatch,
            airflow_root,
            tmp_path / "constraints.txt",
            str(build_constraints),
        )

        command = calls[0][0]
        assert "pendulum>=3" in command
        assert "pytest>=8" in command
        assert "skip-me>=1" not in command
        assert "boto3>=1" in command
        assert command[command.index("--build-constraints") + 1] == str(build_constraints)

    def test_unset_location_preserves_existing_command(self, tmp_path, monkeypatch):
        airflow_root = self._prepare_airflow_root(tmp_path)
        constraint = tmp_path / "constraints.txt"

        calls = self._run_install(monkeypatch, airflow_root, constraint, None)

        command = calls[0][0]
        assert "--build-constraints" not in command
        assert command[-2:] == ["--constraints", str(constraint)]

    def test_fallback_removes_both_constraints(self, tmp_path, monkeypatch):
        airflow_root = self._prepare_airflow_root(tmp_path)
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")

        calls = self._run_install(
            monkeypatch,
            airflow_root,
            tmp_path / "constraints.txt",
            str(build_constraints),
            returncodes=(1, 0),
        )

        constrained, fallback = [command for command, _ in calls]
        assert "--constraints" in constrained
        assert "--build-constraints" in constrained
        assert "--constraints" not in fallback
        assert "--build-constraints" not in fallback


def _run_bash(script: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", script],
        cwd=AIRFLOW_ROOT,
        env={**os.environ, **env},
        text=True,
        capture_output=True,
        check=False,
    )


class TestShellBuildConstraints:
    def test_unset_location_does_not_use_stale_file_or_network(self, tmp_path):
        stale = tmp_path / "build-constraints.txt"
        stale.write_text("stale==1\n")
        curl_log = tmp_path / "curl.log"
        script = f"""
source "{COMMON_SCRIPT}"
common::get_colors
PACKAGING_TOOL=uv
curl() {{ echo called >> "$CURL_LOG"; return 1; }}
unset AIRFLOW_BUILD_CONSTRAINTS_LOCATION
common::resolve_build_constraints
printf '%s\n' "${{#BUILD_CONSTRAINTS_INSTALL_FLAGS[@]}}"
"""

        result = _run_bash(
            script,
            env={"HOME": str(tmp_path), "CURL_LOG": str(curl_log)},
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.rstrip().endswith("0")
        assert not curl_log.exists()
        assert stale.read_text() == "stale==1\n"

    @pytest.mark.parametrize(
        ("packaging_tool", "expected_flag"),
        [("uv", "--build-constraints"), ("pip", "--build-constraint")],
    )
    def test_local_file_uses_tool_specific_flag(self, tmp_path, packaging_tool, expected_flag):
        source_dir = tmp_path / "path with spaces"
        source_dir.mkdir()
        source = source_dir / "build-constraints.txt"
        source.write_text("setuptools==80.0.0\n")
        script = f"""
source "{COMMON_SCRIPT}"
common::get_colors
PACKAGING_TOOL="$PACKAGING_TOOL"
common::resolve_build_constraints
printf '<%s>\n' "${{BUILD_CONSTRAINTS_INSTALL_FLAGS[@]}}"
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "PACKAGING_TOOL": packaging_tool,
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": str(source),
            },
        )

        assert result.returncode == 0, result.stderr
        assert f"<{expected_flag}>" in result.stdout
        assert f"<{tmp_path / 'build-constraints.txt'}>" in result.stdout

    def test_explicit_url_download_is_used(self, tmp_path):
        source = tmp_path / "download-source.txt"
        source.write_text("setuptools==80.0.0\n")
        script = f"""
source "{COMMON_SCRIPT}"
common::get_colors
PACKAGING_TOOL=uv
curl() {{
    local output
    while [[ $# -gt 0 ]]; do
        if [[ $1 == "-o" ]]; then output=$2; shift 2; else shift; fi
    done
    cp "$DOWNLOAD_SOURCE" "$output"
}}
common::resolve_build_constraints
printf '<%s>\n' "${{BUILD_CONSTRAINTS_INSTALL_FLAGS[@]}}"
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "DOWNLOAD_SOURCE": str(source),
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": "https://example.invalid/build-constraints.txt",
            },
        )

        assert result.returncode == 0, result.stderr
        assert "<--build-constraints>" in result.stdout
        assert (tmp_path / "build-constraints.txt").read_text() == "setuptools==80.0.0\n"

    @pytest.mark.parametrize("path_kind", ["missing", "empty", "directory"])
    def test_invalid_explicit_local_path_fails(self, tmp_path, path_kind):
        source = tmp_path / "build-constraints-source.txt"
        if path_kind == "empty":
            source.write_text("")
        elif path_kind == "directory":
            source.mkdir()
        script = f"""
source "{COMMON_SCRIPT}"
common::get_colors
PACKAGING_TOOL=uv
common::resolve_build_constraints
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": str(source),
            },
        )

        assert result.returncode != 0
        assert "non-empty file" in result.stdout

    def test_failed_url_does_not_leave_stale_target(self, tmp_path):
        target = tmp_path / "build-constraints.txt"
        target.write_text("stale==1\n")
        script = f"""
source "{COMMON_SCRIPT}"
common::get_colors
PACKAGING_TOOL=uv
curl() {{ return 22; }}
common::resolve_build_constraints
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": "https://example.invalid/build-constraints.txt",
            },
        )

        assert result.returncode != 0
        assert not target.exists()

    def test_runtime_constraints_fallback_removes_build_constraints(self, tmp_path):
        build_constraints = tmp_path / "explicit-build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        command_log = tmp_path / "commands.log"
        failed_once = tmp_path / "failed"
        script = f"""
source "{COMMON_SCRIPT}"
source <(sed -n '/^function install_from_external_spec()/,/^}}$/p' "{INSTALL_AIRFLOW_SCRIPT}")
common::get_colors
record_command() {{
    printf '%s\n' "$*" >> "$COMMAND_LOG"
    if [[ ! -f "$FAILED_ONCE" ]]; then touch "$FAILED_ONCE"; return 1; fi
    return 0
}}
PACKAGING_TOOL=uv
PACKAGING_TOOL_CMD=record_command
EXTRA_INSTALL_FLAGS=""
UPGRADE_TO_HIGHEST_RESOLUTION=""
UPGRADE_IF_NEEDED=""
ADDITIONAL_PIP_INSTALL_FLAGS=""
EXTRA_UNINSTALL_FLAGS=""
AIRFLOW_INSTALLATION_METHOD=apache-airflow
AIRFLOW_EXTRAS=""
AIRFLOW_VERSION_SPECIFICATION=""
AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION=true
UPGRADE_RANDOM_INDICATOR_STRING=""
install_from_external_spec
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "COMMAND_LOG": str(command_log),
                "FAILED_ONCE": str(failed_once),
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": str(build_constraints),
            },
        )

        assert result.returncode == 0, result.stderr
        constrained, fallback = command_log.read_text().splitlines()
        assert "--constraint" in constrained
        assert "--build-constraints" in constrained
        assert "--constraint" not in fallback
        assert "--build-constraints" not in fallback

    def test_source_install_does_not_receive_build_constraints(self, tmp_path):
        build_constraints = tmp_path / "build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        command_log = tmp_path / "commands.log"
        script = f"""
source "{COMMON_SCRIPT}"
source <(sed -n '/^function install_from_sources()/,/^}}$/p' "{INSTALL_AIRFLOW_SCRIPT}")
common::get_colors
uv() {{ printf '%s\n' "$*" >> "$COMMAND_LOG"; }}
PACKAGING_TOOL_CMD=uv
VIRTUAL_ENV=""
UPGRADE_RANDOM_INDICATOR_STRING=upgrade
install_from_sources
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "COMMAND_LOG": str(command_log),
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": str(build_constraints),
            },
        )

        assert result.returncode == 0, result.stderr
        command = command_log.read_text()
        assert "sync" in command
        assert "--build-constraints" not in command
        assert "--build-constraint" not in command

    def test_additional_dependencies_receive_explicit_build_constraints(self, tmp_path):
        build_constraints = tmp_path / "explicit-build-constraints.txt"
        build_constraints.write_text("setuptools==80.0.0\n")
        command_log = tmp_path / "commands.log"
        script = f"""
source "{COMMON_SCRIPT}"
source <(sed -n '/^function install_additional_dependencies()/,/^}}$/p' "{INSTALL_ADDITIONAL_SCRIPT}")
common::get_colors
record_command() {{ printf '%s\n' "$*" >> "$COMMAND_LOG"; }}
common::install_packaging_tools() {{ :; }}
pip() {{ :; }}
PACKAGING_TOOL=uv
PACKAGING_TOOL_CMD=record_command
EXTRA_INSTALL_FLAGS=""
UPGRADE_TO_HIGHEST_RESOLUTION=""
UPGRADE_IF_NEEDED=""
ADDITIONAL_PIP_INSTALL_FLAGS=""
ADDITIONAL_PYTHON_DEPS="example-package"
UPGRADE_RANDOM_INDICATOR_STRING=""
common::resolve_build_constraints
install_additional_dependencies
"""

        result = _run_bash(
            script,
            env={
                "HOME": str(tmp_path),
                "COMMAND_LOG": str(command_log),
                "AIRFLOW_BUILD_CONSTRAINTS_LOCATION": str(build_constraints),
            },
        )

        assert result.returncode == 0, result.stderr
        command = command_log.read_text()
        assert "--build-constraints" in command
        assert "example-package" in command


_WHEEL_HELPER_SOURCE = r"""
from __future__ import annotations

import base64
import hashlib
import re
import zipfile
from pathlib import Path


def _record_line(arcname, payload):
    digest = hashlib.sha256(payload).digest()
    encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return f"{arcname},sha256={encoded},{len(payload)}"


def write_wheel(out_dir, *, distribution, version):
    escaped = re.sub(r"[^A-Za-z0-9.]+", "_", distribution)
    dist_info = f"{escaped}-{version}.dist-info"
    wheel_path = Path(out_dir) / f"{escaped}-{version}-py3-none-any.whl"
    members = [
        (
            f"{dist_info}/METADATA",
            f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\n".encode(),
        ),
        (
            f"{dist_info}/WHEEL",
            b"Wheel-Version: 1.0\nGenerator: airflow-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        ),
    ]
    record = [_record_line(name, payload) for name, payload in members]
    record.append(f"{dist_info}/RECORD,,")
    members.append((f"{dist_info}/RECORD", ("\n".join(record) + "\n").encode()))
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return wheel_path
"""

_BACKEND_SOURCE = r"""
from __future__ import annotations

import sys
from importlib.metadata import version
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _wheel_helper import write_wheel


def _assert_marker_version():
    actual = version("airflow-build-constraint-marker")
    if actual != "2.0.0":
        raise RuntimeError(f"Expected marker 2.0.0, got {actual}")


def get_requires_for_build_wheel(config_settings=None):
    return []


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    _assert_marker_version()
    target = Path(metadata_directory) / "airflow_build_constraint_subject-0.1.0.dist-info"
    target.mkdir(parents=True, exist_ok=True)
    (target / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: airflow-build-constraint-subject\nVersion: 0.1.0\n"
    )
    (target / "WHEEL").write_text(
        "Wheel-Version: 1.0\nGenerator: airflow-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    )
    return target.name


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _assert_marker_version()
    return write_wheel(
        wheel_directory,
        distribution="airflow-build-constraint-subject",
        version="0.1.0",
    ).name
"""

_PYPROJECT_TOML = """[build-system]
requires = ["airflow-build-constraint-marker>=2"]
build-backend = "backend"
backend-path = ["."]
"""


def _write_wheel(out_dir, *, distribution, version):
    escaped = re.sub(r"[^A-Za-z0-9.]+", "_", distribution)
    dist_info = f"{escaped}-{version}.dist-info"
    wheel_path = Path(out_dir) / f"{escaped}-{version}-py3-none-any.whl"
    members = [
        (
            f"{dist_info}/METADATA",
            f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\n".encode(),
        ),
        (
            f"{dist_info}/WHEEL",
            b"Wheel-Version: 1.0\nGenerator: airflow-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        ),
    ]
    record = []
    for name, payload in members:
        digest = hashlib.sha256(payload).digest()
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        record.append(f"{name},sha256={encoded},{len(payload)}")
    record.append(f"{dist_info}/RECORD,,")
    members.append((f"{dist_info}/RECORD", ("\n".join(record) + "\n").encode()))
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return wheel_path


def _pip_supports_build_constraint():
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0 and "--build-constraint" in result.stdout


@pytest.mark.skipif(
    not _pip_supports_build_constraint(),
    reason="pip does not support --build-constraint",
)
def test_pip_build_constraint_controls_build_isolation(tmp_path):
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    _write_wheel(
        wheelhouse,
        distribution="airflow-build-constraint-marker",
        version="1.0.0",
    )
    _write_wheel(
        wheelhouse,
        distribution="airflow-build-constraint-marker",
        version="2.0.0",
    )
    subject = tmp_path / "subject"
    subject.mkdir()
    (subject / "_wheel_helper.py").write_text(_WHEEL_HELPER_SOURCE)
    (subject / "backend.py").write_text(_BACKEND_SOURCE)
    (subject / "pyproject.toml").write_text(_PYPROJECT_TOML)
    good_constraints = tmp_path / "good.txt"
    good_constraints.write_text("airflow-build-constraint-marker==2.0.0\n")
    bad_constraints = tmp_path / "bad.txt"
    bad_constraints.write_text("airflow-build-constraint-marker==1.0.0\n")
    base_command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--isolated",
        "--disable-pip-version-check",
        "--no-index",
        "--find-links",
        str(wheelhouse),
    ]

    good = subprocess.run(
        [
            *base_command,
            "--target",
            str(tmp_path / "target-good"),
            "--build-constraint",
            str(good_constraints),
            str(subject),
        ],
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    assert good.returncode == 0, f"stdout:\n{good.stdout}\nstderr:\n{good.stderr}"

    bad = subprocess.run(
        [
            *base_command,
            "--target",
            str(tmp_path / "target-bad"),
            "--build-constraint",
            str(bad_constraints),
            str(subject),
        ],
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    assert bad.returncode != 0
    assert "airflow-build-constraint-marker" in (bad.stdout + bad.stderr).lower()
