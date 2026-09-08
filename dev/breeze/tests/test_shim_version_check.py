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
from pathlib import Path
from unittest import mock

import pytest

from airflow_breeze.utils import path_utils
from airflow_breeze.utils.path_utils import (
    BREEZE_SHIM_MARKER,
    _parse_shim_version,
    detect_legacy_global_breeze_install,
    get_expected_shim_version,
    has_uv_breeze_tool_dir,
    warn_if_breeze_launcher_outdated,
    warn_if_shim_outdated,
)

ACTUAL_AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()


def _managed_shim(version_line: str | None) -> str:
    lines = ["#!/usr/bin/env bash", BREEZE_SHIM_MARKER]
    if version_line is not None:
        lines.append(version_line)
    lines.append("set -e")
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize(
    ("version_line", "expected"),
    [
        ("# breeze-shim-version: 1", 1),
        ("# breeze-shim-version: 42", 42),
        ("   # breeze-shim-version: 3   ", 3),
        ("# breeze-shim-version: not-a-number", None),
        ("# something-else: 1", None),
        (None, None),
    ],
)
def test_parse_shim_version(version_line, expected):
    assert _parse_shim_version(_managed_shim(version_line)) == expected


def test_get_expected_shim_version_reads_real_setup_script():
    # The real setup_breeze in the sources is the source of truth — keep this test in sync
    # with the SHIM_VERSION it declares.
    assert get_expected_shim_version(ACTUAL_AIRFLOW_SOURCES) == 1


def test_get_expected_shim_version_from_fake_sources(tmp_path):
    setup_script = tmp_path / "scripts" / "tools" / "setup_breeze"
    setup_script.parent.mkdir(parents=True)
    setup_script.write_text('SHIM_DIR="x"\nSHIM_VERSION="7"\n')
    assert get_expected_shim_version(tmp_path) == 7


def test_get_expected_shim_version_missing_script(tmp_path):
    assert get_expected_shim_version(tmp_path) is None


def _fake_sources_with_version(tmp_path: Path, version: int) -> Path:
    setup_script = tmp_path / "scripts" / "tools" / "setup_breeze"
    setup_script.parent.mkdir(parents=True)
    setup_script.write_text(f'SHIM_VERSION="{version}"\n')
    return tmp_path


def test_warn_if_shim_outdated_no_shim_file(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", tmp_path / "breeze")
    sources = _fake_sources_with_version(tmp_path, 1)
    assert warn_if_shim_outdated(sources) is False
    assert capsys.readouterr().out == ""


def test_warn_if_shim_outdated_foreign_script(tmp_path, monkeypatch, capsys):
    shim = tmp_path / "breeze"
    shim.write_text("#!/usr/bin/env bash\n# some user's own script\n")
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", shim)
    sources = _fake_sources_with_version(tmp_path, 1)
    assert warn_if_shim_outdated(sources) is False
    assert capsys.readouterr().out == ""


def test_warn_if_shim_outdated_up_to_date(tmp_path, monkeypatch, capsys):
    shim = tmp_path / "breeze"
    shim.write_text(_managed_shim("# breeze-shim-version: 1"))
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", shim)
    sources = _fake_sources_with_version(tmp_path, 1)
    assert warn_if_shim_outdated(sources) is False
    assert capsys.readouterr().out == ""


def test_warn_if_shim_outdated_newer_installed(tmp_path, monkeypatch, capsys):
    shim = tmp_path / "breeze"
    shim.write_text(_managed_shim("# breeze-shim-version: 5"))
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", shim)
    sources = _fake_sources_with_version(tmp_path, 2)
    assert warn_if_shim_outdated(sources) is False
    assert capsys.readouterr().out == ""


def test_warn_if_shim_outdated_older_installed(tmp_path, monkeypatch, capsys):
    shim = tmp_path / "breeze"
    shim.write_text(_managed_shim("# breeze-shim-version: 1"))
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", shim)
    sources = _fake_sources_with_version(tmp_path, 2)
    assert warn_if_shim_outdated(sources) is True
    output = capsys.readouterr().out
    assert "out of date" in output
    assert "setup_breeze" in output


def test_warn_if_shim_outdated_pre_versioning_shim(tmp_path, monkeypatch, capsys):
    # A managed shim with no version marker predates versioning — treat it as outdated.
    shim = tmp_path / "breeze"
    shim.write_text(_managed_shim(None))
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", shim)
    sources = _fake_sources_with_version(tmp_path, 1)
    assert warn_if_shim_outdated(sources) is True
    output = capsys.readouterr().out
    assert "out of date" in output
    assert "pre-versioning" in output


def _run_result(stdout: str, returncode: int = 0):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr="")


def test_detect_legacy_global_breeze_install_uv():
    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run") as run:
        run.return_value = _run_result("apache-airflow-breeze v3.0.0\n")
        assert detect_legacy_global_breeze_install() == "uv"


def test_detect_legacy_global_breeze_install_pipx():
    def fake_run(cmd, *args, **kwargs):
        if cmd[:2] == ["uv", "tool"]:
            return _run_result("some-other-tool\n")
        return _run_result("apache-airflow-breeze\n")

    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run", side_effect=fake_run):
        assert detect_legacy_global_breeze_install() == "pipx"


def test_detect_legacy_global_breeze_install_none():
    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run") as run:
        run.return_value = _run_result("")
        assert detect_legacy_global_breeze_install() is None


def test_detect_legacy_global_breeze_install_tools_missing():
    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run", side_effect=FileNotFoundError):
        assert detect_legacy_global_breeze_install() is None


def test_launcher_check_prefers_shim_version(tmp_path, monkeypatch, capsys):
    # When our managed shim is installed, the launcher check does the version check and never
    # shells out to detect a legacy install.
    shim = tmp_path / "breeze"
    shim.write_text(_managed_shim("# breeze-shim-version: 1"))
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", shim)
    sources = _fake_sources_with_version(tmp_path, 2)
    with mock.patch("airflow_breeze.utils.path_utils.detect_legacy_global_breeze_install") as detect:
        assert warn_if_breeze_launcher_outdated(sources) is True
        detect.assert_not_called()
    output = capsys.readouterr().out
    assert "out of date" in output


@pytest.mark.parametrize(
    ("legacy", "uninstall_cmd"),
    [
        ("uv", "uv tool uninstall apache-airflow-breeze"),
        ("pipx", "pipx uninstall apache-airflow-breeze"),
    ],
)
def test_launcher_check_warns_on_legacy_install(tmp_path, monkeypatch, capsys, legacy, uninstall_cmd):
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", tmp_path / "breeze")  # no shim file
    sources = _fake_sources_with_version(tmp_path, 1)
    with mock.patch(
        "airflow_breeze.utils.path_utils.detect_legacy_global_breeze_install", return_value=legacy
    ):
        assert warn_if_breeze_launcher_outdated(sources) is True
    output = capsys.readouterr().out
    assert "legacy global" in output
    assert uninstall_cmd in output
    assert "setup_breeze" in output


def test_launcher_check_silent_without_shim_or_legacy(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(path_utils, "BREEZE_SHIM_PATH", tmp_path / "breeze")  # no shim file
    sources = _fake_sources_with_version(tmp_path, 1)
    with mock.patch("airflow_breeze.utils.path_utils.detect_legacy_global_breeze_install", return_value=None):
        assert warn_if_breeze_launcher_outdated(sources) is False
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    ("tool_dir_stdout", "returncode", "create_tool_dir", "expected"),
    [
        pytest.param("{tool_dir}\n", 0, True, True, id="breeze-tool-dir-present"),
        pytest.param("{tool_dir}\n", 0, False, False, id="no-breeze-tool-dir"),
        pytest.param("{tool_dir}\n", 2, True, False, id="uv-tool-dir-failed"),
        pytest.param("\n", 0, True, False, id="uv-reported-no-tool-dir"),
    ],
)
def test_has_uv_breeze_tool_dir(tmp_path, tool_dir_stdout, returncode, create_tool_dir, expected):
    if create_tool_dir:
        (tmp_path / "apache-airflow-breeze").mkdir()
    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run") as run:
        run.return_value = _run_result(tool_dir_stdout.format(tool_dir=tmp_path), returncode=returncode)
        assert has_uv_breeze_tool_dir() is expected


def test_has_uv_breeze_tool_dir_without_uv():
    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run", side_effect=FileNotFoundError):
        assert has_uv_breeze_tool_dir() is False


def test_detect_legacy_global_breeze_install_corrupted_uv_tool(tmp_path):
    (tmp_path / "apache-airflow-breeze").mkdir()

    def fake_run(cmd, *args, **kwargs):
        if cmd[:3] == ["uv", "tool", "dir"]:
            return _run_result(f"{tmp_path}\n")
        return _run_result("prek v0.3.6\n- prek\n")

    with mock.patch("airflow_breeze.utils.path_utils.subprocess.run", side_effect=fake_run):
        assert detect_legacy_global_breeze_install() == "uv"


SETUP_BREEZE_SCRIPT = ACTUAL_AIRFLOW_SOURCES / "scripts" / "tools" / "setup_breeze"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(0o755)


def _sandbox_env(
    tmp_path: Path, *, breeze_tool_installed: bool, entry_point_installed: bool = False
) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    tool_dir = tmp_path / "uv-tools"
    tool_dir.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    breeze_tool = tool_dir / "apache-airflow-breeze"
    if breeze_tool_installed:
        (breeze_tool / "bin").mkdir(parents=True)
        _write_executable(breeze_tool / "bin" / "breeze", "#!/usr/bin/env bash\nexit 0\n")
    if entry_point_installed:
        shim_path = home / ".local" / "bin" / "breeze"
        shim_path.parent.mkdir(parents=True)
        shim_path.symlink_to(breeze_tool / "bin" / "breeze")
    # Reproduces uv's handling of a tool whose environment is corrupted: the warning goes to
    # stderr and the tool is left out of the listing that setup_breeze greps.
    _write_executable(
        bin_dir / "uv",
        "#!/usr/bin/env bash\n"
        'if [[ "$1 $2" == "tool list" ]]; then\n'
        '  echo "warning: Ignoring malformed tool apache-airflow-breeze" >&2\n'
        '  echo "prek v0.3.6"\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "$1 $2" == "tool dir" ]]; then\n'
        f'  echo "{tool_dir}"\n'
        "  exit 0\n"
        "fi\n"
        "exit 0\n",
    )
    _write_executable(bin_dir / "pipx", "#!/usr/bin/env bash\nexit 0\n")
    return {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(home), "ANSWER": "y"}


def _run_setup_breeze(env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run([str(SETUP_BREEZE_SCRIPT)], env=env, text=True, capture_output=True, check=False)


@pytest.mark.parametrize("entry_point_installed", [True, False])
def test_setup_breeze_reports_uv_tool_install_missing_from_listing(tmp_path, entry_point_installed):
    result = _run_setup_breeze(
        _sandbox_env(tmp_path, breeze_tool_installed=True, entry_point_installed=entry_point_installed)
    )
    assert result.returncode == 1
    assert "A legacy global breeze install was detected." in result.stdout
    assert "uv tool uninstall apache-airflow-breeze" in result.stdout


def test_setup_breeze_replaces_entry_point_left_by_uv_tool_uninstall(tmp_path):
    # An entry point with no tool directory under it is the dangling symlink uv leaves behind.
    env = _sandbox_env(tmp_path, breeze_tool_installed=False, entry_point_installed=True)
    result = _run_setup_breeze(env)
    assert result.returncode == 0, result.stdout + result.stderr
    shim = Path(env["HOME"]) / ".local" / "bin" / "breeze"
    assert not shim.is_symlink()
    assert BREEZE_SHIM_MARKER in shim.read_text()


def test_setup_breeze_refuses_a_broken_symlink_uv_does_not_own(tmp_path):
    env = _sandbox_env(tmp_path, breeze_tool_installed=False, entry_point_installed=True)
    shim = Path(env["HOME"]) / ".local" / "bin" / "breeze"
    shim.unlink()
    shim.symlink_to(tmp_path / "elsewhere" / "breeze")
    result = _run_setup_breeze(env)
    assert result.returncode == 1
    assert "is a broken symlink to" in result.stdout
    assert shim.is_symlink()


def test_setup_breeze_installs_shim_when_uv_owns_no_breeze_tool(tmp_path):
    env = _sandbox_env(tmp_path, breeze_tool_installed=False)
    result = _run_setup_breeze(env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "A legacy global breeze install was detected." not in result.stdout
    assert BREEZE_SHIM_MARKER in (Path(env["HOME"]) / ".local" / "bin" / "breeze").read_text()
