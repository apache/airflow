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
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
INSTALLER = ROOT / "scripts" / "docker" / "install_airflow_when_building_images.sh"
HELPER = INSTALLER.with_name("filter_ci_install_set.py")

# The probe's two answers, mirrored from the helper so a change to either side shows up here.
EXIT_HAS_EXCLUDED_MEMBERS = 0
EXIT_NO_EXCLUDED_MEMBERS = 10


@pytest.fixture
def image_scripts(tmp_path: Path) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    source = INSTALLER.read_text(encoding="utf-8").split("\ncommon::get_colors\n", 1)[0]
    source = source.replace('local work_dir="/tmp/ci-install-set"', f'local work_dir="{tmp_path}/work"')
    (scripts / INSTALLER.name).write_text(source, encoding="utf-8")
    (scripts / "common.sh").write_text("set -euo pipefail\n", encoding="utf-8")
    (scripts / HELPER.name).write_text(HELPER.read_text(encoding="utf-8"), encoding="utf-8")
    return scripts


def _build_environment(bin_dir: Path) -> dict[str, str]:
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "AIRFLOW_PYTHON_VERSION": "3.15.0",
        "PACKAGING_TOOL_CMD": "uv",
        "AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION": "false",
        "UPGRADE_RANDOM_INDICATOR_STRING": "",
        "COLOR_BLUE": "",
        "COLOR_RESET": "",
        "COLOR_RED": "",
        "COLOR_YELLOW": "",
    }
    environment.pop("AIRFLOW_SOURCES", None)
    # The CI image sets no VIRTUAL_ENV; inheriting the caller's would change which interpreter is used.
    environment.pop("VIRTUAL_ENV", None)
    return environment


def _write_logging_stubs(bin_dir: Path, probe_exit: int = EXIT_HAS_EXCLUDED_MEMBERS) -> None:
    """uv and python3 stubs that record their arguments; python3 also plays the helper."""
    (bin_dir / "uv").write_text(
        '#!/usr/bin/env bash\nprintf "%s\\0" uv "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n',
        encoding="utf-8",
    )
    (bin_dir / "python3").write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\0" python3 "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n'
        f'if [[ "$*" == *--probe* ]]; then exit {probe_exit}; fi\n'
        'if [[ -n "${HELPER_EXIT:-}" ]]; then exit "$HELPER_EXIT"; fi\n'
        "while [[ $# -gt 0 ]]; do\n"
        '  case "$1" in\n'
        '    --packages) printf "apache-airflow\\napache-airflow-providers-kept\\n" > "$2"; shift ;;\n'
        '    --editables) printf "%s\\n" "$PWD/kept" "$PWD/kept with space" > "$2"; shift ;;\n'
        "  esac\n"
        "  shift\n"
        "done\n",
        encoding="utf-8",
    )
    for executable in ("uv", "python3"):
        (bin_dir / executable).chmod(0o755)


def _read_commands(log: Path) -> list[list[str]]:
    return [entry.decode().split("\0") for entry in log.read_bytes().split(b"\0\0") if entry]


@pytest.mark.parametrize(
    ("probe_exit", "filtered"),
    [(EXIT_HAS_EXCLUDED_MEMBERS, True), (EXIT_NO_EXCLUDED_MEMBERS, False)],
)
@pytest.mark.parametrize("upgrade", [False, True])
def test_source_install_selects_path_from_the_probe(
    image_scripts: Path, tmp_path: Path, probe_exit: int, filtered: bool, upgrade: bool
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uv").write_text('#!/usr/bin/env bash\nprintf "uv %s\\n" "$*"\n', encoding="utf-8")
    (bin_dir / "python3").write_text(f"#!/usr/bin/env bash\nexit {probe_exit}\n", encoding="utf-8")
    for executable in ("uv", "python3"):
        (bin_dir / executable).chmod(0o755)

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; function install_workspace_without_excluded_providers() { printf "filtered\\n"; }; '
            "install_from_sources",
            "bash",
            str(image_scripts / INSTALLER.name),
        ],
        cwd=tmp_path,
        env={**_build_environment(bin_dir), "UPGRADE_RANDOM_INDICATOR_STRING": "random" if upgrade else ""},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    if filtered and upgrade:
        assert "uv lock --resolution highest" in result.stdout
        assert result.stdout.endswith("filtered\n")
        assert "uv sync" not in result.stdout
    elif filtered:
        assert result.stdout == "filtered\n"
    elif upgrade:
        assert "uv sync --all-packages --resolution highest --group ci-image" in result.stdout
        assert "uv lock" not in result.stdout
    else:
        assert "uv sync --all-packages --frozen --group ci-image" in result.stdout


def test_probe_failure_never_reads_as_nothing_excluded(image_scripts: Path, tmp_path: Path):
    """A broken probe must stop the build.

    Sharing an exit code with "nothing is excluded" would send the build down the plain
    `uv sync --all-packages` path and install exactly the set this whole path exists to avoid.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uv").write_text('#!/usr/bin/env bash\nprintf "uv %s\\n" "$*"\n', encoding="utf-8")
    (bin_dir / "python3").write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
    for executable in ("uv", "python3"):
        (bin_dir / executable).chmod(0o755)

    result = subprocess.run(
        ["bash", "-c", 'source "$1"; install_from_sources', "bash", str(image_scripts / INSTALLER.name)],
        cwd=tmp_path,
        env=_build_environment(bin_dir),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "uv sync" not in result.stdout


@pytest.mark.parametrize("helper_fails", [False, True])
def test_filtered_install_exports_only_the_kept_members(
    image_scripts: Path, tmp_path: Path, helper_fails: bool
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    _write_logging_stubs(bin_dir)

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; install_workspace_without_excluded_providers',
            "bash",
            str(image_scripts / INSTALLER.name),
        ],
        cwd=tmp_path,
        env={
            **_build_environment(bin_dir),
            "COMMAND_LOG": str(log),
            **({"HELPER_EXIT": "23"} if helper_fails else {}),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    # Read the status first: an early exit leaves no log to read.
    assert result.returncode == (23 if helper_fails else 0), result.stderr
    commands = _read_commands(log)
    helper = commands[0]
    assert helper[:4] == ["python3", str(image_scripts / HELPER.name), "--root", str(tmp_path)]
    assert helper[helper.index("--python-minor") + 1] == "3.15"
    if helper_fails:
        assert len(commands) == 1
        return

    export = commands[1]
    assert export[:3] == ["uv", "export", "--frozen"]
    # One --package per kept member is the filter: uv then exports only their closure.
    assert [export[index + 1] for index, token in enumerate(export) if token == "--package"] == [
        "apache-airflow",
        "apache-airflow-providers-kept",
    ]
    for flag in ("--group", "ci-image", "--no-emit-workspace"):
        assert flag in export

    pins_install, editable_install = commands[2], commands[3]
    assert pins_install[-2:] == ["--requirement", str(tmp_path / "work" / "requirements.txt")]
    assert editable_install[-4:] == [
        "--editable",
        str(tmp_path / "kept"),
        "--editable",
        str(tmp_path / "kept with space"),
    ]
    assert "--require-hashes" in pins_install
    assert "--exact" in pins_install
    for flag in ("--no-deps", "--no-binary", "lxml", "xmlsec"):
        assert flag in pins_install and flag in editable_install
    assert commands[4] == ["uv", "pip", "check", "--python", str(bin_dir / "python3")]


@pytest.mark.parametrize("in_virtualenv", [False, True])
def test_filtered_install_targets_the_virtualenv_when_one_is_active(
    image_scripts: Path, tmp_path: Path, in_virtualenv: bool
):
    """The PROD stage puts the base interpreter ahead of the venv on PATH, so PATH alone is not enough."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    _write_logging_stubs(bin_dir)
    virtual_env = tmp_path / "venv"
    environment = {**_build_environment(bin_dir), "COMMAND_LOG": str(log)}
    if in_virtualenv:
        environment["VIRTUAL_ENV"] = str(virtual_env)

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; install_workspace_without_excluded_providers',
            "bash",
            str(image_scripts / INSTALLER.name),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    expected = str(virtual_env / "bin" / "python") if in_virtualenv else str(bin_dir / "python3")
    installs = [entry for entry in _read_commands(log) if entry[:2] == ["uv", "pip"]]
    assert installs
    for install in installs:
        assert install[install.index("--python") + 1] == expected


@pytest.mark.parametrize("fallback_allowed", [False, True])
def test_filtered_install_relocks_only_when_fallback_is_allowed(
    image_scripts: Path, tmp_path: Path, fallback_allowed: bool
):
    """A lock missing a new dependency stops the export; 3.11-3.13 re-resolve, so this path must too."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    _write_logging_stubs(bin_dir)
    # The export fails the first time, as it does when the lock lacks a new dependency.
    (bin_dir / "uv").write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\0" uv "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n'
        'if [[ "$1" == "export" && ! -f "$ATTEMPTED" ]]; then touch "$ATTEMPTED"; exit 23; fi\n',
        encoding="utf-8",
    )
    (bin_dir / "uv").chmod(0o755)

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; install_workspace_without_excluded_providers allow-relock',
            "bash",
            str(image_scripts / INSTALLER.name),
        ],
        cwd=tmp_path,
        env={
            **_build_environment(bin_dir),
            "COMMAND_LOG": str(log),
            "ATTEMPTED": str(tmp_path / "attempted"),
            "AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION": str(fallback_allowed).lower(),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    commands = _read_commands(log)
    relocked = [entry for entry in commands if entry[:2] == ["uv", "lock"]]
    if fallback_allowed:
        assert result.returncode == 0, result.stderr
        assert relocked, commands
    else:
        # The failing command's own status must survive rather than being flattened to 1.
        assert result.returncode == 23
        assert not relocked, commands


def test_filtered_install_never_trades_hash_checking_for_a_successful_build():
    """Guard the integrity check the filtered path replaced.

    ``uv sync --frozen`` checks every download against the lock's sha256. The filtered path installs
    through ``uv pip install`` instead, which performs no such check on its own, so the check rides on
    the hashes the export already carries. Making a stubborn build succeed by exporting
    ``--no-hashes``, or by dropping ``--require-hashes`` from the install, silently removes it again
    and nothing else here would notice: the image still builds, the packages are no longer verified.
    """
    source = INSTALLER.read_text(encoding="utf-8")
    body = source.split("function prepare_filtered_install_set()", 1)[1]

    export = next(line for line in body.splitlines() if "uv export" in line)
    assert "--no-hashes" not in export, export
    assert "hash_args=(--require-hashes" in body
    # The array has to reach the install, or the exported hashes are never enforced.
    assert '"${hash_args[@]}"' in body


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.ci"])
def test_image_includes_filter_helper(dockerfile: str):
    source = (ROOT / dockerfile).read_text(encoding="utf-8")
    assert 'COPY <<"EOF" /filter_ci_install_set.py\n' in source
    copies = [
        line for line in source.replace("\\\n", "").splitlines() if line.startswith("COPY --from=scripts ")
    ]
    assert any("filter_ci_install_set.py" in line and "/scripts/docker/" in line for line in copies)
