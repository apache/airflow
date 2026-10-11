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
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
INSTALLER = ROOT / "scripts" / "docker" / "install_airflow_when_building_images.sh"
HELPER = INSTALLER.with_name("filter_ci_install_set.py")

# The probe's two answers as the stubs below give them. Only the test that runs the real helper ties
# them to the helper's own codes.
EXIT_HAS_EXCLUDED_MEMBERS = 0
EXIT_NO_EXCLUDED_MEMBERS = 10


@pytest.fixture
def image_scripts(tmp_path: Path) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    source = INSTALLER.read_text(encoding="utf-8")
    work_dir_line = 'local work_dir="/tmp/ci-install-set"'
    # A replacement that silently matched nothing would send every test to one shared directory.
    assert source.count(work_dir_line) == 1
    assert source.count("\ncommon::get_colors\n") == 1
    source = source.split("\ncommon::get_colors\n", 1)[0]
    source = source.replace(work_dir_line, f'local work_dir="{tmp_path}/work"')
    (scripts / INSTALLER.name).write_text(source, encoding="utf-8")
    (scripts / "common.sh").write_text("set -euo pipefail\n", encoding="utf-8")
    (scripts / HELPER.name).write_text(HELPER.read_text(encoding="utf-8"), encoding="utf-8")
    return scripts


def _build_environment(bin_dir: Path) -> dict[str, str]:
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "PACKAGING_TOOL_CMD": "uv",
        "AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION": "false",
        "UPGRADE_RANDOM_INDICATOR_STRING": "",
        "COLOR_BLUE": "",
        "COLOR_RESET": "",
        "COLOR_RED": "",
        "COLOR_YELLOW": "",
    }
    environment.pop("AIRFLOW_SOURCES", None)
    # The installer must not read the build argument; leaving it unset makes any read fail under `set -u`.
    environment.pop("AIRFLOW_PYTHON_VERSION", None)
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
            'source "$1"; function install_workspace_without_excluded_providers() { '
            'printf "filtered%s\\n" "${1:+ $1}"; }; '
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
        # The upgrade build has just re-locked, so it installs that lock without checking it.
        assert result.stdout.endswith("filtered\n")
        assert "uv sync" not in result.stdout
    elif filtered:
        assert result.stdout == "filtered allow-relock\n"
    elif upgrade:
        assert "uv sync --all-packages --resolution highest --group ci-image" in result.stdout
        assert "uv lock" not in result.stdout
    else:
        assert "uv sync --all-packages --frozen --group ci-image" in result.stdout


@pytest.mark.parametrize(
    ("probe_exit", "skipped"),
    [(EXIT_HAS_EXCLUDED_MEMBERS, True), (EXIT_NO_EXCLUDED_MEMBERS, False)],
)
def test_locked_third_party_install_skips_when_providers_are_excluded(
    image_scripts: Path, tmp_path: Path, probe_exit: int, skipped: bool
):
    """The warm-up layer has no workspace sources, so neither the plain sync nor the filtered path fits."""
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
        env={**_build_environment(bin_dir), "INSTALL_LOCKED_THIRD_PARTY_ONLY": "true"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "filtered" not in result.stdout
    if skipped:
        assert "uv " not in result.stdout
    else:
        assert "uv sync --all-packages --frozen --group ci-image --no-install-workspace" in result.stdout


@pytest.mark.parametrize("build_argument", [None, "", "3.14", "3.14.2"])
@pytest.mark.parametrize(
    ("probe_exit", "filtered"),
    [(EXIT_HAS_EXCLUDED_MEMBERS, True), (EXIT_NO_EXCLUDED_MEMBERS, False)],
)
def test_probe_leaves_the_python_version_to_the_interpreter(
    image_scripts: Path, tmp_path: Path, build_argument: str | None, probe_exit: int, filtered: bool
):
    """An empty or minor-only build argument must not decide which path the build takes."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    _write_logging_stubs(bin_dir, probe_exit=probe_exit)
    environment = {**_build_environment(bin_dir), "COMMAND_LOG": str(log)}
    if build_argument is not None:
        environment["AIRFLOW_PYTHON_VERSION"] = build_argument

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; if has_providers_excluded_for_python; then echo filtered; else echo plain; fi',
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
    assert result.stdout == ("filtered\n" if filtered else "plain\n")
    (probe,) = _read_commands(log)
    assert probe == ["python3", str(image_scripts / HELPER.name), "--root", str(tmp_path), "--probe"]


def test_probe_failure_never_reads_as_nothing_excluded(image_scripts: Path, tmp_path: Path):
    """A broken probe must stop the build.

    Sharing an exit code with "nothing is excluded" would send the build down the plain
    `uv sync --all-packages` path and install exactly the set this whole path exists to avoid.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uv").write_text('#!/usr/bin/env bash\nprintf "uv %s\\n" "$*"\n', encoding="utf-8")
    (bin_dir / "python3").write_text("#!/usr/bin/env bash\nexit 23\n", encoding="utf-8")
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

    assert result.returncode == 23
    assert "Could not determine excluded providers (exit 23)" in result.stdout
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
    # The helper takes the Python version from the interpreter running it, never from the build argument.
    assert not [token for token in helper if token.startswith("--python")]
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
    assert "--no-hashes" not in export

    pins_install, editable_install = commands[2], commands[3]
    assert pins_install[-2:] == ["--requirement", export[export.index("--output-file") + 1]]
    assert "--no-config" in pins_install
    assert pins_install[pins_install.index("--no-build-isolation-package") + 1] == "sphinx-redoc"
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


@pytest.mark.parametrize(
    ("check_exit", "fallback_allowed"), [(0, False), (1, False), (1, True), (2, False), (2, True)]
)
def test_frozen_filtered_install_relocks_a_stale_lock_only_when_fallback_is_allowed(
    image_scripts: Path, tmp_path: Path, check_exit: int, fallback_allowed: bool
):
    """`uv export --frozen` exports a stale lock as it stands, so staleness is checked before exporting.

    A dependency added to a manifest without relocking would otherwise only surface when `uv pip check`
    finds it missing, after the install, where nothing re-resolves it. `uv lock --check` exits 1 for a
    lock that does not match the manifests and 2 when it cannot check at all; only the first may be
    re-resolved, or a failure to check would read as a stale lock.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    _write_logging_stubs(bin_dir)
    (bin_dir / "uv").write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\0" uv "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n'
        f'if [[ "$*" == "lock --check"* ]]; then exit {check_exit}; fi\n',
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
            "AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION": str(fallback_allowed).lower(),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    commands = [entry[:2] if entry[0] == "uv" else entry[:1] for entry in _read_commands(log)]
    assert commands[0] == ["uv", "lock"]
    assert _read_commands(log)[0][2] == "--check"
    if check_exit == 0:
        assert result.returncode == 0, result.stderr
        assert commands[1:] == [["python3"], ["uv", "export"], ["uv", "pip"], ["uv", "pip"], ["uv", "pip"]]
    elif fallback_allowed and check_exit == 1:
        assert result.returncode == 0, result.stderr
        # Re-locked before anything is exported from the lock.
        assert commands[1:] == [
            ["uv", "lock"],
            ["python3"],
            ["uv", "export"],
            ["uv", "pip"],
            ["uv", "pip"],
            ["uv", "pip"],
        ]
        assert "--check" not in _read_commands(log)[1]
    else:
        # Nothing is re-locked, exported or installed, and the check's own status survives.
        assert result.returncode == check_exit
        assert commands == [["uv", "lock"]]
        expected_reason = "does not match" if check_exit == 1 else "could not be checked"
        assert expected_reason in result.stdout


@pytest.mark.parametrize("excludes_running_interpreter", [True, False])
def test_source_install_follows_the_real_helper(
    image_scripts: Path, tmp_path: Path, excludes_running_interpreter: bool
):
    """The shell and the helper agree on the probe's exit codes and on the helper's options."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    (bin_dir / "uv").write_text(
        '#!/usr/bin/env bash\nprintf "%s\\0" uv "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n',
        encoding="utf-8",
    )
    (bin_dir / "uv").chmod(0o755)
    (bin_dir / "python3").symlink_to(sys.executable)
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    other = f"{sys.version_info.major}.{sys.version_info.minor + 1}"
    workspace = tmp_path / "workspace"
    (workspace / "providers" / "kept").mkdir(parents=True)
    (workspace / "providers" / "excluded").mkdir(parents=True)
    (workspace / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow"\n'
        '[tool.uv.workspace]\nmembers = [".", "providers/kept", "providers/excluded"]\n',
        encoding="utf-8",
    )
    (workspace / "providers" / "kept" / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow-providers-kept"\nrequires-python = ">=3.11"\n', encoding="utf-8"
    )
    (workspace / "providers" / "excluded" / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow-providers-excluded"\n'
        f'requires-python = ">=3.11,!={running if excludes_running_interpreter else other}.*"\n',
        encoding="utf-8",
    )

    result = subprocess.run(
        ["bash", "-c", 'source "$1"; install_from_sources', "bash", str(image_scripts / INSTALLER.name)],
        cwd=workspace,
        env={**_build_environment(bin_dir), "COMMAND_LOG": str(log)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    commands = _read_commands(log)
    if excludes_running_interpreter:
        assert commands[0][:3] == ["uv", "lock", "--check"]
        export = commands[1]
        assert export[:3] == ["uv", "export", "--frozen"]
        assert [export[index + 1] for index, token in enumerate(export) if token == "--package"] == [
            "apache-airflow",
            "apache-airflow-providers-kept",
        ]
        editable_install = commands[3]
        assert [
            Path(editable_install[index + 1]).resolve()
            for index, token in enumerate(editable_install)
            if token == "--editable"
        ] == [workspace.resolve(), (workspace / "providers" / "kept").resolve()]
    else:
        assert commands[0][:4] == ["uv", "sync", "--all-packages", "--frozen"]


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.ci"])
def test_image_includes_filter_helper(dockerfile: str):
    source = (ROOT / dockerfile).read_text(encoding="utf-8")
    assert 'COPY <<"EOF" /filter_ci_install_set.py\n' in source
    copies = [
        line for line in source.replace("\\\n", "").splitlines() if line.startswith("COPY --from=scripts ")
    ]
    assert any("filter_ci_install_set.py" in line and "/scripts/docker/" in line for line in copies)
