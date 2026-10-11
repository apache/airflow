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
    }
    environment.pop("AIRFLOW_SOURCES", None)
    return environment


@pytest.mark.parametrize(
    ("metadata", "requires_python", "filtered"),
    [
        ('excluded-python-versions:\n  - "3.15"\n', ">=3.11,!=3.15.*", True),
        ("excluded-python-versions:\n  - '3.15'\n", ">=3.11,!=3.15.*", True),
        ("excluded-python-versions: ['3.15']\n", ">=3.11,!=3.15.*", True),
        ("", ">=3.11", False),
    ],
)
@pytest.mark.parametrize("upgrade", [False, True])
def test_source_install_selects_path_without_ci_sources(
    image_scripts: Path, tmp_path: Path, metadata: str, requires_python: str, filtered: bool, upgrade: bool
):
    workspace = tmp_path / "workspace"
    provider = workspace / "providers" / "example"
    provider.mkdir(parents=True)
    (workspace / "pyproject.toml").write_text(
        '[project]\nname = "test-root"\n[tool.uv.workspace]\nmembers = [".", "providers/example"]\n',
        encoding="utf-8",
    )
    (provider / "pyproject.toml").write_text(
        f'[project]\nname = "test-provider"\nrequires-python = "{requires_python}"\n', encoding="utf-8"
    )
    (provider / "provider.yaml").write_text(metadata, encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uv").write_text('#!/usr/bin/env bash\nprintf "uv %s\\n" "$*"\n', encoding="utf-8")
    (bin_dir / "uv").chmod(0o755)

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; function install_workspace_without_excluded_providers() { printf "filtered\\n"; }; install_from_sources',
            "bash",
            str(image_scripts / INSTALLER.name),
        ],
        cwd=workspace,
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


@pytest.mark.parametrize("helper_fails", [False, True])
def test_filtered_install_consumes_set_and_propagates_helper_failure(
    image_scripts: Path, tmp_path: Path, helper_fails: bool
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands"
    (bin_dir / "uv").write_text(
        '#!/usr/bin/env bash\nprintf "%s\\0" uv "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n',
        encoding="utf-8",
    )
    (bin_dir / "python3").write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\0" python3 "$@" >> "$COMMAND_LOG"\nprintf "\\0" >> "$COMMAND_LOG"\n'
        'if [[ "$HELPER_FAILS" == "true" ]]; then exit 23; fi\n'
        "while [[ $# -gt 0 ]]; do\n"
        '  case "$1" in\n'
        '    --output) printf "shared==1.0\\n" > "$2"; shift ;;\n'
        '    --editables) printf "%s\\n" "$KEPT_MEMBER" "$KEPT_WITH_SPACE" > "$2"; shift ;;\n'
        "  esac\n"
        "  shift\n"
        "done\n",
        encoding="utf-8",
    )
    for command in ("uv", "python3"):
        (bin_dir / command).chmod(0o755)
    kept = str(tmp_path / "kept")
    kept_with_space = str(tmp_path / "kept with space")
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
            "HELPER_FAILS": str(helper_fails).lower(),
            "KEPT_MEMBER": kept,
            "KEPT_WITH_SPACE": kept_with_space,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    commands = [entry.decode().split("\0") for entry in log.read_bytes().split(b"\0\0") if entry]

    assert commands[0][:3] == ["uv", "export", "--frozen"]
    helper_command = commands[1]
    assert helper_command[:4] == ["python3", str(image_scripts / HELPER.name), "--root", str(tmp_path)]
    assert helper_command[helper_command.index("--python-minor") + 1] == "3.15"
    if helper_fails:
        assert result.returncode == 23
        assert len(commands) == 2
    else:
        assert result.returncode == 0, result.stderr
        assert commands[2][-2:] == ["--requirement", str(tmp_path / "work" / "requirements.txt")]
        assert commands[3][-4:] == ["--editable", kept, "--editable", kept_with_space]
        assert "--no-deps" in commands[2] and "--no-deps" in commands[3]
        assert commands[4] == ["uv", "pip", "check", "--python", str(bin_dir / "python3")]


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.ci"])
def test_image_includes_filter_helper(dockerfile: str):
    source = (ROOT / dockerfile).read_text(encoding="utf-8")
    assert 'COPY <<"EOF" /filter_ci_install_set.py\n' in source
    copies = [
        line for line in source.replace("\\\n", "").splitlines() if line.startswith("COPY --from=scripts ")
    ]
    assert any("filter_ci_install_set.py" in line and "/scripts/docker/" in line for line in copies)
