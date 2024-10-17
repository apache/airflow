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

import ast
import difflib
import os
import shlex
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

from rich.console import Console

AIRFLOW_SOURCES_ROOT_PATH = Path(__file__).parents[3].resolve()
AIRFLOW_BREEZE_SOURCES_PATH = AIRFLOW_SOURCES_ROOT_PATH / "dev" / "breeze"
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.9"

console = Console(width=400, color_system="standard")


def read_airflow_version() -> str:
    ast_obj = ast.parse((AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "__init__.py").read_text())
    for node in ast_obj.body:
        if isinstance(node, ast.Assign):
            if node.targets[0].id == "__version__":  # type: ignore[attr-defined]
                return ast.literal_eval(node.value)

    raise RuntimeError("Couldn't find __version__ in AST")


def pre_process_files(files: list[str]) -> list[str]:
    """Pre-process files passed to mypy.

    * When running build on non-main branch do not take providers into account.
    * When running "airflow/providers" package, then we need to add --namespace-packages flag.
    * When running "airflow" package, then we need to exclude providers.
    """
    default_branch = os.environ.get("DEFAULT_BRANCH")
    if not default_branch or default_branch == "main":
        return files
    result = [file for file in files if not file.startswith(f"airflow{os.sep}providers")]
    if "airflow/providers" in files:
        if len(files) > 1:
            raise RuntimeError(
                "When running `airflow/providers` package, you cannot run any other packages because only "
                "airflow/providers package requires --namespace-packages flag to be set"
            )
        result.append("--namespace-packages")
    if "airflow" in files:
        if len(files) > 1:
            raise RuntimeError(
                "When running `airflow` package, you cannot run any other packages because only "
                "airflow/providers package requires --exclude airflow/providers/.* flag to be set"
            )
        result.extend(["--exclude", "airflow/providers/.*"])
    return result


def insert_documentation(
    file_path: Path, content: list[str], header: str, footer: str, add_comment: bool = False
) -> bool:
    found = False
    old_content = file_path.read_text()
    lines = old_content.splitlines(keepends=True)
    replacing = False
    result: list[str] = []
    for line in lines:
        if line.strip().startswith(header.strip()):
            replacing = True
            found = True
            result.append(line)
            if add_comment:
                result.extend(["# " + line if line != "\n" else "#\n" for line in content])
            else:
                result.extend(content)
        if line.strip().startswith(footer.strip()):
            replacing = False
        if not replacing:
            result.append(line)
    new_content = "".join(result)
    if not found:
        print(f"Header {header} not found in {file_path}")
        sys.exit(1)
    if new_content != old_content:
        file_path.write_text(new_content)
        console.print(f"Updated {file_path}")
        return True
    return False


def initialize_breeze_precommit(name: str, file: str):
    if name not in ("__main__", "__mp_main__"):
        msg = (
            "This file is intended to be executed as an executable program. You cannot use it as a module."
            f"To run this script, run the ./{file} command"
        )
        raise SystemExit(msg)

    if os.environ.get("SKIP_BREEZE_PRE_COMMITS"):
        console.print("[yellow]Skipping breeze pre-commit as SKIP_BREEZE_PRE_COMMIT is set")
        sys.exit(0)
    if shutil.which("breeze") is None:
        console.print(
            "[red]The `breeze` command is not on path.[/]\n\n"
            "[yellow]Please install breeze with `pipx install -e ./dev/breeze` from Airflow sources "
            "and make sure you run `pipx ensurepath`[/]\n\n"
            "[bright_blue]You can also set SKIP_BREEZE_PRE_COMMITS env variable to non-empty "
            "value to skip all breeze tests."
        )
        sys.exit(1)


def run_command_via_breeze_shell(
    cmd: list[str],
    python_version: str = DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    backend: str = "none",
    executor: str = "SequentialExecutor",
    extra_env: dict[str, str] | None = None,
    project_name: str = "pre-commit",
    skip_environment_initialization: bool = True,
    warn_image_upgrade_needed: bool = False,
    **other_popen_kwargs,
) -> subprocess.CompletedProcess:
    extra_env = extra_env or {}
    subprocess_cmd: list[str] = [
        "breeze",
        "shell",
        "--python",
        python_version,
        "--backend",
        backend,
        "--executor",
        executor,
        "--quiet",
        "--restart",
        "--skip-image-upgrade-check",
        "--tty",
        "disabled",
    ]
    if warn_image_upgrade_needed:
        subprocess_cmd.append("--warn-image-upgrade-needed")
    if skip_environment_initialization:
        subprocess_cmd.append("--skip-environment-initialization")
    if project_name:
        subprocess_cmd.extend(["--project-name", project_name])
    subprocess_cmd.append(" ".join([shlex.quote(arg) for arg in cmd]))
    if os.environ.get("VERBOSE_COMMANDS"):
        console.print(
            f"[magenta]Running command: {' '.join([shlex.quote(item) for item in subprocess_cmd])}[/]"
        )
    result = subprocess.run(
        subprocess_cmd,
        check=False,
        text=True,
        **other_popen_kwargs,
        env={
            **os.environ,
            "SKIP_BREEZE_SELF_UPGRADE_CHECK": "true",
            "SKIP_GROUP_OUTPUT": "true",
            "SKIP_SAVING_CHOICES": "true",
            "ANSWER": "no",
            **extra_env,
        },
    )
    # Stop remaining containers
    down_command = ["docker", "compose", "--progress", "quiet"]
    if project_name:
        down_command.extend(["--project-name", project_name])
    down_command.extend(["down", "--remove-orphans"])
    subprocess.run(down_command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result


class ConsoleDiff(difflib.Differ):
    def _dump(self, tag, x, lo, hi):
        """Generate comparison results for a same-tagged range."""
        for i in range(lo, hi):
            if tag == "+":
                yield f"[green]{tag} {x[i]}[/]"
            elif tag == "-":
                yield f"[red]{tag} {x[i]}[/]"
            else:
                yield f"{tag} {x[i]}"


def check_list_sorted(the_list: list[str], message: str, errors: list[str]) -> bool:
    sorted_list = sorted(set(the_list))
    if the_list == sorted_list:
        console.print(f"{message} is [green]ok[/]")
        console.print(the_list)
        console.print()
        return True
    console.print(f"{message} [red]NOK[/]")
    console.print(textwrap.indent("\n".join(ConsoleDiff().compare(the_list, sorted_list)), " " * 4))
    console.print()
    errors.append(f"ERROR in {message}. The elements are not sorted/unique.")
    return False


def validate_cmd_result(cmd_result, include_ci_env_check=False):
    if include_ci_env_check:
        if cmd_result.returncode != 0 and os.environ.get("CI") != "true":
            console.print(
                "\n[yellow]If you see strange stacktraces above, especially about missing imports "
                "run this command:[/]\n"
            )
            console.print("[magenta]breeze ci-image build --python 3.9 --upgrade-to-newer-dependencies[/]\n")

    elif cmd_result.returncode != 0:
        console.print(
            "[warning]\nIf you see strange stacktraces above, "
            "run `breeze ci-image build --python 3.9` and try again."
        )
    sys.exit(cmd_result.returncode)
