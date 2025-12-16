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
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
AIRFLOW_CORE_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-core"
AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_CORE_ROOT_PATH / "src"
AIRFLOW_BREEZE_SOURCES_PATH = AIRFLOW_ROOT_PATH / "dev" / "breeze"
AIRFLOW_PROVIDERS_ROOT_PATH = AIRFLOW_ROOT_PATH / "providers"
AIRFLOW_TASK_SDK_ROOT_PATH = AIRFLOW_ROOT_PATH / "task-sdk"
AIRFLOW_TASK_SDK_SOURCES_PATH = AIRFLOW_TASK_SDK_ROOT_PATH / "src"

# Here we should add the second level paths that we want to have sub-packages in
KNOWN_SECOND_LEVEL_PATHS = ["apache", "atlassian", "common", "cncf", "dbt", "microsoft"]

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.10"

GITHUB_TOKEN: str | None = os.environ.get("GITHUB_TOKEN")

try:
    from rich.console import Console

    console = Console(width=400, color_system="standard")
except ImportError:
    console = None  # type: ignore[assignment]


@contextmanager
def temporary_tsc_project(
    tsconfig_path: Path, files: list[str]
) -> Generator[_TemporaryFileWrapper, None, None]:
    """
    Create a temporary tsconfig.json file that extends the main tsconfig.json file.
    This is needed to run TypeScript compiler with specific files included only
    """
    if not tsconfig_path.exists():
        raise RuntimeError(f"Cannot find {tsconfig_path}")
    temp_tsconfig_path = NamedTemporaryFile(mode="wt", suffix=".json", dir=tsconfig_path.parent, delete=True)
    files_joined = ", ".join([f'"{file}"' for file in files])
    content = f'{{"extends": "./{tsconfig_path.name}", "include": [{files_joined}]}}'
    if console:
        console.print(f"[magenta]Creating temporary tsconfig.json at {temp_tsconfig_path.name}[/]")
        console.print(content)
    else:
        print(f"Creating temporary tsconfig.json at {temp_tsconfig_path.name}", file=sys.stderr)
        print(content, file=sys.stderr)
    temp_tsconfig_path.write(content)
    temp_tsconfig_path.flush()
    yield temp_tsconfig_path


def run_command(*args, **kwargs) -> None:
    """
    Run command with given arguments and return the result.
    """
    cmd = " ".join([shlex.quote(arg) for arg in args[0]])
    cwd = kwargs.get("cwd", os.getcwd())
    text = f"Running command: `{cmd}` in directory: `{cwd}`"
    if console:
        console.print(f"[magenta]{text}[/]")
    else:
        print("#" * min(len(text), 200), file=sys.stderr)
        print(text, file=sys.stderr)
        print("#" * min(len(text), 200), file=sys.stderr)
    time_start = time.time()
    subprocess.check_call(*args, **kwargs)
    time_end = time.time()
    if console:
        console.print(f"[green]After {text}[/]")
        console.print(f"[green]Command finished in {time_end - time_start:.2f} seconds[/]")
    else:
        print("#" * min(len(text), 200), file=sys.stderr)
        print(f"After {text}")
        print(f"Command finished in {time_end - time_start:.2f} seconds", file=sys.stderr)
        print("#" * min(len(text), 200), file=sys.stderr)


def read_airflow_version() -> str:
    ast_obj = ast.parse((AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py").read_text())
    for node in ast_obj.body:
        if isinstance(node, ast.Assign):
            if node.targets[0].id == "__version__":  # type: ignore[attr-defined]
                return ast.literal_eval(node.value)

    raise RuntimeError("Couldn't find __version__ in AST")


def pre_process_files(files: list[str]) -> list[str]:
    """Pre-process files passed to mypy.

    * Exclude conftest.py files and __init__.py files
    * When running build on non-main branch do not take providers into account.
    * When running "airflow-core" package, then we need to exclude providers.
    """

    files = [file for file in files if not file.endswith("conftest.py") and not file.endswith("__init__.py")]
    default_branch = os.environ.get("DEFAULT_BRANCH")
    if not default_branch or default_branch == "main":
        return files
    return [file for file in files if not file.startswith("providers")]


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


def initialize_breeze_prek(name: str, file: str):
    if name not in ("__main__", "__mp_main__"):
        raise SystemExit(
            "This file is intended to be executed as an executable program. You cannot use it as a module."
            f"To run this script, run the ./{file} command"
        )

    if os.environ.get("SKIP_BREEZE_PREK_HOOKS"):
        console.print("[yellow]Skipping breeze prek hooks as SKIP_BREEZE_PREK_HOOKS is set")
        sys.exit(0)
    if shutil.which("breeze") is None:
        console.print(
            "[red]The `breeze` command is not on path.[/]\n\n"
            "[yellow]Please install breeze.\n"
            "You can use uv with `uv tool install -e ./dev/breeze or "
            "`pipx install -e ./dev/breeze`.\n"
            "It will install breeze from Airflow sources "
            "(make sure you run `pipx ensurepath` if you use pipx)[/]\n\n"
            "[bright_blue]You can also set SKIP_BREEZE_PREK_HOOKS env variable to non-empty "
            "value to skip all breeze tests."
        )
        sys.exit(1)


def run_command_via_breeze_shell(
    cmd: list[str],
    python_version: str = DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    backend: str = "none",
    executor: str = "LocalExecutor",
    extra_env: dict[str, str] | None = None,
    project_name: str = "prek",
    skip_environment_initialization: bool = True,
    warn_image_upgrade_needed: bool = False,
    enable_pseudo_terminal: bool = False,
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
        # Note: The terminal is disabled - because prek is run inside git without a pseudo-terminal
        "--tty",
        "enabled" if enable_pseudo_terminal else "disabled",
    ]
    if warn_image_upgrade_needed:
        subprocess_cmd.append("--warn-image-upgrade-needed")
    if skip_environment_initialization:
        subprocess_cmd.append("--skip-environment-initialization")
    if project_name:
        subprocess_cmd.extend(["--project-name", project_name])
    subprocess_cmd.append(" ".join([shlex.quote(arg) for arg in cmd]))
    new_env = {
        **os.environ,
        "SKIP_BREEZE_SELF_UPGRADE_CHECK": "true",
        "SKIP_GROUP_OUTPUT": "true",
        "SKIP_SAVING_CHOICES": "true",
        "ANSWER": "no",
        **extra_env,
    }

    if os.environ.get("VERBOSE_COMMANDS") or os.environ.get("CI") == "true":
        if console:
            console.print(
                f"[magenta]Running command: {' '.join([shlex.quote(item) for item in subprocess_cmd])}[/]"
            )
            console.print("[magenta]With environment:[/]")
            console.print(new_env)
        else:
            print(f"Running command: {' '.join([shlex.quote(item) for item in subprocess_cmd])}")
            print("With environment:")
            print(new_env)
    result = subprocess.run(
        subprocess_cmd,
        check=False,
        text=True,
        **other_popen_kwargs,
        env=new_env,
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
            if console:
                console.print(
                    "\n[yellow]If you see strange stacktraces above, especially about missing imports "
                    "run this command:[/]\n"
                )
                console.print(
                    "[magenta]breeze ci-image build --python 3.10 --upgrade-to-newer-dependencies[/]\n"
                )
            else:
                print(
                    "\nIf you see strange stacktraces above, especially about missing imports "
                    "run this command:\nbreeze ci-image build --python 3.10 --upgrade-to-newer-dependencies\n"
                )

    elif cmd_result.returncode != 0:
        if console:
            console.print(
                "[warning]\nIf you see strange stacktraces above, "
                "run `breeze ci-image build --python 3.10` and try again."
            )
        else:
            print(
                "\nIf you see strange stacktraces above, "
                "run `breeze ci-image build --python 3.10` and try again."
            )
    sys.exit(cmd_result.returncode)


def get_provider_id_from_path(file_path: Path) -> str | None:
    """
    Get the provider id from the path of the file it belongs to.
    """
    for parent in file_path.parents:
        # This works fine for both new and old providers structure - because we moved provider.yaml to
        # the top-level of the provider and this code finding "providers"  will find the "providers" package
        # in old structure and "providers" directory in new structure - in both cases we can determine
        # the provider id from the relative folders
        if (parent / "provider.yaml").exists():
            for providers_root_candidate in parent.parents:
                if providers_root_candidate.name == "providers":
                    return parent.relative_to(providers_root_candidate).as_posix().replace("/", ".")
            return None
    return None


def get_provider_base_dir_from_path(file_path: Path) -> Path | None:
    """
    Get the provider base dir (where provider.yaml is) from the path of the file it belongs to.
    """
    for parent in file_path.parents:
        if (parent / "provider.yaml").exists():
            return parent
    return None


def get_all_provider_ids() -> list[str]:
    """
    Get all providers from the new provider structure
    """
    all_provider_ids = []
    for provider_file in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        if provider_file.is_relative_to(AIRFLOW_PROVIDERS_ROOT_PATH / "src"):
            continue
        provider_id = get_provider_id_from_path(provider_file)
        if provider_id:
            all_provider_ids.append(provider_id)
    return all_provider_ids


def get_all_provider_yaml_files() -> list[Path]:
    """
    Get all providers from the new provider structure
    """
    all_provider_yaml_files = []
    for provider_file in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        if provider_file.is_relative_to(AIRFLOW_PROVIDERS_ROOT_PATH / "src"):
            continue
        all_provider_yaml_files.append(provider_file)
    return all_provider_yaml_files


def get_all_provider_info_dicts() -> dict[str, dict]:
    """
    Get provider yaml info for all providers from the new provider structure
    """
    providers: dict[str, dict] = {}
    for provider_file in get_all_provider_yaml_files():
        provider_id = str(provider_file.parent.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH)).replace(os.sep, ".")
        import yaml

        provider_info = yaml.safe_load(provider_file.read_text())
        if provider_info["state"] != "suspended":
            providers[provider_id] = provider_info
    return providers


def get_imports_from_file(file_path: Path, *, only_top_level: bool) -> list[str]:
    """
    Returns list of all imports in file.

    For following code:
    import os
    from collections import defaultdict
    import numpy as np
    from pandas import DataFrame as DF

    def inner():
        import json
        from pathlib import Path, PurePath
    from __future__ import annotations

    When only_top_level = False then returns
        ['os', 'collections.defaultdict', 'numpy', 'pandas.DataFrame']
    When only_top_level = False then returns
        ['os', 'collections.defaultdict', 'numpy', 'pandas.DataFrame', 'json', 'pathlib.Path', 'pathlib.PurePath']
    """
    root = ast.parse(file_path.read_text(), file_path.name)
    imports: list[str] = []

    nodes = ast.iter_child_nodes(root) if only_top_level else ast.walk(root)
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module == "__future__":
                continue
            for alias in node.names:
                name = alias.name
                fullname = f"{node.module}.{name}" if node.module else name
                imports.append(fullname)

    return imports


def retrieve_gh_token(*, token: str | None = None, description: str, scopes: str) -> str:
    if token:
        return token
    if GITHUB_TOKEN:
        return GITHUB_TOKEN
    output = subprocess.check_output(["gh", "auth", "token"])
    token = output.decode().strip()
    if not token:
        if not console:
            raise RuntimeError("Please add rich to your script dependencies and run it again")
        console.print(
            "[red]GITHUB_TOKEN environment variable is not set. "
            "This might lead to failures on rate limits.[/]\n"
            "You can fix that by installing `gh` and running `gh auth login` or "
            f"set it to a valid GitHub token with {scopes} scope. "
            f"You can create one by clicking the URL:\n\n"
            f"https://github.com/settings/tokens/new?scopes={scopes}&description={description}\n\n"
            "Once you have the token you can prepend prek command with GITHUB_TOKEN='<your token>' or"
            "set it in your environment with export GITHUB_TOKEN='<your token>'\n\n"
        )
        sys.exit(1)
    return token
