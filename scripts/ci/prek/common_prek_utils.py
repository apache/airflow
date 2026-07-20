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

import abc
import ast
import difflib
import os
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
import time
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any

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

# Maps a Docker build platform string (as declared in ``provider.yaml`` under
# ``excluded-platforms``) to the ``platform_machine`` values Python reports on that
# architecture. ``linux/arm64`` covers both Linux (``aarch64``) and macOS Apple Silicon
# (``arm64``) so a provider opting out of ARM is never pulled in on any ARM machine where
# its native dependency cannot be built.
EXCLUDED_PLATFORM_MACHINES: dict[str, list[str]] = {
    "linux/arm64": ["aarch64", "arm64"],
}

GITHUB_TOKEN_ENV_VARS = ("GH_TOKEN", "GITHUB_TOKEN")

try:
    from rich.console import Console
    from rich.panel import Panel

    console = Console(width=400, color_system="standard")
except ImportError:
    console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment,misc]


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
    time_start = time.monotonic()
    subprocess.check_call(*args, **kwargs)
    time_end = time.monotonic()
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


GLOBAL_CONSTANTS_PATH = (
    AIRFLOW_ROOT_PATH / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py"
)


def _read_global_constants_assignment(name: str) -> Any:
    """Read a top-level assignment from global_constants.py.

    Handles both plain assignments (``NAME = ...``) and annotated assignments
    (``NAME: type = ...``). The value must be a literal so it can be safely
    evaluated with ``ast.literal_eval``.
    """
    tree = ast.parse(GLOBAL_CONSTANTS_PATH.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == name and node.value is not None:
                return ast.literal_eval(node.value)
    raise RuntimeError(f"{name} not found in global_constants.py")


def read_allowed_kubernetes_versions() -> list[str]:
    """Parse ALLOWED_KUBERNETES_VERSIONS from global_constants.py (single source of truth).

    Returns versions without the ``v`` prefix, e.g. ``["1.30.13", "1.31.12", ...]``.
    """
    versions: list[str] = _read_global_constants_assignment("ALLOWED_KUBERNETES_VERSIONS")
    return [v.lstrip("v") for v in versions]


def read_allowed_python_major_minor_versions() -> list[str]:
    """Parse ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS from global_constants.py (single source of truth)."""
    return list(_read_global_constants_assignment("ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS"))


def read_current_postgres_versions() -> list[str]:
    """Parse CURRENT_POSTGRES_VERSIONS from global_constants.py (single source of truth)."""
    return list(_read_global_constants_assignment("CURRENT_POSTGRES_VERSIONS"))


def read_current_mysql_versions() -> list[str]:
    """The MySQL release versions Airflow currently tests with.

    Mirrors how ``CURRENT_MYSQL_VERSIONS`` is built in global_constants.py: the
    "old" releases plus the LTS releases, plus an innovation release when one is
    configured. Returns the numeric versions only (e.g. ``["8.0", "8.4"]``); the
    docs add the textual "Innovation" annotation on top of these.
    """
    versions: list[str] = list(_read_global_constants_assignment("MYSQL_OLD_RELEASES"))
    versions += list(_read_global_constants_assignment("MYSQL_LTS_RELEASES"))
    innovation = _read_global_constants_assignment("MYSQL_INNOVATION_RELEASE")
    if innovation:
        versions.append(innovation)
    return versions


def read_default_python_major_minor_version_for_images() -> str:
    """Parse DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES from global_constants.py."""
    value = _read_global_constants_assignment("DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES")
    if not isinstance(value, str):
        raise RuntimeError(
            "DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES in global_constants.py "
            f"must be a string, got {type(value).__name__}"
        )
    return value


def pre_process_mypy_files(files: list[str]) -> list[str]:
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


def is_hidden_within_root(path: Path, root: Path) -> bool:
    """Whether any path component below ``root`` is dot-prefixed."""
    return any(part.startswith(".") for part in path.relative_to(root).parts)


def insert_documentation(
    file_path: Path,
    content: list[str],
    header: str,
    footer: str,
    add_comment: bool = False,
    extra_information: str | None = None,
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
        console.print(f"Updated {file_path} with {extra_information or 'generated documentation'}")
        return True
    return False


_UV_REQUIRED_VERSION_RE = re.compile(
    r"""^\s*required-version\s*=\s*["']\s*>=\s*(?P<ver>\d+(?:\.\d+){0,2})\s*["']""",
    re.MULTILINE,
)


def _parse_version(ver: str) -> tuple[int, ...]:
    """Turn "0.9.17" into (0, 9, 17). Extra pre-release/build suffixes are ignored."""
    match = re.match(r"^(\d+(?:\.\d+)*)", ver.strip())
    if not match:
        raise ValueError(f"Cannot parse version: {ver!r}")
    return tuple(int(part) for part in match.group(1).split("."))


def read_uv_required_min_version() -> tuple[str, tuple[int, ...]]:
    """Read the minimum uv version from the root ``pyproject.toml``.

    Parses ``[tool.uv] required-version = ">=X.Y.Z"`` and returns ``(raw, tuple)``.
    We parse by regex to avoid pulling a TOML dep into every prek script.
    """
    pyproject = (AIRFLOW_ROOT_PATH / "pyproject.toml").read_text()
    # Narrow to the [tool.uv] section so we don't match a different required-version.
    match = re.search(r"^\[tool\.uv\]\s*$(?P<body>.*?)(?=^\[|\Z)", pyproject, re.MULTILINE | re.DOTALL)
    if not match:
        raise RuntimeError("`[tool.uv]` section not found in root pyproject.toml")
    ver_match = _UV_REQUIRED_VERSION_RE.search(match.group("body"))
    if not ver_match:
        raise RuntimeError('`required-version = ">=X.Y.Z"` not found under `[tool.uv]` in pyproject.toml')
    raw = ver_match.group("ver")
    return raw, _parse_version(raw)


def check_uv_version(uv_bin: str = "uv") -> None:
    """Fail the hook if ``uv_bin`` is older than ``[tool.uv] required-version``.

    Called manually by prek hooks that invoke ``uv`` (directly or via breeze) so a
    contributor with an outdated uv sees a clear error before the hook spends time
    running and emits a confusing downstream failure.
    """
    try:
        raw_min, min_tuple = read_uv_required_min_version()
    except Exception as exc:
        # Don't block hooks on parse bugs — warn and continue.
        message = f"Could not determine required uv version from pyproject.toml: {exc}"
        if console:
            console.print(f"[yellow]WARNING: {message}")
        else:
            print(f"WARNING: {message}")
        return

    try:
        output = subprocess.check_output([uv_bin, "--version"], text=True).strip()
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        message = (
            f"Could not run `{uv_bin} --version` to verify uv version (required: >= {raw_min}). Error: {exc}"
        )
        if console:
            console.print(f"[red]{message}")
        else:
            print(message)
        sys.exit(1)

    match = re.search(r"\b(\d+\.\d+(?:\.\d+)?)", output)
    if not match:
        message = f"Unexpected `uv --version` output: {output!r}"
        if console:
            console.print(f"[yellow]WARNING: {message}")
        else:
            print(f"WARNING: {message}")
        return
    actual_raw = match.group(1)
    actual_tuple = _parse_version(actual_raw)
    if actual_tuple < min_tuple:
        message = (
            f"uv {actual_raw} at `{uv_bin}` is older than the project-required "
            f">= {raw_min} (see `[tool.uv] required-version` in pyproject.toml). "
            "Upgrade uv before running this hook, e.g.:\n"
            "  uv self update\n"
            "or, to refresh the project-pinned uv in the main venv (included in the "
            "`dev` dependency group via the `all` extras):\n"
            "  uv sync\n"
        )
        if console:
            console.print(f"[red]{message}")
        else:
            print(message)
        sys.exit(1)


def initialize_breeze_prek(name: str, file: str):
    if name not in ("__main__", "__mp_main__"):
        raise SystemExit(
            "This file is intended to be executed as an executable program. You cannot use it as a module."
            f"To run this script, run the ./{file} command"
        )

    if os.environ.get("SKIP_BREEZE_PREK_HOOKS"):
        console.print("[yellow]Skipping breeze prek hooks as SKIP_BREEZE_PREK_HOOKS is set")
        sys.exit(0)
    # Breeze itself runs under uv, so enforce the project's minimum uv version up front.
    check_uv_version()
    if shutil.which("breeze") is None:
        console.print(
            "[red]The `breeze` command is not on path.[/]\n\n"
            "[yellow]Please install breeze. Recommended: run `./scripts/tools/setup_breeze` "
            "from the repo root — it installs a shim at `~/.local/bin/breeze` that runs breeze "
            "via `uvx` from the current git worktree (see ADR 0017).\n"
            "Legacy global install (`uv tool install -e ./dev/breeze` or "
            "`pipx install -e ./dev/breeze`) still works but is no longer recommended.[/]\n\n"
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
    project_name: str = "breeze-prek",
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
    try:
        return subprocess.run(
            subprocess_cmd,
            check=False,
            text=True,
            **other_popen_kwargs,
            env=new_env,
        )
    finally:
        # Always clean up containers, networks, and volumes the breeze shell
        # invocation created — even if the subprocess raised (KeyboardInterrupt,
        # OSError, etc.). Without --volumes the next prek run inherits state
        # from the previous run, which is the bug this finally clause prevents.
        down_command = ["docker", "compose", "--progress", "quiet"]
        if project_name:
            down_command.extend(["--project-name", project_name])
        down_command.extend(["down", "--remove-orphans", "--volumes"])
        subprocess.run(down_command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_command_via_breeze_run(
    cmd: list[str],
    python_version: str = DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    backend: str = "none",
    executor: str = "LocalExecutor",
    extra_env: dict[str, str] | None = None,
    project_name: str = "breeze-prek",
    skip_environment_initialization: bool = True,
    warn_image_upgrade_needed: bool = False,
    enable_pseudo_terminal: bool = False,
    **other_popen_kwargs,
) -> subprocess.CompletedProcess:
    extra_env = extra_env or {}
    # Kept for call-site compatibility. `breeze run` does not use `executor`.
    _ = executor
    subprocess_cmd: list[str] = [
        "breeze",
        "run",
        "--python",
        python_version,
        "--backend",
        backend,
        "--tty",
        "enabled" if enable_pseudo_terminal else "disabled",
    ]
    if not warn_image_upgrade_needed:
        subprocess_cmd.append("--skip-image-upgrade-check")
    if skip_environment_initialization:
        # `breeze run` always runs non-interactively; keep parameter for compatibility.
        pass
    if project_name:
        subprocess_cmd.extend(["--project-name", project_name])
    subprocess_cmd.extend(cmd)
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
    return subprocess.run(
        subprocess_cmd,
        check=False,
        text=True,
        **other_popen_kwargs,
        env=new_env,
    )


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


def get_all_provider_ids(exclude_suspended_providers: bool = False) -> list[str]:
    """
    Get all providers from the new provider structure
    """
    all_provider_ids = []
    for provider_file in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        if provider_file.is_relative_to(AIRFLOW_PROVIDERS_ROOT_PATH / "src"):
            continue
        if exclude_suspended_providers:
            import yaml

            provider_info = yaml.safe_load(provider_file.read_text())
            if provider_info.get("state") == "suspended":
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


_NOQA_RE = re.compile(r"#\s*noqa\s*:\s*([^\n]*)", re.IGNORECASE)
_NOQA_CODE_RE = re.compile(r"[A-Z]+\d+\b")


def _parse_noqa_codes(line: str) -> set[str]:
    """Extract codes from the leading comma-separated list in a ``# noqa: <codes>`` comment.

    Each code must be terminated by a word boundary, so tokens like ``SDK002x``
    or ``F401foo`` are not treated as the corresponding code.

    Anything after the first non-code token is treated as explanatory text and
    ignored, so ``# noqa: F401 - see SDK002 docs`` only yields ``{"F401"}``.
    """
    match = _NOQA_RE.search(line)
    if not match:
        return set()
    codes: set[str] = set()
    for raw in match.group(1).split(","):
        code_match = _NOQA_CODE_RE.match(raw.strip())
        if not code_match:
            break
        codes.add(code_match.group(0))
    return codes


def has_nocheck_marker(source_lines: list[str], node: ast.ImportFrom | ast.Import, nocheck_code: str) -> bool:
    """
    Check if the import statement has a ``# noqa: <codes>`` comment that lists
    ``nocheck_code`` on any of its lines. The code may appear anywhere in the
    comma-separated code list (e.g. ``# noqa: F401, SDK002``).
    """
    start = node.lineno
    end = node.end_lineno or start
    for lineno in range(start, end + 1):
        if lineno <= len(source_lines) and nocheck_code in _parse_noqa_codes(source_lines[lineno - 1]):
            return True
    return False


def find_import_violations(
    file_path: Path,
    *,
    is_violating_module: Callable[[str], bool],
    nocheck_code: str,
    check_plain_imports: bool = False,
) -> list[tuple[int, str]]:
    """
    Walk imports in ``file_path`` and return ``(lineno, statement)`` for each
    that matches ``is_violating_module`` and is not suppressed by a
    ``# noqa: <nocheck_code>`` comment.

    :param check_plain_imports: also check ``import x`` statements (in addition
        to ``from x import y``).
    """
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    source_lines = source.splitlines()
    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if not node.module:
                continue
            if is_violating_module(node.module):
                violating_names = [alias.name for alias in node.names]
            else:
                # Catch ``from airflow import settings`` style imports where the
                # offending module is the dotted ``<module>.<name>`` path.
                violating_names = [
                    alias.name for alias in node.names if is_violating_module(f"{node.module}.{alias.name}")
                ]
            if not violating_names:
                continue
            if has_nocheck_marker(source_lines, node, nocheck_code):
                continue
            statement = f"from {node.module} import {', '.join(violating_names)}"
            violations.append((node.lineno, statement))
        elif check_plain_imports and isinstance(node, ast.Import):
            for alias in node.names:
                if is_violating_module(alias.name):
                    if has_nocheck_marker(source_lines, node, nocheck_code):
                        continue
                    statement = f"import {alias.name}"
                    if alias.asname:
                        statement += f" as {alias.asname}"
                    violations.append((node.lineno, statement))

    return violations


def report_import_violations(
    files: list[str],
    *,
    check_func: Callable[[Path], list[tuple[int, str]]],
    violation_label: str,
    nocheck_code: str | None = None,
    only_python_files: bool = False,
) -> None:
    """Run ``check_func`` on each file, print violations, and exit(1) if any are found.

    When ``nocheck_code`` is given, a hint pointing at the ``# noqa: <code>``
    escape hatch is printed alongside the failure summary.
    """
    file_paths = [Path(f) for f in files if not only_python_files or f.endswith(".py")]
    total_violations = 0

    for file_path in file_paths:
        mismatches = check_func(file_path)
        if mismatches:
            console.print(f"[red]{file_path}[/red]:")
            for line_num, statement in mismatches:
                console.print(f"  [yellow]Line {line_num}[/yellow]: {statement}")
            total_violations += len(mismatches)

    if total_violations:
        console.print()
        console.print(f"[red]Found {total_violations} {violation_label}[/red]")
        if nocheck_code:
            console.print(
                f"[yellow]Hint:[/yellow] if an import above is intentional, append "
                f"`# noqa: {nocheck_code}` to the import line (single-line imports) "
                f"or to the opening/closing paren line (multi-line imports) to "
                f"suppress this check for that statement."
            )
        sys.exit(1)


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


def get_remote_for_main() -> str:
    """
    Return the remote name to use when fetching main.
    Prefers the remote that points to apache/airflow; otherwise uses origin.
    """
    result = subprocess.run(
        ["git", "remote", "-v"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return "origin"

    apache_remote = None
    origin_remote = None
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            name, url = parts[0], parts[1]
            if "apache/airflow" in url:
                apache_remote = name
                break
            if name == "origin":
                origin_remote = name

    return apache_remote or origin_remote or "origin"


def env_without_github_tokens(env: dict[str, str] | None = None) -> dict[str, str]:
    cleaned_env = dict(os.environ if env is None else env)
    for token_env_var in GITHUB_TOKEN_ENV_VARS:
        cleaned_env.pop(token_env_var, None)
    return cleaned_env


def get_github_token_from_env(env: dict[str, str] | None = None) -> str | None:
    source_env = os.environ if env is None else env
    for token_env_var in GITHUB_TOKEN_ENV_VARS:
        token = source_env.get(token_env_var)
        if token:
            return token
    return None


def resolve_github_token(*, token: str | None = None, env: dict[str, str] | None = None) -> str | None:
    """Resolve a token while preventing ambient env tokens from shadowing ``gh auth login``."""
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            check=False,
            env=env_without_github_tokens(env),
        )
    except FileNotFoundError:
        return get_github_token_from_env(env)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return get_github_token_from_env(env)


def retrieve_gh_token(*, token: str | None = None, description: str, scopes: str) -> str:
    token = resolve_github_token(token=token)
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


def parse_operations(
    operations_file: Path, exclude_operation_classes: set, exclude_methods: set
) -> dict[str, list[str]]:
    """Parse airflowctl operations file and return a mapping of CLI group names to subcommands."""
    commands: dict[str, list[str]] = {}

    with open(operations_file) as f:
        tree = ast.parse(f.read(), filename=str(operations_file))

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("Operations"):
            if node.name in exclude_operation_classes:
                continue

            group_name = node.name.replace("Operations", "").lower()
            commands[group_name] = []

            for child in node.body:
                if isinstance(child, ast.FunctionDef):
                    method_name = child.name
                    if method_name in exclude_methods or method_name.startswith("_"):
                        continue
                    subcommand = method_name.replace("_", "-")
                    commands[group_name].append(subcommand)

    return commands


def _is_safe_relative(rel: str, repo_root: Path) -> bool:
    """Whether ``rel`` is a plain relative path that stays inside ``repo_root``."""
    candidate = Path(rel)
    if candidate.is_absolute():
        return False
    try:
        (repo_root / candidate).resolve().relative_to(repo_root.resolve())
    except ValueError:
        return False
    return True


class AllowlistManager(abc.ABC):
    """Common base for prek hooks that track per-file occurrence counts in allowlist files.

    Subclasses implement :meth:`iter_files`, :meth:`count_occurrences`, and
    :meth:`violation_panel_text` to define what gets scanned, how violations
    are counted, and what help text to show.  Everything else — loading, saving,
    generating, cleaning up, and the check loop — is handled here.
    """

    def __init__(self, allowlist_file: Path, *, repo_root: Path = AIRFLOW_ROOT_PATH) -> None:
        self.allowlist_file = allowlist_file
        self.repo_root = repo_root

    def parse(self, text: str) -> dict[str, int]:
        """Parse allowlist *text* into a ``{rel_path: count}`` mapping.

        Entries that escape the repo root (absolute paths or ``..`` segments)
        are silently skipped.
        """
        result: dict[str, int] = {}
        for raw_line in text.splitlines():
            if not (stripped := raw_line.strip()):
                continue

            rel_str, _, count_str = stripped.rpartition("::")
            if not rel_str or not count_str:
                continue

            try:
                count = int(count_str)
            except ValueError:
                continue

            if not _is_safe_relative(rel_str, self.repo_root):
                if console:
                    console.print(
                        f"[yellow]Ignoring unsafe allowlist entry (escapes repo root):[/yellow] {rel_str}"
                    )
                continue

            result[rel_str] = count

        return result

    def load(self) -> dict[str, int]:
        """Return mapping of ``relative_path -> allowed_count``."""
        if not self.allowlist_file.exists():
            return {}
        return self.parse(self.allowlist_file.read_text())

    def save(self, counts: dict[str, int]) -> None:
        lines = [f"{rel}::{count}" for rel, count in sorted(counts.items())]
        self.allowlist_file.write_text("\n".join(lines) + "\n")

    @abc.abstractmethod
    def iter_files(self) -> Iterable[Path]:
        """Return all files to scan during ``--generate`` or ``--all-files``."""

    @abc.abstractmethod
    def count_occurrences(self, path: Path) -> int:
        """Count the number of violations/occurrences in a single file."""

    @abc.abstractmethod
    def violation_panel_text(self) -> str:
        """Return the rich markup body for the violation help panel."""

    def format_violation_details(self, path: Path) -> list[str]:
        """Return extra detail lines for each violating file."""
        return []

    def check(self, files: list[Path], allowlist: dict[str, int]) -> int:
        """Run the check loop: compare counts, tighten entries, report violations."""
        violations: list[tuple[Path, int, int]] = []
        tightened: list[tuple[str, int, int]] = []

        for path in files:
            if not path.exists() or path.suffix != ".py":
                continue
            actual = self.count_occurrences(path)
            rel = str(path.relative_to(self.repo_root))
            allowed = allowlist.get(rel, 0)
            if actual > allowed:
                violations.append((path, actual, allowed))
            elif actual < allowed:
                if actual == 0:
                    del allowlist[rel]
                else:
                    allowlist[rel] = actual
                tightened.append((rel, allowed, actual))

        if tightened:
            self.save(allowlist)
            if console:
                console.print(
                    f"[green]Tightened {len(tightened)} entr{'y' if len(tightened) == 1 else 'ies'} "
                    f"in [cyan]{self.allowlist_file.relative_to(self.repo_root)}[/cyan][/green] "
                    "(stage the updated file):"
                )
                for rel, old, new in tightened:
                    console.print(f"  [cyan]{rel}[/cyan]  {old} → {new}")

        if violations:
            if console:
                console.print(
                    Panel.fit(
                        self.violation_panel_text(),
                        title="[red]Check failed[/red]",
                        border_style="red",
                    )
                )
                for path, actual, allowed in violations:
                    console.print(
                        f"  [cyan]{path.relative_to(self.repo_root)}[/cyan]  "
                        f"count={actual} (allowed={allowed})"
                    )
                    for detail in self.format_violation_details(path):
                        console.print(detail)
            return 1

        return 1 if tightened else 0

    def generate(self) -> int:
        if console:
            console.print(f"Scanning [cyan]{self.repo_root}[/cyan] …")
        counts: dict[str, int] = {}
        for path in self.iter_files():
            n = self.count_occurrences(path)
            if n > 0:
                counts[str(path.relative_to(self.repo_root))] = n

        self.save(counts)
        total = sum(counts.values())
        if console:
            console.print(
                f"[green]Generated[/green] [cyan]{self.allowlist_file.relative_to(self.repo_root)}[/cyan] "
                f"with [bold]{len(counts)}[/bold] files / [bold]{total}[/bold] occurrences."
            )
        return 0

    def cleanup(self) -> int:
        allowlist = self.load()
        if not allowlist:
            if console:
                console.print("[yellow]Allowlist is empty – nothing to clean up.[/yellow]")
            return 0

        stale: list[str] = [rel for rel in allowlist if not (self.repo_root / rel).exists()]
        if stale:
            if console:
                console.print(
                    f"[yellow]Removing {len(stale)} stale entr{'y' if len(stale) == 1 else 'ies'}:[/yellow]"
                )
                for s in sorted(stale):
                    console.print(f"  [dim]-[/dim] {s}")
            for s in stale:
                del allowlist[s]
            self.save(allowlist)
            if console:
                console.print(
                    f"\n[green]Updated[/green] [cyan]{self.allowlist_file.relative_to(self.repo_root)}[/cyan]"
                )
        else:
            if console:
                console.print("[green]No stale entries found.[/green]")
        return 0
