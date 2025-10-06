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
"""Useful tools for running commands."""

from __future__ import annotations

import atexit
import contextlib
import os
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from typing import Any

from rich.markup import escape

from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.functools_cache import clearable_cache
from airflow_breeze.utils.path_utils import (
    AIRFLOW_ROOT_PATH,
    UI_ASSET_COMPILE_LOCK,
    UI_ASSET_HASH_PATH,
    UI_ASSET_OUT_DEV_MODE_FILE,
    UI_ASSET_OUT_FILE,
    UI_DIST_PATH,
    UI_NODE_MODULES_PATH,
)
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

# RunCommandResult = type[subprocess.CompletedProcess] | type[subprocess.CalledProcessError]
RunCommandResult = CompletedProcess[Any] | CalledProcessError

OPTION_MATCHER = re.compile(r"^[A-Z_]*=.*$")


def run_command(
    cmd: list[str] | str,
    title: str | None = None,
    *,
    check: bool = True,
    no_output_dump_on_exception: bool = False,
    env: Mapping[str, str] | None = None,
    cwd: Path | str | None = None,
    input: str | bytes | None = None,
    output: Output | None = None,
    output_outside_the_group: bool = False,
    verbose_override: bool | None = None,
    dry_run_override: bool | None = None,
    quiet: bool = False,
    **kwargs,
) -> RunCommandResult:
    """
    Runs command passed as list of strings with some extra functionality over Popen (kwargs from Popen can
    be used in this command even if not explicitly specified).

    It prints diagnostics when requested, also allows to "dry_run" the commands rather than actually
    execute them.

    An important factor for having this command running tool is to be able (in verbose mode) to directly
    copy&paste the verbose output and run the command manually - including all the environment variables
    needed to run the command.

    :param cmd: command to run
    :param title: optional title for the command (otherwise likely title is automatically determined)
    :param check: whether to check status value and run exception (same as Popen)
    :param no_output_dump_on_exception: whether to suppress printing logs from output when command fails
    :param env: mapping of environment variables to set for the run command
    :param cwd: working directory to set for the command
    :param input: input string to pass to stdin of the process (bytes if text=False, str, otherwise)
    :param output: redirects stderr/stdout to Output if set to Output class.
    :param output_outside_the_group: if this is set to True, then output of the command will be done
        outside the "CI folded group" in CI - so that it is immediately visible without unfolding.
    :param verbose_override: override verbose parameter with the one specified if not None.
    :param dry_run_override: override dry_run parameter with the one specified if not None.
    :param quiet: if True, suppresses all output (including the command itself) and runs it in
    :param kwargs: kwargs passed to POpen
    """

    def exclude_command(_index: int, _arg: str) -> bool:
        if _index == 0:
            # First argument is always passed
            return False
        if _arg.startswith("-"):
            return True
        if not _arg:
            return True
        if _arg.startswith("/"):
            # Skip any absolute paths
            return True
        if _arg == "never":
            return True
        if OPTION_MATCHER.match(_arg):
            return True
        return False

    def shorten_command(_index: int, _argument: str) -> str:
        if _argument.startswith("/"):
            _argument = _argument.split("/")[-1]
        return shlex.quote(_argument)

    if not title:
        shortened_command = [
            shorten_command(index, argument)
            for index, argument in enumerate(cmd if isinstance(cmd, list) else shlex.split(cmd))
            if not exclude_command(index, argument)
        ]
        # Heuristics to get a (possibly) short but explanatory title showing what the command does
        # If title is not provided explicitly
        title = "<" + " ".join(shortened_command[:5]) + ">"  # max 4 args
    workdir: str = str(cwd) if cwd else os.getcwd()
    cmd_env = os.environ.copy()
    cmd_env.setdefault("HOME", str(Path.home()))
    if env:
        cmd_env.update(env)
    if output:
        if "capture_output" not in kwargs or not kwargs["capture_output"]:
            kwargs["stdout"] = output.file
            kwargs["stderr"] = subprocess.STDOUT
    command_to_print = " ".join(shlex.quote(c) for c in cmd) if isinstance(cmd, list) else cmd
    env_to_print = get_environments_to_print(env)
    if not get_verbose(verbose_override) and not get_dry_run(dry_run_override) or quiet:
        if quiet and not kwargs.get("capture_output"):
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL
        return subprocess.run(cmd, input=input, check=check, env=cmd_env, cwd=workdir, **kwargs)
    with ci_group(f"Running command: {title}"):
        get_console(output=output).print(f"\n[info]Working directory {workdir}\n")
        if input:
            get_console(output=output).print("[info]Input:")
            get_console(output=output).print(input)
            get_console(output=output).print()
        # Soft wrap allows to copy&paste and run resulting output as it has no hard EOL
        get_console(output=output).print(
            f"\n[info]{env_to_print}{escape(command_to_print)}[/]\n", soft_wrap=True
        )
        if get_dry_run(dry_run_override):
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")
        try:
            if output_outside_the_group and os.environ.get("GITHUB_ACTIONS") == "true":
                get_console().print("::endgroup::")
            return subprocess.run(cmd, input=input, check=check, env=cmd_env, cwd=workdir, **kwargs)
        except subprocess.CalledProcessError as ex:
            if no_output_dump_on_exception:
                if check:
                    raise
                return ex
            if ex.stdout:
                get_console(output=output).print(
                    "[info]========================= OUTPUT start ============================[/]"
                )
                get_console(output=output).print(ex.stdout)
                get_console(output=output).print(
                    "[info]========================= OUTPUT end ==============================[/]"
                )
            if ex.stderr:
                get_console(output=output).print(
                    "[error]========================= STDERR start ============================[/]"
                )
                get_console(output=output).print(ex.stderr)
                get_console(output=output).print(
                    "[error]========================= STDERR end ==============================[/]"
                )
            if check:
                raise
            return ex


def get_environments_to_print(env: Mapping[str, str] | None):
    if not env:
        return ""
    system_env: dict[str, str] = {}
    my_env: dict[str, str] = {}
    for key, val in env.items():
        if os.environ.get(key) == val:
            system_env[key] = val
        else:
            my_env[key] = val
    env_to_print = "".join(f'{key}="{val}" \\\n' for (key, val) in sorted(system_env.items()))
    env_to_print += r"""\
"""
    env_to_print += "".join(f'{key}="{val}" \\\n' for (key, val) in sorted(my_env.items()))
    return env_to_print


def assert_prek_installed():
    """
    Check if prek is installed in the right version.

    :return: True is the prek is installed in the right version.
    """
    # Local import to make autocomplete work
    import yaml
    from packaging.version import Version

    prek_config = yaml.safe_load((AIRFLOW_ROOT_PATH / ".pre-commit-config.yaml").read_text())
    min_prek_version = prek_config["minimum_prek_version"]

    python_executable = sys.executable
    get_console().print(f"[info]Checking prek installed for {python_executable}[/]")
    need_to_reinstall_prek = False
    try:
        command_result = run_command(
            ["prek", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
        if command_result.returncode == 0:
            if command_result.stdout:
                prek_version = command_result.stdout.split(" ")[1].strip()
                if Version(prek_version) >= Version(min_prek_version):
                    get_console().print(
                        f"\n[success]Package prek is installed. "
                        f"Good version {prek_version} (>= {min_prek_version})[/]\n"
                    )
                else:
                    get_console().print(
                        f"\n[error]Package name prek version is wrong. It should be "
                        f"at least {min_prek_version} and is {prek_version}.[/]\n\n"
                    )
                    sys.exit(1)
            else:
                get_console().print(
                    "\n[warning]Could not determine version of prek. You might need to update it![/]\n"
                )
        else:
            need_to_reinstall_prek = True
            get_console().print("\n[error]Error checking for prek-installation:[/]\n")
            get_console().print(command_result.stderr)
    except FileNotFoundError as e:
        need_to_reinstall_prek = True
        get_console().print(f"\n[error]Error checking for prek installation: [/]\n{e}\n")
    if need_to_reinstall_prek:
        get_console().print("[info]Make sure to install prek. For example by running:\n")
        get_console().print("   uv tool install prek\n")
        get_console().print("Or if you prefer pipx:\n")
        get_console().print("   pipx install prek")
        sys.exit(1)


def check_pnpm_installed():
    """
    Check if pnpm is installed and install it if npm is available.
    """
    if shutil.which("pnpm"):
        return

    get_console().print("[warning]pnpm is not installed. Installing pnpm...[/]")

    # Check if npm is available (required to install pnpm)
    if not shutil.which("npm"):
        get_console().print("[error]npm is not installed. Please install Node.js and npm first.[/]")
        get_console().print("[warning]Visit: https://nodejs.org/[/]")
        sys.exit(1)

    try:
        get_console().print("[info]Installing pnpm using npm...[/]")
        result = run_command(
            ["npm", "install", "-g", "pnpm"],
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            get_console().print("[success]pnpm has been installed successfully![/]")
        else:
            get_console().print(f"[error]Failed to install pnpm: {result.stderr}[/]")
            get_console().print("[warning]Please install pnpm manually: https://pnpm.io/installation[/]")
            sys.exit(1)
    except Exception as e:
        get_console().print(f"[error]Failed to install pnpm: {e}[/]")
        get_console().print("[warning]Please install pnpm manually: https://pnpm.io/installation[/]")
        sys.exit(1)


def get_filesystem_type(filepath: str):
    """
    Determine the type of filesystem used - we might want to use different parameters if tmpfs is used.
    :param filepath: path to check
    :return: type of filesystem
    """
    # We import it locally so that click autocomplete works
    try:
        import psutil
    except ImportError:
        return "unknown"

    root_type = "unknown"
    for part in psutil.disk_partitions(all=True):
        if part.mountpoint == "/":
            root_type = part.fstype
        elif filepath.startswith(part.mountpoint):
            return part.fstype

    return root_type


def instruct_build_image(python: str):
    """Print instructions to the user that they should build the image"""
    get_console().print(f"[warning]\nThe CI image for Python version {python} may be outdated[/]\n")
    get_console().print(
        f"\n[info]Please run at the earliest convenience:[/]\n\nbreeze ci-image build --python {python}\n\n"
    )


@contextlib.contextmanager
def working_directory(source_path: Path):
    prev_cwd = Path.cwd()
    os.chdir(source_path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def change_file_permission(file_to_fix: Path):
    """Update file permissions to not be group-writeable. Needed to solve cache invalidation problems."""
    if file_to_fix.exists():
        current = stat.S_IMODE(os.stat(file_to_fix).st_mode)
        new = current & ~stat.S_IWGRP & ~stat.S_IWOTH  # Removes group/other write permission
        os.chmod(file_to_fix, new)


def change_directory_permission(directory_to_fix: Path):
    """Update directory permissions to not be group-writeable. Needed to solve cache invalidation problems."""
    if directory_to_fix.exists():
        current = stat.S_IMODE(os.stat(directory_to_fix).st_mode)
        new = current & ~stat.S_IWGRP & ~stat.S_IWOTH  # Removes group/other write permission
        new = (
            new | stat.S_IXGRP | stat.S_IXOTH
        )  # Add group/other execute permission (to be able to list directories)
        os.chmod(directory_to_fix, new)


@working_directory(AIRFLOW_ROOT_PATH)
def fix_group_permissions():
    """Fixes permissions of all the files and directories that have group-write access."""
    if get_verbose():
        get_console().print("[info]Fixing group permissions[/]")
    files_to_fix_result = run_command(["git", "ls-files", "./"], capture_output=True, check=False, text=True)
    if files_to_fix_result.returncode == 0:
        files_to_fix = files_to_fix_result.stdout.strip().splitlines()
        for file_to_fix in files_to_fix:
            change_file_permission(Path(file_to_fix))
    directories_to_fix_result = run_command(
        ["git", "ls-tree", "-r", "-d", "--name-only", "HEAD"], capture_output=True, check=False, text=True
    )
    if directories_to_fix_result.returncode == 0:
        directories_to_fix = directories_to_fix_result.stdout.strip().splitlines()
        for directory_to_fix in directories_to_fix:
            change_directory_permission(Path(directory_to_fix))


def is_repo_rebased(repo: str, branch: str):
    """Returns True if the local branch contains the latest remote SHA (i.e. if it is rebased)"""
    # We import it locally so that click autocomplete works
    import requests

    gh_url = f"https://api.github.com/repos/{repo}/commits/{branch}"
    headers_dict = {"Accept": "application/vnd.github.VERSION.sha"}
    latest_sha = requests.get(gh_url, headers=headers_dict).text.strip()
    rebased = False
    command_result = run_command(["git", "log", "--format=format:%H"], capture_output=True, text=True)
    commit_list = command_result.stdout.strip().splitlines() if command_result is not None else "missing"
    if latest_sha in commit_list:
        rebased = True
    return rebased


def check_if_buildx_plugin_installed() -> bool:
    """
    Checks if buildx plugin is locally available.

    :return True if the buildx plugin is installed.
    """
    check_buildx = ["docker", "buildx", "version"]
    docker_buildx_version_result = run_command(
        check_buildx,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    if docker_buildx_version_result.returncode == 0:
        return True
    return False


@clearable_cache
def commit_sha():
    """Returns commit SHA of current repo. Cached for various usages."""
    command_result = run_command(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    if command_result.stdout:
        return command_result.stdout.strip()
    return "COMMIT_SHA_NOT_FOUND"


def check_if_image_exists(image: str) -> bool:
    cmd_result = run_command(
        ["docker", "inspect", image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return cmd_result.returncode == 0


def _run_compile_internally(
    command_to_execute: list[str], dev: bool, compile_lock: Path, asset_out: Path
) -> RunCommandResult:
    from filelock import SoftFileLock, Timeout

    env = os.environ.copy()
    if dev:
        return run_command(
            command_to_execute,
            check=False,
            no_output_dump_on_exception=True,
            text=True,
            env=env,
        )
    compile_lock.parent.mkdir(parents=True, exist_ok=True)
    compile_lock.unlink(missing_ok=True)
    try:
        with SoftFileLock(compile_lock, timeout=5):
            with open(asset_out, "w") as output_file:
                result = run_command(
                    command_to_execute,
                    check=False,
                    no_output_dump_on_exception=True,
                    text=True,
                    env=env,
                    stderr=subprocess.STDOUT,
                    stdout=output_file,
                )
            if result.returncode == 0:
                asset_out.unlink(missing_ok=True)
            return result
    except Timeout:
        get_console().print("[error]Another asset compilation is running. Exiting[/]\n")
        get_console().print("[warning]If you are sure there is no other compilation,[/]")
        get_console().print("[warning]Remove the lock file and re-run compilation:[/]")
        get_console().print(compile_lock)
        get_console().print()
        sys.exit(1)


def kill_process_group(gid: int):
    """
    Kills all processes in the process group and ignore if the group is missing.

    :param gid: process group id
    """
    with contextlib.suppress(OSError):
        os.killpg(gid, signal.SIGTERM)


def clean_ui_assets():
    get_console().print("[info]Cleaning ui assets[/]")
    UI_ASSET_HASH_PATH.unlink(missing_ok=True)
    shutil.rmtree(UI_NODE_MODULES_PATH, ignore_errors=True)
    shutil.rmtree(UI_DIST_PATH, ignore_errors=True)
    get_console().print("[success]Cleaned ui assets[/]")


def run_compile_ui_assets(
    dev: bool,
    run_in_background: bool,
    force_clean: bool,
    additional_ui_hooks: list[str],
):
    if force_clean:
        clean_ui_assets()
    if dev:
        get_console().print("\n[warning] The command below will run forever until you press Ctrl-C[/]\n")
        get_console().print(
            "\n[info]If you want to see output of the compilation command,\n"
            "[info]cancel it, go to airflow/ui folder and run 'pnpm dev'.\n"
            "[info]However, it requires you to have local pnpm installation.\n"
        )
    command_to_execute = [
        "prek",
        "run",
        "--hook-stage",
        "manual",
        "compile-ui-assets-dev" if dev else "compile-ui-assets",
        *additional_ui_hooks,
        "--all-files",
        "--verbose",
    ]
    get_console().print(
        "[info]The output of the asset compilation is stored in: [/]"
        f"{UI_ASSET_OUT_DEV_MODE_FILE if dev else UI_ASSET_OUT_FILE}\n"
    )
    if run_in_background:
        pid = os.fork()
        if pid:
            # Parent process - send signal to process group of the child process
            atexit.register(kill_process_group, pid)
        else:
            # Check if we are not a group leader already (We should not be)
            if os.getpid() != os.getsid(0):
                # and create a new process group where we are the leader
                os.setpgid(0, 0)
            _run_compile_internally(command_to_execute, dev, UI_ASSET_COMPILE_LOCK, UI_ASSET_OUT_FILE)
            sys.exit(0)
    else:
        return _run_compile_internally(command_to_execute, dev, UI_ASSET_COMPILE_LOCK, UI_ASSET_OUT_FILE)
