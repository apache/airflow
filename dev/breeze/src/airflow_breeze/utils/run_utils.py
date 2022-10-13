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

import contextlib
import os
import re
import shlex
import stat
import subprocess
import sys
from distutils.version import StrictVersion
from functools import lru_cache
from pathlib import Path
from threading import Thread
from typing import Mapping, Union

from rich.markup import escape

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

RunCommandResult = Union[subprocess.CompletedProcess, subprocess.CalledProcessError]

OPTION_MATCHER = re.compile(r"^[A-Z_]*=.*$")


def run_command(
    cmd: list[str],
    title: str | None = None,
    *,
    check: bool = True,
    verbose: bool = False,
    dry_run: bool = False,
    no_output_dump_on_exception: bool = False,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    input: str | None = None,
    output: Output | None = None,
    **kwargs,
) -> RunCommandResult:
    """
    Runs command passed as list of strings with some extra functionality over POpen (kwargs from PoPen can
    be used in this command even if not explicitly specified).

    It prints diagnostics when requested, also allows to "dry_run" the commands rather than actually
    execute them.

    An important factor for having this command running tool is to be able (in verbose mode) to directly
    copy&paste the verbose output and run the command manually - including all the environment variables
    needed to run the command.

    :param cmd: command to run
    :param title: optional title for the command (otherwise likely title is automatically determined)
    :param check: whether to check status value and run exception (same as POpem)
    :param verbose: print commands when running
    :param dry_run: do not execute "the" command - just print what would happen
    :param no_output_dump_on_exception: whether to suppress printing logs from output when command fails
    :param env: mapping of environment variables to set for the run command
    :param cwd: working directory to set for the command
    :param input: input string to pass to stdin of the process
    :param output: redirects stderr/stdout to Output if set to Output class.
    :param kwargs: kwargs passed to POpen
    """

    def exclude_command(_index: int, _arg: str) -> bool:
        if _index == 0:
            # First argument is always passed
            return False
        if _arg.startswith('-'):
            return True
        if len(_arg) == 0:
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
            for index, argument in enumerate(cmd)
            if not exclude_command(index, argument)
        ]
        # Heuristics to get a (possibly) short but explanatory title showing what the command does
        # If title is not provided explicitly
        title = "<" + ' '.join(shortened_command[:5]) + ">"  # max 4 args
    workdir: str = str(cwd) if cwd else os.getcwd()
    cmd_env = os.environ.copy()
    cmd_env.setdefault("HOME", str(Path.home()))
    if env:
        cmd_env.update(env)
    if output:
        if 'capture_output' not in kwargs or not kwargs['capture_output']:
            kwargs['stdout'] = output.file
            kwargs['stderr'] = subprocess.STDOUT
    command_to_print = ' '.join(shlex.quote(c) for c in cmd)
    env_to_print = get_environments_to_print(env)
    if not verbose and not dry_run:
        return subprocess.run(cmd, input=input, check=check, env=cmd_env, cwd=workdir, **kwargs)
    with ci_group(title=f"Running command: {title}", message_type=None):
        get_console(output=output).print(f"\n[info]Working directory {workdir}\n")
        if input:
            get_console(output=output).print("[info]Input:")
            get_console(output=output).print(input)
            get_console(output=output).print()
        # Soft wrap allows to copy&paste and run resulting output as it has no hard EOL
        get_console(output=output).print(
            f"\n[info]{env_to_print}{escape(command_to_print)}[/]\n", soft_wrap=True
        )
        if dry_run:
            return subprocess.CompletedProcess(cmd, returncode=0)
        try:
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
    env_to_print = ''.join(f'{key}="{val}" \\\n' for (key, val) in sorted(system_env.items()))
    env_to_print += r"""\
"""
    env_to_print += ''.join(f'{key}="{val}" \\\n' for (key, val) in sorted(my_env.items()))
    return env_to_print


def assert_pre_commit_installed(verbose: bool):
    """
    Check if pre-commit is installed in the right version.
    :param verbose: print commands when running
    :return: True is the pre-commit is installed in the right version.
    """
    # Local import to make autocomplete work
    import yaml

    pre_commit_config = yaml.safe_load((AIRFLOW_SOURCES_ROOT / ".pre-commit-config.yaml").read_text())
    min_pre_commit_version = pre_commit_config["minimum_pre_commit_version"]

    python_executable = sys.executable
    get_console().print(f"[info]Checking pre-commit installed for {python_executable}[/]")
    command_result = run_command(
        [python_executable, "-m", "pre_commit", "--version"],
        verbose=verbose,
        capture_output=True,
        text=True,
        check=False,
    )
    if command_result.returncode == 0:
        if command_result.stdout:
            pre_commit_version = command_result.stdout.split(" ")[-1].strip()
            if StrictVersion(pre_commit_version) >= StrictVersion(min_pre_commit_version):
                get_console().print(
                    f"\n[success]Package pre_commit is installed. "
                    f"Good version {pre_commit_version} (>= {min_pre_commit_version})[/]\n"
                )
            else:
                get_console().print(
                    f"\n[error]Package name pre_commit version is wrong. It should be"
                    f"aat least {min_pre_commit_version} and is {pre_commit_version}.[/]\n\n"
                )
                sys.exit(1)
        else:
            get_console().print(
                "\n[warning]Could not determine version of pre-commit. You might need to update it![/]\n"
            )
    else:
        get_console().print("\n[error]Error checking for pre-commit-installation:[/]\n")
        get_console().print(command_result.stderr)
        get_console().print("\nMake sure to run:\n      breeze setup self-upgrade\n\n")
        sys.exit(1)


def get_filesystem_type(filepath: str):
    """
    Determine the type of filesystem used - we might want to use different parameters if tmpfs is used.
    :param filepath: path to check
    :return: type of filesystem
    """
    # We import it locally so that click autocomplete works
    import psutil

    root_type = "unknown"
    for part in psutil.disk_partitions(all=True):
        if part.mountpoint == '/':
            root_type = part.fstype
            continue
        if filepath.startswith(part.mountpoint):
            return part.fstype

    return root_type


def instruct_build_image(python: str):
    """Print instructions to the user that they should build the image"""
    get_console().print(f'[warning]\nThe CI image for Python version {python} may be outdated[/]\n')
    get_console().print(
        f"\n[info]Please run at the earliest "
        f"convenience:[/]\n\nbreeze ci-image build --python {python}\n\n"
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


@working_directory(AIRFLOW_SOURCES_ROOT)
def fix_group_permissions(verbose: bool):
    """Fixes permissions of all the files and directories that have group-write access."""
    if verbose:
        get_console().print("[info]Fixing group permissions[/]")
    files_to_fix_result = run_command(['git', 'ls-files', './'], capture_output=True, text=True)
    if files_to_fix_result.returncode == 0:
        files_to_fix = files_to_fix_result.stdout.strip().split('\n')
        for file_to_fix in files_to_fix:
            change_file_permission(Path(file_to_fix))
    directories_to_fix_result = run_command(
        ['git', 'ls-tree', '-r', '-d', '--name-only', 'HEAD'], capture_output=True, text=True
    )
    if directories_to_fix_result.returncode == 0:
        directories_to_fix = directories_to_fix_result.stdout.strip().split('\n')
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
    command_result = run_command(['git', 'log', '--format=format:%H'], capture_output=True, text=True)
    commit_list = command_result.stdout.strip().splitlines() if command_result is not None else "missing"
    if latest_sha in commit_list:
        rebased = True
    return rebased


def check_if_buildx_plugin_installed(verbose: bool) -> bool:
    """
    Checks if buildx plugin is locally available.
    :param verbose: print commands when running
    :return True if the buildx plugin is installed.
    """
    check_buildx = ['docker', 'buildx', 'version']
    docker_buildx_version_result = run_command(
        check_buildx,
        verbose=verbose,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    if docker_buildx_version_result.returncode == 0:
        return True
    return False


@lru_cache(maxsize=None)
def commit_sha():
    """Returns commit SHA of current repo. Cached for various usages."""
    command_result = run_command(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, check=False)
    if command_result.stdout:
        return command_result.stdout.strip()
    else:
        return "COMMIT_SHA_NOT_FOUND"


def filter_out_none(**kwargs) -> dict:
    """Filters out all None values from parameters passed."""
    for key in list(kwargs):
        if kwargs[key] is None:
            kwargs.pop(key)
    return kwargs


def check_if_image_exists(image: str, verbose: bool, dry_run: bool) -> bool:
    cmd_result = run_command(
        ["docker", "inspect", image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        check=False,
        verbose=verbose,
        dry_run=dry_run,
    )
    return cmd_result.returncode == 0


def get_ci_image_for_pre_commits(verbose: bool, dry_run: bool) -> str:
    github_repository = os.environ.get('GITHUB_REPOSITORY', APACHE_AIRFLOW_GITHUB_REPOSITORY)
    python_version = "3.7"
    airflow_image = f"ghcr.io/{github_repository}/{AIRFLOW_BRANCH}/ci/python{python_version}"
    skip_image_pre_commits = os.environ.get('SKIP_IMAGE_PRE_COMMITS', "false")
    if skip_image_pre_commits[0].lower() == "t":
        get_console().print(
            f"[info]Skipping image check as SKIP_IMAGE_PRE_COMMITS is set to {skip_image_pre_commits}[/]"
        )
        sys.exit(0)
    if not check_if_image_exists(
        image=airflow_image,
        verbose=verbose,
        dry_run=dry_run,
    ):
        get_console().print(f'[red]The image {airflow_image} is not available.[/]\n')
        get_console().print(
            f"\n[yellow]Please run this to fix it:[/]\n\n"
            f"breeze ci-image build --python {python_version}\n\n"
        )
        sys.exit(1)
    return airflow_image


def _run_compile_internally(command_to_execute: list[str], dry_run: bool, verbose: bool):
    env = os.environ.copy()
    compile_www_assets_result = run_command(
        command_to_execute,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
        no_output_dump_on_exception=True,
        text=True,
        env=env,
    )
    return compile_www_assets_result


def run_compile_www_assets(
    dev: bool,
    run_in_background: bool,
    verbose: bool,
    dry_run: bool,
):
    if dev:
        get_console().print("\n[warning] The command below will run forever until you press Ctrl-C[/]\n")
        get_console().print(
            "\n[info]If you want to see output of the compilation command,\n"
            "[info]cancel it, go to airflow/www folder and run 'yarn dev'.\n"
            "[info]However, it requires you to have local yarn installation.\n"
        )
    command_to_execute = [
        sys.executable,
        "-m",
        "pre_commit",
        'run',
        "--hook-stage",
        "manual",
        'compile-www-assets-dev' if dev else 'compile-www-assets',
        '--all-files',
    ]
    if run_in_background:
        thread = Thread(
            daemon=True, target=_run_compile_internally, args=(command_to_execute, dry_run, verbose)
        )
        thread.start()
    else:
        return _run_compile_internally(command_to_execute, dry_run, verbose)
