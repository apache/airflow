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
import contextlib
import os
import shlex
import stat
import subprocess
import sys
from distutils.version import StrictVersion
from functools import lru_cache
from pathlib import Path
from re import match
from typing import Dict, Generator, List, Mapping, Optional, Union

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

RunCommandResult = Union[subprocess.CompletedProcess, subprocess.CalledProcessError]


def run_command(
    cmd: List[str],
    title: Optional[str] = None,
    *,
    check: bool = True,
    verbose: bool = False,
    dry_run: bool = False,
    no_output_dump_on_exception: bool = False,
    env: Optional[Mapping[str, str]] = None,
    cwd: Optional[Path] = None,
    input: Optional[str] = None,
    enabled_output_group: bool = False,
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
    :param enabled_output_group: if set to true, in CI the logs will be placed in separate, foldable group.
    :param kwargs: kwargs passed to POpen
    """
    if not title:
        # Heuristics to get a short but explanatory title showing what the command does
        # If title is not provided explicitly
        title = ' '.join(
            shlex.quote(c)
            for c in cmd
            if not c.startswith('-')  # exclude options
            and len(c) > 0
            and (c[0] != "/" or c.endswith(".sh"))  # exclude volumes
            and not c == "never"  # exclude --pull never
            and not match(r"^[A-Z_]*=.*$", c)
        )
    workdir: str = str(cwd) if cwd else os.getcwd()
    if verbose or dry_run:
        command_to_print = ' '.join(shlex.quote(c) for c in cmd)
        env_to_print = get_environments_to_print(env)
        with ci_group(title=f"Running {title}"):
            get_console().print(f"\n[info]Working directory {workdir}\n")
            if input:
                get_console().print("[info]Input:")
                get_console().print(input)
                get_console().print()
            # Soft wrap allows to copy&paste and run resulting output as it has no hard EOL
            get_console().print(f"\n[info]{env_to_print}{command_to_print}[/]\n", soft_wrap=True)
        if dry_run:
            return subprocess.CompletedProcess(cmd, returncode=0)
    try:
        cmd_env = os.environ.copy()
        cmd_env.setdefault("HOME", str(Path.home()))
        if env:
            cmd_env.update(env)
        with ci_group(title=f"Output of {title}", enabled=enabled_output_group):
            return subprocess.run(cmd, input=input, check=check, env=cmd_env, cwd=workdir, **kwargs)
    except subprocess.CalledProcessError as ex:
        if not no_output_dump_on_exception:
            if ex.stdout:
                get_console().print(
                    "[info]========================= OUTPUT start ============================[/]"
                )
                get_console().print(ex.stdout)
                get_console().print(
                    "[info]========================= OUTPUT end ==============================[/]"
                )
            if ex.stderr:
                get_console().print(
                    "[error]========================= STDERR start ============================[/]"
                )
                get_console().print(ex.stderr)
                get_console().print(
                    "[error]========================= STDERR end ==============================[/]"
                )
        if check:
            raise
        return ex


def get_environments_to_print(env: Optional[Mapping[str, str]]):
    if not env:
        return ""
    system_env: Dict[str, str] = {}
    my_env: Dict[str, str] = {}
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
        get_console().print("\nMake sure to run:\n      breeze self-upgrade\n\n")
        sys.exit(1)


def get_filesystem_type(filepath):
    """
    Determine the type of filesystem used - we might want to use different parameters if tmpfs is used.
    :param filepath: path to check
    :return: type of filesystem
    """
    # We import it locally so that click autocomplete works
    import psutil

    root_type = "unknown"
    for part in psutil.disk_partitions():
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
        f"\n[info]Please run at the earliest convenience:[/]\n\nbreeze build-image --python {python}\n\n"
    )


@contextlib.contextmanager
def working_directory(source_path: Path) -> Generator[None, None, None]:
    """
    # Equivalent of pushd and popd in bash script.
    # https://stackoverflow.com/a/42441759/3101838
    :param source_path:
    :return:
    """
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
    """Returns True if the local branch contains latest remote SHA (i.e. if it is rebased)"""
    # We import it locally so that click autocomplete works
    import requests

    gh_url = f"https://api.github.com/repos/{repo}/commits/{branch}"
    headers_dict = {"Accept": "application/vnd.github.VERSION.sha"}
    latest_sha = requests.get(gh_url, headers=headers_dict).text.strip()
    rebased = False
    command_result = run_command(['git', 'log', '--format=format:%H'], capture_output=True, text=True)
    output = command_result.stdout.strip().splitlines() if command_result is not None else "missing"
    if latest_sha in output:
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


def fail_if_image_missing(image: str, verbose: bool, dry_run: bool, instruction: str) -> None:
    skip_image_pre_commits = os.environ.get('SKIP_IMAGE_PRE_COMMITS', "false")
    if skip_image_pre_commits[0].lower() == "t":
        get_console().print(
            f"[info]Skipping image check as SKIP_IMAGE_PRE_COMMITS is set to {skip_image_pre_commits}[/]"
        )
        sys.exit(0)
    cmd_result = run_command(
        ["docker", "inspect", image], stdout=subprocess.DEVNULL, check=False, verbose=verbose, dry_run=dry_run
    )
    if cmd_result.returncode != 0:
        print(f'[red]The image {image} is not available.[/]\n')
        print(f"\n[yellow]Please run at the earliest convenience:[/]\n\n{instruction}\n\n")
        sys.exit(1)


def get_runnable_ci_image(verbose: bool, dry_run: bool) -> str:
    github_repository = os.environ.get('GITHUB_REPOSITORY', "apache/airflow")
    python_version = "3.7"
    airflow_image = f"ghcr.io/{github_repository}/{AIRFLOW_BRANCH}/ci/python{python_version}"
    fail_if_image_missing(
        image=airflow_image,
        verbose=verbose,
        dry_run=dry_run,
        instruction=f"breeze build-image --python {python_version}",
    )
    return airflow_image


def run_compile_www_assets(
    verbose: bool,
    dry_run: bool,
):
    from airflow_breeze.utils.docker_command_utils import perform_environment_checks

    assert_pre_commit_installed(verbose=verbose)
    perform_environment_checks(verbose=verbose)
    command_to_execute = [
        sys.executable,
        "-m",
        "pre_commit",
        'run',
        "--hook-stage",
        "manual",
        'compile-www-assets',
        '--all-files',
    ]
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
