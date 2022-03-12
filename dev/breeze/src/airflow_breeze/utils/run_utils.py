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

import contextlib
import hashlib
import os
import shlex
import shutil
import stat
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import psutil
import requests

from airflow_breeze.cache import update_md5checksum_in_cache
from airflow_breeze.console import console
from airflow_breeze.global_constants import FILES_FOR_REBUILD_CHECK
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCE


def run_command(
    cmd: List[str],
    *,
    check: bool = True,
    verbose: bool = False,
    suppress_raise_exception: bool = False,
    suppress_console_print: bool = False,
    env: Optional[Mapping[str, str]] = None,
    cwd: Optional[Path] = None,
    **kwargs,
):
    workdir: str = str(cwd) if cwd else os.getcwd()
    if verbose:
        command_to_print = ' '.join(shlex.quote(c) for c in cmd)
        # if we pass environment variables to execute, then
        env_to_print = ' '.join(f'{key}="{val}"' for (key, val) in env.items()) if env else ''
        console.print(f"\n[blue]Working directory {workdir} [/]\n")
        # Soft wrap allows to copy&paste and run resulting output as it has no hard EOL
        console.print(f"\n[blue]{env_to_print} {command_to_print}[/]\n", soft_wrap=True)

    try:
        # copy existing environment variables
        cmd_env = deepcopy(os.environ)
        if env:
            # Add environment variables passed as parameters
            cmd_env.update(env)
        return subprocess.run(cmd, check=check, env=cmd_env, cwd=workdir, **kwargs)
    except subprocess.CalledProcessError as ex:
        if not suppress_console_print:
            console.print("========================= OUTPUT start ============================")
            console.print(ex.stderr)
            console.print(ex.stdout)
            console.print("========================= OUTPUT end ============================")
        if not suppress_raise_exception:
            raise


def generate_md5(filename, file_size: int = 65536):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for file_chunk in iter(lambda: f.read(file_size), b""):
            hash_md5.update(file_chunk)
    return hash_md5.hexdigest()


def filter_out_none(**kwargs) -> Dict:
    for key in list(kwargs):
        if kwargs[key] is None:
            kwargs.pop(key)
    return kwargs


def check_package_installed(package_name: str) -> bool:
    is_installed = False
    if shutil.which('pre-commit') is not None:
        is_installed = True
        console.print(f"\n[blue]Package name {package_name} is installed to run static check test")
    else:
        console.print(
            f"\n[red]Error: Package name {package_name} is not installed. \
            Please install using https://pre-commit.com/#install to continue[/]\n"
        )
    return is_installed


def get_filesystem_type(filepath):
    root_type = "unknown"
    for part in psutil.disk_partitions():
        if part.mountpoint == '/':
            root_type = part.fstype
            continue
        if filepath.startswith(part.mountpoint):
            return part.fstype

    return root_type


def calculate_md5_checksum_for_files(md5sum_cache_dir: Path):
    not_modified_files = []
    modified_files = []
    for calculate_md5_file in FILES_FOR_REBUILD_CHECK:
        file_to_get_md5 = Path(AIRFLOW_SOURCE, calculate_md5_file)
        md5_checksum = generate_md5(file_to_get_md5)
        sub_dir_name = file_to_get_md5.parts[-2]
        actual_file_name = file_to_get_md5.parts[-1]
        cache_file_name = Path(md5sum_cache_dir, sub_dir_name + '-' + actual_file_name + '.md5sum')
        file_content = md5_checksum + '  ' + str(file_to_get_md5) + '\n'
        is_modified = update_md5checksum_in_cache(file_content, cache_file_name)
        if is_modified:
            modified_files.append(calculate_md5_file)
        else:
            not_modified_files.append(calculate_md5_file)
    return modified_files, not_modified_files


def md5sum_check_if_build_is_needed(md5sum_cache_dir: Path, the_image_type: str) -> bool:
    build_needed = False
    modified_files, not_modified_files = calculate_md5_checksum_for_files(md5sum_cache_dir)
    if len(modified_files) > 0:
        console.print('The following files are modified: ', modified_files)
        console.print(f'Likely {the_image_type} image needs rebuild')
        build_needed = True
    else:
        console.print(
            f'Docker image build is not needed for {the_image_type} build as no important files are changed!'
        )
    return build_needed


def instruct_build_image(the_image_type: str, python_version: str):
    console.print(f'\nThe {the_image_type} image for python version {python_version} may be outdated\n')
    console.print('Please run this command at earliest convenience:\n')
    if the_image_type == 'CI':
        console.print(f'./Breeze2 build-ci-image --python {python_version}')
    else:
        console.print(f'./Breeze2 build-prod-image --python {python_version}')
    console.print("\nIf you run it via pre-commit as individual hook, you can run 'pre-commit run build'.\n")


def instruct_for_setup():
    CMDNAME = 'Breeze2'
    console.print(f"\nYou can setup autocomplete by running {CMDNAME} setup-autocomplete'")
    console.print("  You can toggle ascii/cheatsheet by running:")
    console.print(f"      * {CMDNAME} toggle-suppress-cheatsheet")
    console.print(f"      * {CMDNAME} toggle-suppress-asciiart\n")


@contextlib.contextmanager
def working_directory(source_path: Path):
    # Equivalent of pushd and popd in bash script.
    # https://stackoverflow.com/a/42441759/3101838
    prev_cwd = Path.cwd()
    os.chdir(source_path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def change_file_permission(file_to_fix: Path):
    if file_to_fix.exists():
        current = stat.S_IMODE(os.stat(file_to_fix).st_mode)
        new = current & ~stat.S_IWGRP & ~stat.S_IWOTH  # Removes group/other write permission
        os.chmod(file_to_fix, new)


def change_directory_permission(directory_to_fix: Path):
    if directory_to_fix.exists():
        current = stat.S_IMODE(os.stat(directory_to_fix).st_mode)
        new = current & ~stat.S_IWGRP & ~stat.S_IWOTH  # Removes group/other write permission
        new = (
            new | stat.S_IXGRP | stat.S_IXOTH
        )  # Add group/other execute permission (to be able to list directories)
        os.chmod(directory_to_fix, new)


@working_directory(AIRFLOW_SOURCE)
def fix_group_permissions():
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


def get_latest_sha(repo: str, branch: str):
    gh_url = f"https://api.github.com/repos/{repo}/commits/{branch}"
    headers_dict = {"Accept": "application/vnd.github.VERSION.sha"}
    resp = requests.get(gh_url, headers=headers_dict)
    return resp.text


def is_repo_rebased(latest_sha: str):
    rebased = False
    output = run_command(['git', 'log', '--format=format:%H'], capture_output=True, text=True)
    output = output.stdout.strip().splitlines()
    if latest_sha in output:
        rebased = True
    return rebased
