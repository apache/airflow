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
"""
Utilities to check - with MD5 - whether files have been modified since the last successful build.
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

from airflow_breeze.global_constants import ALL_PROVIDER_YAML_FILES, FILES_FOR_REBUILD_CHECK
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command


def check_md5checksum_in_cache_modified(file_hash: str, cache_path: Path, update: bool) -> bool:
    """
    Check if the file hash is present in cache and its content has been modified. Optionally updates
    the hash.

    :param file_hash: hash of the current version of the file
    :param cache_path: path where the hash is stored
    :param update: whether to update hash if it is found different
    :return: True if the hash file was missing or hash has changed.
    """
    if cache_path.exists():
        old_md5_checksum_content = Path(cache_path).read_text()
        if old_md5_checksum_content.strip() != file_hash.strip():
            if update:
                save_md5_file(cache_path, file_hash)
            return True
    else:
        if update:
            save_md5_file(cache_path, file_hash)
        return True
    return False


def generate_md5(filename, file_size: int = 65536):
    """Generates md5 hash for the file."""
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for file_chunk in iter(lambda: f.read(file_size), b""):
            hash_md5.update(file_chunk)
    return hash_md5.hexdigest()


def check_md5_sum_for_file(file_to_check: str, md5sum_cache_dir: Path, update: bool):
    file_to_get_md5 = AIRFLOW_SOURCES_ROOT / file_to_check
    md5_checksum = generate_md5(file_to_get_md5)
    sub_dir_name = file_to_get_md5.parts[-2]
    actual_file_name = file_to_get_md5.parts[-1]
    cache_file_name = Path(md5sum_cache_dir, sub_dir_name + "-" + actual_file_name + ".md5sum")
    file_content = md5_checksum + "  " + str(file_to_get_md5) + "\n"
    is_modified = check_md5checksum_in_cache_modified(file_content, cache_file_name, update=update)
    return is_modified


def calculate_md5_checksum_for_files(
    md5sum_cache_dir: Path, update: bool = False, skip_provider_dependencies_check: bool = False
) -> tuple[list[str], list[str]]:
    """
    Calculates checksums for all interesting files and stores the hashes in the md5sum_cache_dir.
    Optionally modifies the hashes.

    :param md5sum_cache_dir: directory where to store cached information
    :param update: whether to update the hashes
    :param skip_provider_dependencies_check: whether to skip regeneration of the provider dependencies
    :return: Tuple of two lists: modified and not-modified files
    """
    not_modified_files = []
    modified_files = []
    if not skip_provider_dependencies_check:
        modified_provider_yaml_files = []
        for file in ALL_PROVIDER_YAML_FILES:
            # Only check provider yaml files once and save the result immediately.
            # If we need to regenerate the dependencies and they are not modified then
            # all is fine and we can save checksums for the new files
            if check_md5_sum_for_file(file, md5sum_cache_dir, True):
                modified_provider_yaml_files.append(file)
        if modified_provider_yaml_files:
            get_console().print(
                "[info]Attempting to generate provider dependencies. "
                "Provider yaml files changed since last check:[/]"
            )
            get_console().print(
                [os.fspath(file.relative_to(AIRFLOW_SOURCES_ROOT)) for file in modified_provider_yaml_files]
            )
            # Regenerate provider_dependencies.json
            run_command(
                [
                    sys.executable,
                    os.fspath(
                        AIRFLOW_SOURCES_ROOT
                        / "scripts"
                        / "ci"
                        / "pre_commit"
                        / "pre_commit_update_providers_dependencies.py"
                    ),
                ],
                cwd=AIRFLOW_SOURCES_ROOT,
            )
    for file in FILES_FOR_REBUILD_CHECK:
        is_modified = check_md5_sum_for_file(file, md5sum_cache_dir, update)
        if is_modified:
            modified_files.append(file)
        else:
            not_modified_files.append(file)
    return modified_files, not_modified_files


def md5sum_check_if_build_is_needed(md5sum_cache_dir: Path, skip_provider_dependencies_check: bool) -> bool:
    """
    Checks if build is needed based on whether important files were modified.

    :param md5sum_cache_dir: directory where cached md5 sums are stored
    :param skip_provider_dependencies_check: whether to skip regeneration of the provider dependencies

    :return: True if build is needed.
    """
    build_needed = False
    modified_files, not_modified_files = calculate_md5_checksum_for_files(
        md5sum_cache_dir, update=False, skip_provider_dependencies_check=skip_provider_dependencies_check
    )
    if modified_files:
        get_console().print(
            f"[warning]The following important files are modified in {AIRFLOW_SOURCES_ROOT} "
            f"since last time image was built: [/]\n\n"
        )
        for file in modified_files:
            get_console().print(f" * [info]{file}[/]")
        get_console().print("\n[warning]Likely CI image needs rebuild[/]\n")
        build_needed = True
    else:
        get_console().print(
            "[info]Docker image build is not needed for CI build as no important files are changed! "
            "You can add --force-build to force it[/]"
        )
    return build_needed


def save_md5_file(cache_path: Path, file_content: str) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(file_content)
