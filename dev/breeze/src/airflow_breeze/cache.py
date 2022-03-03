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

import sys
from pathlib import Path
from typing import Any, List, Optional, Tuple

from airflow_breeze import global_constants
from airflow_breeze.console import console
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


def check_if_cache_exists(param_name: str) -> bool:
    return (Path(BUILD_CACHE_DIR) / f".{param_name}").exists()


def read_from_cache_file(param_name: str) -> Optional[str]:
    cache_exists = check_if_cache_exists(param_name)
    if cache_exists:
        return (Path(BUILD_CACHE_DIR) / f".{param_name}").read_text().strip()
    else:
        return None


def touch_cache_file(param_name: str, root_dir: Path = BUILD_CACHE_DIR):
    (Path(root_dir) / f".{param_name}").touch()


def write_to_cache_file(param_name: str, param_value: str, check_allowed_values: bool = True) -> None:
    allowed = False
    allowed_values = None
    if check_allowed_values:
        allowed, allowed_values = check_if_values_allowed(param_name, param_value)
    if allowed or not check_allowed_values:
        print('BUILD CACHE DIR:', BUILD_CACHE_DIR)
        cache_file = Path(BUILD_CACHE_DIR, f".{param_name}").open("w+")
        cache_file.write(param_value)
    else:
        console.print(f'[cyan]You have sent the {param_value} for {param_name}')
        console.print(f'[cyan]Allowed value for the {param_name} are {allowed_values}')
        console.print('[cyan]Provide one of the supported params. Write to cache dir failed')
        sys.exit()


def check_cache_and_write_if_not_cached(
    param_name: str, default_param_value: str
) -> Tuple[bool, Optional[str]]:
    is_cached = False
    cached_value = read_from_cache_file(param_name)
    if cached_value is None:
        write_to_cache_file(param_name, default_param_value)
        cached_value = default_param_value
    else:
        allowed, allowed_values = check_if_values_allowed(param_name, cached_value)
        if allowed:
            is_cached = True
        else:
            write_to_cache_file(param_name, default_param_value)
            cached_value = default_param_value
    return is_cached, cached_value


def check_if_values_allowed(param_name: str, param_value: str) -> Tuple[bool, List[Any]]:
    allowed = False
    allowed_values: List[Any] = []
    allowed_values = getattr(global_constants, f'ALLOWED_{param_name.upper()}S')
    if param_value in allowed_values:
        allowed = True
    return allowed, allowed_values


def delete_cache(param_name: str) -> bool:
    deleted = False
    if check_if_cache_exists(param_name):
        (Path(BUILD_CACHE_DIR) / f".{param_name}").unlink()
        deleted = True
    return deleted


def update_md5checksum_in_cache(file_content: str, cache_file_name: Path) -> bool:
    modified = False
    if cache_file_name.exists():
        old_md5_checksum_content = Path(cache_file_name).read_text()
        if old_md5_checksum_content.strip() != file_content.strip():
            Path(cache_file_name).write_text(file_content)
            modified = True
    else:
        Path(cache_file_name).write_text(file_content)
        modified = True
    return modified


def write_env_in_cache(env_variables) -> Path:
    shell_path = Path(BUILD_CACHE_DIR, "shell_command.env")
    with open(shell_path, 'w') as shell_env_file:
        for env_variable in env_variables:
            shell_env_file.write(env_variable + '\n')
    return shell_path
