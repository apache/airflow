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
Some of the arguments ("Python/Backend/Versions of the backend) are cached locally in
".build" folder so that the last used value is used in the subsequent run if not specified.

This allows to not remember what was the last version of Python used, if you just want to enter
the shell with the same version as the "previous run".
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from airflow_breeze import global_constants
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


def check_if_cache_exists(param_name: str) -> bool:
    return (Path(BUILD_CACHE_DIR) / f".{param_name}").exists()


def read_from_cache_file(param_name: str) -> str | None:
    cache_exists = check_if_cache_exists(param_name)
    if cache_exists:
        return (Path(BUILD_CACHE_DIR) / f".{param_name}").read_text().strip()
    else:
        return None


def touch_cache_file(param_name: str, root_dir: Path = BUILD_CACHE_DIR):
    (Path(root_dir) / f".{param_name}").touch()


def write_to_cache_file(param_name: str, param_value: str, check_allowed_values: bool = True) -> None:
    """
    Writs value to cache. If asked it can also check if the value is allowed for the parameter. and exit
    in case the value is not allowed for that parameter instead of writing it.
    :param param_name: name of the parameter
    :param param_value: new value for the parameter
    :param check_allowed_values: whether to fail if the parameter value is not allowed for that name.
    """
    allowed = False
    allowed_values = None
    if check_allowed_values:
        allowed, allowed_values = check_if_values_allowed(param_name, param_value)
    if allowed or not check_allowed_values:
        cache_path = Path(BUILD_CACHE_DIR, f".{param_name}")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(param_value)
    else:
        get_console().print(f"[cyan]You have sent the {param_value} for {param_name}")
        get_console().print(f"[cyan]Allowed value for the {param_name} are {allowed_values}")
        get_console().print("[cyan]Provide one of the supported params. Write to cache dir failed")
        sys.exit(1)


def read_and_validate_value_from_cache(param_name: str, default_param_value: str) -> tuple[bool, str | None]:
    """
    Reads and validates value from cache is present and whether its value is valid according to current rules.
    It could happen that the allowed values have been modified since the last time cached value was set,
    so this check is crucial to check outdated values.
    If the value is not set or in case the cached value stored is not currently allowed,
    the default value is stored in the cache and returned instead.

    :param param_name: name of the parameter
    :param default_param_value: default value of the parameter
    :return: Tuple informing whether the value was read from cache and the parameter value that is
         set in the cache after this method returns.
    """
    is_from_cache = False
    cached_value = read_from_cache_file(param_name)
    if cached_value is None:
        write_to_cache_file(param_name, default_param_value)
        cached_value = default_param_value
    else:
        allowed, allowed_values = check_if_values_allowed(param_name, cached_value)
        if allowed:
            is_from_cache = True
        else:
            write_to_cache_file(param_name, default_param_value)
            cached_value = default_param_value
    return is_from_cache, cached_value


def check_if_values_allowed(param_name: str, param_value: str) -> tuple[bool, list[Any]]:
    """Checks if parameter value is allowed by looking at global constants."""
    allowed = False
    allowed_values = getattr(global_constants, f"ALLOWED_{param_name.upper()}S")
    if param_value in allowed_values:
        allowed = True
    return allowed, allowed_values


def delete_cache(param_name: str) -> bool:
    """Deletes value from cache. Returns true if the delete operation happened (i.e. cache was present)."""
    deleted = False
    if check_if_cache_exists(param_name):
        (Path(BUILD_CACHE_DIR) / f".{param_name}").unlink()
        deleted = True
    return deleted
