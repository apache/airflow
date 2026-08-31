#
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
"""Dag file utilities for finding and loading Dag files."""

from __future__ import annotations

import hashlib
import re
import zipfile
from collections.abc import Callable
from pathlib import Path

UNUSUAL_MODULE_PREFIX = "unusual_prefix_"
MODIFIED_DAG_MODULE_NAME = f"{UNUSUAL_MODULE_PREFIX}{{path_hash}}_{{module_name}}"


def get_unique_dag_module_name(file_path: str) -> str:
    """Return a unique module name in the format unusual_prefix_{sha1 of module's file path}_{original module name}."""
    if isinstance(file_path, str):
        path_hash = hashlib.sha1(file_path.encode("utf-8"), usedforsecurity=False).hexdigest()
        org_mod_name = re.sub(r"[.-]", "_", Path(file_path).stem)
        return MODIFIED_DAG_MODULE_NAME.format(path_hash=path_hash, module_name=org_mod_name)
    raise ValueError("file_path should be a string to generate unique module name")


def might_contain_dag_via_default_heuristic(file_path: str, zip_file: zipfile.ZipFile | None = None) -> bool:
    """
    Heuristic that guesses whether a Python file contains an Airflow DAG definition.

    :param file_path: Path to the file to be checked.
    :param zip_file: if passed, checks the archive. Otherwise, check local filesystem.
    :return: True, if file might contain DAGs.
    """
    if zip_file:
        with zip_file.open(file_path) as current_file:
            content = current_file.read()
    else:
        if zipfile.is_zipfile(file_path):
            return True
        with open(file_path, "rb") as dag_file:
            content = dag_file.read()
    content = content.lower()
    if b"airflow" not in content:
        return False
    return any(s in content for s in (b"dag", b"asset"))


def might_contain_dag(file_path: str, safe_mode: bool, zip_file: zipfile.ZipFile | None = None) -> bool:
    """
    Check whether a Python file contains Airflow DAGs.

    When safe_mode is off (with False value), this function always returns True.

    If might_contain_dag_callable isn't specified, it uses airflow default heuristic.
    """
    if not safe_mode:
        return True

    might_contain_dag_callable: Callable[[str, zipfile.ZipFile | None], bool] | None = None
    try:
        from airflow.configuration import conf

        might_contain_dag_callable = conf.getimport(
            "core",
            "might_contain_dag_callable",
            fallback=None,
        )
    except Exception:
        pass

    if might_contain_dag_callable is None:
        might_contain_dag_callable = might_contain_dag_via_default_heuristic

    return might_contain_dag_callable(file_path=file_path, zip_file=zip_file)
