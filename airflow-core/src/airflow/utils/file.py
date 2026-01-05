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
from __future__ import annotations

import ast
import hashlib
import logging
import os
import re
import zipfile
from collections.abc import Generator
from io import TextIOWrapper
from pathlib import Path
from typing import overload

from airflow._shared.module_loading import find_path_from_directory
from airflow.configuration import conf

log = logging.getLogger(__name__)

MODIFIED_DAG_MODULE_NAME = "unusual_prefix_{path_hash}_{module_name}"


ZIP_REGEX = re.compile(rf"((.*\.zip){re.escape(os.sep)})?(.*)")


@overload
def correct_maybe_zipped(fileloc: None) -> None: ...


@overload
def correct_maybe_zipped(fileloc: str | Path) -> str | Path: ...


def correct_maybe_zipped(fileloc: None | str | Path) -> None | str | Path:
    """If the path contains a folder with a .zip suffix, treat it as a zip archive and return path."""
    if not fileloc:
        return fileloc
    search_ = ZIP_REGEX.search(str(fileloc))
    if not search_:
        return fileloc
    _, archive, _ = search_.groups()
    if archive and zipfile.is_zipfile(archive):
        return archive
    return fileloc


def open_maybe_zipped(fileloc, mode="r"):
    """
    Open the given file.

    If the path contains a folder with a .zip suffix, then the folder
    is treated as a zip archive, opening the file inside the archive.

    :return: a file object, as in `open`, or as in `ZipFile.open`.
    """
    _, archive, filename = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return TextIOWrapper(zipfile.ZipFile(archive, mode=mode).open(filename))
    return open(fileloc, mode=mode)


def list_py_file_paths(
    directory: str | os.PathLike[str] | None,
    safe_mode: bool = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE", fallback=True),
) -> list[str]:
    """
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :param safe_mode: whether to use a heuristic to determine whether a file
        contains Airflow DAG definitions. If not provided, use the
        core.DAG_DISCOVERY_SAFE_MODE configuration setting. If not set, default
        to safe.
    :return: a list of paths to Python files in the specified directory
    """
    file_paths: list[str] = []
    if directory is None:
        file_paths = []
    elif os.path.isfile(directory):
        file_paths = [str(directory)]
    elif os.path.isdir(directory):
        file_paths.extend(find_dag_file_paths(directory, safe_mode))
    return file_paths


def find_dag_file_paths(directory: str | os.PathLike[str], safe_mode: bool) -> list[str]:
    """Find file paths of all DAG files."""
    file_paths = []
    ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")

    for file_path in find_path_from_directory(directory, ".airflowignore", ignore_file_syntax):
        path = Path(file_path)
        try:
            if path.is_file() and (path.suffix == ".py" or zipfile.is_zipfile(path)):
                if might_contain_dag(file_path, safe_mode):
                    file_paths.append(file_path)
        except Exception:
            log.exception("Error while examining %s", file_path)

    return file_paths


COMMENT_PATTERN = re.compile(r"\s*#.*")


def might_contain_dag(file_path: str, safe_mode: bool, zip_file: zipfile.ZipFile | None = None) -> bool:
    """
    Check whether a Python file contains Airflow DAGs.

    When safe_mode is off (with False value), this function always returns True.

    If might_contain_dag_callable isn't specified, it uses airflow default heuristic
    """
    if not safe_mode:
        return True

    might_contain_dag_callable = conf.getimport(
        "core",
        "might_contain_dag_callable",
        fallback="airflow.utils.file.might_contain_dag_via_default_heuristic",
    )
    return might_contain_dag_callable(file_path=file_path, zip_file=zip_file)


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


def _find_imported_modules(module: ast.Module) -> Generator[str, None, None]:
    for st in module.body:
        if isinstance(st, ast.Import):
            for n in st.names:
                yield n.name
        elif isinstance(st, ast.ImportFrom) and st.module is not None:
            yield st.module


def iter_airflow_imports(file_path: str) -> Generator[str, None, None]:
    """Find Airflow modules imported in the given file."""
    try:
        parsed = ast.parse(Path(file_path).read_bytes())
    except Exception:
        return
    for m in _find_imported_modules(parsed):
        if m.startswith("airflow."):
            yield m


def get_unique_dag_module_name(file_path: str) -> str:
    """Return a unique module name in the format unusual_prefix_{sha1 of module's file path}_{original module name}."""
    if isinstance(file_path, str):
        path_hash = hashlib.sha1(file_path.encode("utf-8"), usedforsecurity=False).hexdigest()
        org_mod_name = re.sub(r"[.-]", "_", Path(file_path).stem)
        return MODIFIED_DAG_MODULE_NAME.format(path_hash=path_hash, module_name=org_mod_name)
    raise ValueError("file_path should be a string to generate unique module name")
