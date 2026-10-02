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
import os
import re
import zipfile
from collections.abc import Generator
from io import TextIOWrapper
from pathlib import Path
from typing import overload

from airflow._shared.module_loading import (
    get_unique_dag_module_name as get_unique_dag_module_name,
    might_contain_dag as might_contain_dag,
    might_contain_dag_via_default_heuristic as might_contain_dag_via_default_heuristic,
)

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


def find_enclosing_file(path: Path) -> Path | None:
    """
    Return ``path`` or its nearest ancestor that is a file, or ``None`` if there is none.

    A Dag definition nested in a container is referenced by a path that does not exist on
    disk (``archive.zip/dag.py``, for instance); this resolves it to the container.
    """
    return next((candidate for candidate in (path, *path.parents) if candidate.is_file()), None)


COMMENT_PATTERN = re.compile(r"\s*#.*")


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


def __getattr__(name: str):
    if name == "find_path_from_directory":
        import warnings

        from airflow._shared.module_loading import find_path_from_directory
        from airflow.utils.deprecation_tools import DeprecatedImportWarning

        warnings.warn(
            "Importing find_path_from_directory from airflow.utils.file is deprecated "
            "and will be removed in a future version. "
            "Use airflow._shared.module_loading.find_path_from_directory instead.",
            DeprecatedImportWarning,
            stacklevel=2,
        )
        return find_path_from_directory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
