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
import os
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

UNUSUAL_MODULE_PREFIX = "unusual_prefix_"
MODIFIED_DAG_MODULE_NAME = f"{UNUSUAL_MODULE_PREFIX}{{path_hash}}_{{module_name}}"

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from typing import Protocol

    class _MightContainDagCallable(Protocol):
        def __call__(
            self,
            file_path: str | _DagDefinitionLike,
            zip_file: zipfile.ZipFile | None = None,
        ) -> bool: ...

    class _ConfLike(Protocol):
        def getimport(self, section: str, key: str, **kwargs: Any) -> Any: ...

    class _DagDefinitionLike(Protocol):
        """Structural view of a DagDefinition: read its bytes, or materialize it as a file."""

        def read_bytes(self) -> bytes: ...

        def as_file(self) -> AbstractContextManager[Path]: ...


def get_unique_dag_module_name(file_path: str) -> str:
    """Return a unique module name in the format unusual_prefix_{sha1 of module's file path}_{original module name}."""
    if isinstance(file_path, str):
        path_hash = hashlib.sha1(file_path.encode("utf-8"), usedforsecurity=False).hexdigest()
        org_mod_name = re.sub(r"[.-]", "_", Path(file_path).stem)
        return MODIFIED_DAG_MODULE_NAME.format(path_hash=path_hash, module_name=org_mod_name)
    raise ValueError("file_path should be a string to generate unique module name")


def accepts_dag_definition(func: _MightContainDagCallable) -> _MightContainDagCallable:
    """
    Mark a ``might_contain_dag_callable`` as accepting a Dag definition, not only a path.

    A marked callable is handed the definition itself, so an archive member or any other
    non-filesystem source is checked without being written to a temporary file first.
    """
    func.accepts_dag_definition = True  # type: ignore[attr-defined]
    return func


@accepts_dag_definition
def might_contain_dag_via_default_heuristic(
    file_path: str | _DagDefinitionLike,
    zip_file: zipfile.ZipFile | None = None,
) -> bool:
    """
    Heuristic that guesses whether a Python file contains an Airflow DAG definition.

    :param file_path: path to the file to check, or a DagDefinition-like object whose bytes
        are read directly (nothing is read from disk).
    :param zip_file: if passed, checks the named member inside the archive. Otherwise, check
        the local filesystem.
    :return: True, if file might contain DAGs.
    """
    if not isinstance(file_path, (str, os.PathLike)):
        data = file_path.read_bytes()
    elif zip_file:
        with zip_file.open(file_path) as current_file:
            data = current_file.read()
    elif zipfile.is_zipfile(file_path):
        return True
    else:
        with open(file_path, "rb") as dag_file:
            data = dag_file.read()
    data = data.lower()
    if b"airflow" not in data:
        return False
    return any(s in data for s in (b"dag", b"asset"))


def might_contain_dag(
    file_path: str | _DagDefinitionLike,
    safe_mode: bool = True,
    zip_file: zipfile.ZipFile | None = None,
    *,
    conf: _ConfLike,
) -> bool:
    """
    Check whether a source might contain Airflow DAGs.

    ``file_path`` may be a filesystem path (optionally with a ``zip_file`` archive whose
    member it names) or a DagDefinition-like object exposing ``read_bytes()`` and
    ``as_file()``. Passing a definition lets the check run against an in-memory or
    archive-backed source without materializing a file. When safe_mode is off (with False
    value), this function always returns True.

    A callable marked with :func:`accepts_dag_definition`, including the default heuristic,
    is handed the definition and reads its bytes directly. Any other callable only
    understands the legacy ``(file_path, zip_file)`` signature, so a definition is
    materialized through its own ``as_file()`` and passed by path for compatibility.
    """
    if not safe_mode:
        return True

    might_contain_dag_callable: _MightContainDagCallable | None = None
    try:
        might_contain_dag_callable = conf.getimport(
            "core",
            "might_contain_dag_callable",
            fallback=None,
        )
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(
            "Failed to load might_contain_dag_callable from config, falling back to default heuristic: %s",
            e,
        )

    if might_contain_dag_callable is None:
        return might_contain_dag_via_default_heuristic(file_path, zip_file=zip_file)

    if isinstance(file_path, (str, os.PathLike)) or getattr(
        might_contain_dag_callable, "accepts_dag_definition", False
    ):
        return might_contain_dag_callable(file_path=file_path, zip_file=zip_file)
    # Legacy callables only accept (file_path, zip_file); let the definition materialize itself.
    with file_path.as_file() as materialized:
        return might_contain_dag_callable(file_path=str(materialized), zip_file=None)
