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

import inspect
import logging
from collections.abc import Callable, Mapping
from functools import cache
from typing import TYPE_CHECKING

from fsspec.implementations.local import LocalFileSystem

from airflow.providers_manager import ProvidersManager
from airflow.sdk.module_loading import import_string
from airflow.stats import Stats

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from airflow.sdk.io.typedef import Properties


log = logging.getLogger(__name__)


def _file(_: str | None, storage_options: Properties) -> LocalFileSystem:
    return LocalFileSystem(**storage_options)


# builtin supported filesystems
_BUILTIN_SCHEME_TO_FS: dict[str, Callable[[str | None, Properties], AbstractFileSystem]] = {
    "file": _file,
    "local": _file,
}


@cache
def _register_filesystems() -> Mapping[
    str,
    Callable[[str | None, Properties], AbstractFileSystem] | Callable[[str | None], AbstractFileSystem],
]:
    scheme_to_fs = _BUILTIN_SCHEME_TO_FS.copy()
    with Stats.timer("airflow.io.load_filesystems") as timer:
        manager = ProvidersManager()
        for fs_module_name in manager.filesystem_module_names:
            fs_module = import_string(fs_module_name)
            for scheme in getattr(fs_module, "schemes", []):
                if scheme in scheme_to_fs:
                    log.warning("Overriding scheme %s for %s", scheme, fs_module_name)

                method = getattr(fs_module, "get_fs", None)
                if method is None:
                    raise ImportError(f"Filesystem {fs_module_name} does not have a get_fs method")
                scheme_to_fs[scheme] = method

    log.debug("loading filesystems from providers took %.3f seconds", timer.duration)
    return scheme_to_fs


def get_fs(
    scheme: str, conn_id: str | None = None, storage_options: Properties | None = None
) -> AbstractFileSystem:
    """
    Get a filesystem by scheme.

    :param scheme: the scheme to get the filesystem for
    :return: the filesystem method
    :param conn_id: the airflow connection id to use
    :param storage_options: the storage options to pass to the filesystem
    """
    filesystems = _register_filesystems()
    try:
        fs = filesystems[scheme]
    except KeyError:
        raise ValueError(f"No filesystem registered for scheme {scheme}") from None

    options = storage_options or {}

    # MyPy does not recognize dynamic parameters inspection when we call the method, and we have to do
    # it for compatibility reasons with already released providers, that's why we need to ignore
    # mypy errors here
    parameters = inspect.signature(fs).parameters
    if len(parameters) == 1:
        if options:
            raise AttributeError(
                f"Filesystem {scheme} does not support storage options, but options were passed."
                f"This most likely means that you are using an old version of the provider that does not "
                f"support storage options. Please upgrade the provider if possible."
            )
        return fs(conn_id)  # type: ignore[call-arg]
    return fs(conn_id, options)  # type: ignore[call-arg]


def has_fs(scheme: str) -> bool:
    """
    Check if a filesystem is available for a scheme.

    :param scheme: the scheme to check
    :return: True if a filesystem is available for the scheme
    """
    return scheme in _register_filesystems()
