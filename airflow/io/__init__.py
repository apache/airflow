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

import logging
from typing import (
    Callable,
)

from fsspec.implementations.local import LocalFileSystem

from airflow.providers_manager import ProvidersManager
from airflow.stats import Stats
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)


def _file(_: str | None) -> LocalFileSystem:
    return LocalFileSystem()


# always support local file systems
SCHEME_TO_FS: dict[str, Callable] = {
    "file": _file,
}


def _register_schemes() -> None:
    with Stats.timer("airflow.io.load_filesystems") as timer:
        manager = ProvidersManager()
        for fs_module_name in manager.filesystem_module_names:
            fs_module = import_string(fs_module_name)
            for scheme in getattr(fs_module, "schemes", []):
                if scheme in SCHEME_TO_FS:
                    log.warning("Overriding scheme %s for %s", scheme, fs_module_name)

                method = getattr(fs_module, "get_fs", None)
                if method is None:
                    raise ImportError(f"Filesystem {fs_module_name} does not have a get_fs method")
                SCHEME_TO_FS[scheme] = method

    log.debug("loading filesystems from providers took %.3f seconds", timer.duration)


_register_schemes()
