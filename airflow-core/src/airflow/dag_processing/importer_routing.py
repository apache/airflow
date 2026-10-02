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
"""Route Dag files to the process that parses them, by the bundle's Dag importer registry."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter, find_claiming_coordinator
from airflow.sdk.importers import get_importer_registry

if TYPE_CHECKING:
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator  # noqa: SDK001
    from airflow.sdk.importers import AbstractDagImporter  # noqa: SDK001

log = logging.getLogger(__name__)


def warm_importer_registry(bundle_name: str) -> None:
    """Build and cache the bundle's Dag importer registry, or log why it cannot be built."""
    try:
        get_importer_registry(bundle_name)
    except Exception:
        log.exception("Cannot build the Dag importer registry for bundle %s", bundle_name)


def get_claiming_coordinator(
    path: str | os.PathLike[str], bundle_name: str | None
) -> SubprocessCoordinator | None:
    """
    Return the coordinator whose runtime parses ``path``, or ``None`` when a Python child parses it.

    A runtime parses the file when its importer is a coordinator's Dag importer. When the bundle's
    importers cannot be loaded, the error is logged and a Python child parses the file.
    """
    try:
        return find_claiming_coordinator(path, bundle_name)
    except Exception:
        log.exception("Cannot load the Dag importer for %s in bundle %s", path, bundle_name)
        return None


def is_coordinator_importer(importer: AbstractDagImporter) -> bool:
    """Return whether *importer* is a coordinator's Dag importer, whose files a Lang-SDK runtime parses."""
    return isinstance(importer, CoordinatorDagImporter)
