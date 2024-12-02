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

import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

from airflow.configuration import conf


class BaseDagBundle(ABC):
    """
    Base class for DAG bundles.

    DAG bundles are used both by the DAG processor and by a worker when running a task. These usage
    patterns are different, however.

    When running a task, we know what version of the bundle we need (assuming the bundle supports versioning).
    And we likely only need to keep this specific bundle version around for as long as we have tasks running using
    that bundle version. This also means, that on a single worker, it's possible that multiple versions of the same
    bundle are used at the same time.

    In contrast, the DAG processor uses a bundle to keep the DAGs from that bundle up to date. There will not be
    multiple versions of the same bundle in use at the same time. The DAG processor will always use the latest version.

    :param name: String identifier for the DAG bundle
    :param version: Version of the DAG bundle (Optional)
    """

    supports_versioning: bool = False

    def __init__(self, *, name: str, version: str | None = None) -> None:
        self.name = name
        self.version = version

    @property
    def _dag_bundle_root_storage_path(self) -> Path:
        """
        Where bundles can store DAGs on disk (if local disk is required).

        This is the root path, shared by various bundles. Each bundle should have its own subdirectory.
        """
        if configured_location := conf.get("core", "dag_bundle_storage_path"):
            return Path(configured_location)
        return Path(tempfile.gettempdir(), "airflow", "dag_bundles")

    @property
    @abstractmethod
    def path(self) -> Path:
        """
        Path for this bundle.

        Airflow will use this path to load/execute the DAGs from the bundle.
        """

    @abstractmethod
    def get_current_version(self) -> str | None:
        """
        Retrieve a string that represents the version of the DAG bundle.

        Airflow can use this value to retrieve this same bundle version later.
        """

    @abstractmethod
    def refresh(self) -> None:
        """Retrieve the latest version of the files in the bundle."""
