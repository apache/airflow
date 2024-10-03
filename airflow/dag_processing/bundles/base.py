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

    DAG bundles can be used in 2 ways:
      - In an ephemeral manner, essentially to run a task. This may mean a specific version of the DAG bundle is retrieved.
      - In an ongoing manner, by the DAG processor, to keep the DAGs in the bundle up to date.

    :param id: String identifier for the DAG bundle
    :param version: Version of the DAG bundle (Optional)
    """

    supports_versioning: bool = False

    def __init__(self, *, id: str, version: str | None = None) -> None:
        self.id = id
        self.version = version

    @property
    def _dag_bundle_storage_path(self) -> Path:
        """Where Airflow can store DAG bundles on disk (if local disk is required)."""
        if configured_location := conf.get("core", "dag_bundle_storage_path"):
            return Path(configured_location)
        return Path(tempfile.gettempdir(), "airflow", "dag_bundles")

    @property
    @abstractmethod
    def path(self) -> Path:
        """Path where the DAGs from this backend live."""

    @abstractmethod
    def get_current_version(self) -> str:
        """Version that represents the version of the DAGs."""

    @abstractmethod
    def refresh(self) -> None:
        """Make sure the backend has the latest DAGs."""

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up the backend."""
