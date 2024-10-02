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
    Base class for DAG bundle backends.

    DAG bundles can be used in 2 ways:
      - In an ephemeral manner, essentially to run a task. If the bundle supports versioning, the version is passed.
      - In an ongoing manner, by the DAG processor to keep the DAGs from the bundle up to date.
    """

    supports_versioning: bool = False

    def __init__(self, *, id: str, version: str | None = None) -> None:
        self.id = id
        self.version = version

    @property
    def dag_bundle_storage_path(self) -> Path:
        if temp_dir := conf.get("core", "dag_bundle_storage_path"):
            return Path(temp_dir)
        return Path(tempfile.gettempdir())

    @abstractmethod
    def path(self) -> Path:
        """Path where the DAGs from this backend is stored."""

    @abstractmethod
    def get_current_version(self) -> str:
        """Version that represents the version of the DAGs."""

    @abstractmethod
    def refresh(self) -> None:
        """Make sure the backend has the latest DAGs."""

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up the backend."""
