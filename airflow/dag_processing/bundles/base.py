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

import fcntl
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
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
    :param refresh_interval: How often the bundle should be refreshed from the source in seconds
        (Optional - defaults to [dag_processor] refresh_interval)
    :param version: Version of the DAG bundle (Optional)
    """

    supports_versioning: bool = False
    _locked: bool = False

    def __init__(
        self,
        *,
        name: str,
        refresh_interval: int = conf.getint("dag_processor", "refresh_interval"),
        version: str | None = None,
    ) -> None:
        self.name = name
        self.version = version
        self.refresh_interval = refresh_interval
        self.is_initialized: bool = False

    def initialize(self) -> None:
        """
        Initialize the bundle.

        This method is called by the DAG processor and worker before the bundle is used,
        and allows for deferring expensive operations until that point in time. This will
        only be called when Airflow needs the bundle files on disk - some uses only need
        to call the `view_url` method, which can run without initializing the bundle.

        This method must ultimately be safe to call concurrently from different threads or processes.
        If it isn't naturally safe, you'll need to make it so with some form of locking.
        There is a `lock` context manager on this class available for this purpose.
        """
        self.is_initialized = True

    @property
    def _dag_bundle_root_storage_path(self) -> Path:
        """
        Where bundles can store DAGs on disk (if local disk is required).

        This is the root bundle storage path, common to all bundles. Each bundle should use a subdirectory of this path.
        """
        if configured_location := conf.get("dag_processor", "dag_bundle_storage_path", fallback=None):
            return Path(configured_location)
        return Path(tempfile.gettempdir(), "airflow", "dag_bundles")

    @property
    @abstractmethod
    def path(self) -> Path:
        """
        Path for this bundle.

        Airflow will use this path to find/load/execute the DAGs from the bundle.
        After `initialize` has been called, all dag files in the bundle should be accessible from this path.
        """

    @abstractmethod
    def get_current_version(self) -> str | None:
        """
        Retrieve a string that represents the version of the DAG bundle.

        Airflow can use this value to retrieve this same bundle version later.
        """

    @abstractmethod
    def refresh(self) -> None:
        """
        Retrieve the latest version of the files in the bundle.

        This method must ultimately be safe to call concurrently from different threads or processes.
        If it isn't naturally safe, you'll need to make it so with some form of locking.
        There is a `lock` context manager on this class available for this purpose.
        """

    def view_url(self, version: str | None = None) -> str | None:
        """
        URL to view the bundle on an external website. This is shown to users in the Airflow UI, allowing them to navigate to this url for more details about that version of the bundle.

        This needs to function without `initialize` being called.

        :param version: Version to view
        :return: URL to view the bundle
        """

    @contextmanager
    def lock(self):
        """
        Ensure only a single bundle can enter this context at a time, by taking an exclusive lock on a lockfile.

        This is useful when a bundle needs to perform operations that are not safe to run concurrently.
        """
        if self._locked:
            yield
            return

        lock_dir_path = self._dag_bundle_root_storage_path / "_locks"
        lock_dir_path.mkdir(parents=True, exist_ok=True)
        lock_file_path = lock_dir_path / f"{self.name}.lock"
        with open(lock_file_path, "w") as lock_file:
            # Exclusive lock - blocks until it is available
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                self._locked = True
                yield
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                self._locked = False
