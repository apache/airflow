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

from pathlib import Path

from airflow import settings
from airflow.dag_processing.bundles.base import BaseDagBundle


class LocalDagBundle(BaseDagBundle):
    """
    Local DAG bundle - exposes a local directory as a DAG bundle.

    :param path: Local path where the DAGs are stored
    :param pin_symlink_on_refresh: Useful when ``path`` is a symlink that an external
        synchronization process swaps atomically, e.g. the kubernetes `git-sync
        <https://github.com/kubernetes/git-sync>`_ sidecar. When enabled, the symlink is
        resolved on each refresh and the resolved target is pinned until the next
        refresh, so a swap happening mid-way cannot mix files from two different
        targets within a single parse round or task run. Note that each component
        (dag processor, worker, triggerer) resolves and pins independently when it
        initializes the bundle. (Optional, defaults to False)
    """

    supports_versioning = False

    def __init__(self, *, path: str | None = None, pin_symlink_on_refresh: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        if path is None:
            path = settings.DAGS_FOLDER

        self._path = Path(path)
        self._pin_symlink_on_refresh = pin_symlink_on_refresh
        self._pinned_path: Path | None = None

    def initialize(self) -> None:
        self.refresh()
        super().initialize()

    def get_current_version(self) -> None:
        return None

    def refresh(self) -> None:
        """Re-resolve the symlink when pinning is enabled - otherwise there is nothing to refresh."""
        if self._pin_symlink_on_refresh:
            self._pinned_path = self._path.resolve()

    @property
    def path(self) -> Path:
        if self._pinned_path is not None:
            return self._pinned_path
        return self._path
