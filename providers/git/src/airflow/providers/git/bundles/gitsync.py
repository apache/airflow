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

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException

log = structlog.get_logger(__name__)


class GitSyncDagBundle(BaseDagBundle):
    """
    Exposes a git checkout maintained by an external synchronization process as a DAG bundle.

    Use this bundle when something else -- typically the Kubernetes `git-sync
    <https://github.com/kubernetes/git-sync>`_ sidecar -- is responsible for pulling the
    repository, and Airflow should only read the files. Unlike
    :class:`~airflow.providers.git.bundles.git.GitDagBundle`, no ``git fetch`` (nor any
    Airflow Connection resolution) ever happens in the dag processor: for large,
    frequently parsed repositories this keeps git traffic and credentials out of the
    parsing path entirely.

    git-sync publishes each synced commit as a separate worktree directory and atomically
    swaps a symlink to point at the current one. This bundle resolves that symlink on each
    :meth:`refresh` and pins the resolved worktree until the next refresh, so a sync that
    happens mid-parse cannot mix files from two different commits into one parsing round.

    .. note::
        Because the previous worktree must remain readable while a parse using the pinned
        worktree is still in flight, run git-sync with ``--stale-worktree-timeout`` set to
        a value comfortably larger than your longest parse loop, so that stale worktrees
        are not deleted immediately after a swap.

    This bundle does not support versioning: the synced checkout only ever contains the
    current state, so Airflow cannot materialize an arbitrary older version from it. Any
    component reading the bundle (dag processor, workers, triggerer) must have the synced
    path available locally, e.g. by running the git-sync sidecar in each pod or mounting a
    shared volume.

    :param sync_path: Path to the symlink (or directory) maintained by the external
        process. For git-sync, this is ``<--root>/<--link>``.
    :param subdir: Subdirectory within the repository where the DAGs are stored (Optional)
    """

    supports_versioning = False

    def __init__(self, *, sync_path: str, subdir: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        if self.version:
            raise AirflowException(
                f"{type(self).__name__} does not support versioning; it only exposes the "
                "current state of the externally synchronized checkout."
            )
        self.sync_path = Path(sync_path)
        self.subdir = subdir
        self._pinned_path: Path | None = None
        self._log = log.bind(bundle_name=self.name, sync_path=str(self.sync_path))

    def initialize(self) -> None:
        self.refresh()
        super().initialize()

    def refresh(self) -> None:
        if not self.sync_path.exists():
            raise AirflowException(
                f"git-sync path {self.sync_path} does not exist. "
                "Is the synchronization process (e.g. the git-sync sidecar) running?"
            )
        # Resolve the symlink git-sync swaps atomically, and pin the resolved worktree so
        # that all files read until the next refresh come from a single commit.
        self._pinned_path = self.sync_path.resolve()
        self._log.debug("Pinned externally synchronized worktree", pinned_path=str(self._pinned_path))

    def get_current_version(self) -> None:
        return None

    @property
    def path(self) -> Path:
        base = self._pinned_path if self._pinned_path is not None else self.sync_path
        if self.subdir:
            return base / self.subdir
        return base

    def __repr__(self):
        return (
            f"<GitSyncDagBundle("
            f"name={self.name!r}, "
            f"sync_path={str(self.sync_path)!r}, "
            f"subdir={self.subdir!r}"
            f")>"
        )
