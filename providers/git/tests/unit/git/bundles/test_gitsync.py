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

import os
from pathlib import Path

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.git.bundles.gitsync import GitSyncDagBundle

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="symlink creation requires elevated privileges on Windows"
)


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


def _make_git_sync_layout(root: Path, sha: str, files: dict[str, str]) -> Path:
    """Create a fake git-sync worktree directory and return its path."""
    worktree = root / ".worktrees" / sha
    worktree.mkdir(parents=True)
    for name, content in files.items():
        file_path = worktree / name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)
    return worktree


def _swap_link(link: Path, target: Path) -> None:
    """Atomically repoint the git-sync style symlink, as git-sync does."""
    tmp_link = link.parent / (link.name + ".tmp")
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    tmp_link.symlink_to(target, target_is_directory=True)
    tmp_link.replace(link)


class TestGitSyncDagBundle:
    def test_supports_versioning(self):
        assert GitSyncDagBundle.supports_versioning is False

    def test_version_kwarg_rejected(self, tmp_path):
        with pytest.raises(AirflowException, match="does not support versioning"):
            GitSyncDagBundle(name="test", sync_path=str(tmp_path), version="abc123")

    def test_get_current_version_is_none(self, tmp_path):
        bundle = GitSyncDagBundle(name="test", sync_path=str(tmp_path))
        assert bundle.get_current_version() is None

    def test_initialize_pins_symlink_target(self, tmp_path):
        sync_root = tmp_path / "git"
        sync_root.mkdir()
        worktree = _make_git_sync_layout(sync_root, "a" * 40, {"dag.py": "# dag"})
        link = sync_root / "repo"
        link.symlink_to(worktree, target_is_directory=True)

        bundle = GitSyncDagBundle(name="test", sync_path=str(link))
        bundle.initialize()

        assert bundle.path == worktree.resolve()
        assert (bundle.path / "dag.py").read_text() == "# dag"

    def test_mid_parse_swap_does_not_change_pinned_path(self, tmp_path):
        """A symlink swap between refreshes must not affect the pinned worktree."""
        sync_root = tmp_path / "git"
        sync_root.mkdir()
        worktree_old = _make_git_sync_layout(sync_root, "a" * 40, {"dag.py": "# old"})
        worktree_new = _make_git_sync_layout(sync_root, "b" * 40, {"dag.py": "# new"})
        link = sync_root / "repo"
        link.symlink_to(worktree_old, target_is_directory=True)

        bundle = GitSyncDagBundle(name="test", sync_path=str(link))
        bundle.initialize()
        assert (bundle.path / "dag.py").read_text() == "# old"

        # git-sync swaps the symlink while a parse round is still using the bundle.
        _swap_link(link, worktree_new)

        # The pinned path still serves the commit that was current at refresh time...
        assert (bundle.path / "dag.py").read_text() == "# old"

        # ...and the next refresh repoints to the new worktree.
        bundle.refresh()
        assert bundle.path == worktree_new.resolve()
        assert (bundle.path / "dag.py").read_text() == "# new"

    def test_subdir(self, tmp_path):
        sync_root = tmp_path / "git"
        sync_root.mkdir()
        worktree = _make_git_sync_layout(sync_root, "a" * 40, {"dags/dag.py": "# dag"})
        link = sync_root / "repo"
        link.symlink_to(worktree, target_is_directory=True)

        bundle = GitSyncDagBundle(name="test", sync_path=str(link), subdir="dags")
        bundle.initialize()

        assert bundle.path == worktree.resolve() / "dags"
        assert (bundle.path / "dag.py").read_text() == "# dag"

    def test_plain_directory_is_supported(self, tmp_path):
        """A plain directory (no symlink indirection) also works."""
        checkout = tmp_path / "checkout"
        checkout.mkdir()
        (checkout / "dag.py").write_text("# dag")

        bundle = GitSyncDagBundle(name="test", sync_path=str(checkout))
        bundle.initialize()

        assert bundle.path == checkout.resolve()

    def test_missing_sync_path_raises(self, tmp_path):
        bundle = GitSyncDagBundle(name="test", sync_path=str(tmp_path / "missing"))
        with pytest.raises(AirflowException, match="does not exist"):
            bundle.initialize()

    def test_path_before_initialize_falls_back_to_sync_path(self, tmp_path):
        bundle = GitSyncDagBundle(name="test", sync_path=str(tmp_path))
        assert bundle.path == tmp_path
