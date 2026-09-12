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

from airflow.dag_processing.bundles.local import LocalDagBundle

from tests_common.test_utils.config import conf_vars


class TestLocalDagBundle:
    def test_path(self):
        bundle = LocalDagBundle(name="test", path="/hello")
        assert bundle.path == Path("/hello")

    @conf_vars({("core", "dags_folder"): "/tmp/somewhere/dags"})
    def test_path_default(self):
        bundle = LocalDagBundle(name="test", refresh_interval=300)
        assert bundle.path == Path("/tmp/somewhere/dags")

    def test_none_for_version(self):
        assert LocalDagBundle.supports_versioning is False

        bundle = LocalDagBundle(name="test", path="/hello")

        assert bundle.get_current_version() is None


@pytest.mark.skipif(os.name == "nt", reason="symlink creation requires elevated privileges on Windows")
class TestLocalDagBundleSymlinkPinning:
    @staticmethod
    def _swap_link(link: Path, target: Path) -> None:
        """Atomically repoint the symlink, as e.g. git-sync does."""
        tmp_link = link.parent / (link.name + ".tmp")
        tmp_link.symlink_to(target, target_is_directory=True)
        tmp_link.replace(link)

    @staticmethod
    def _make_target(root: Path, name: str, content: str) -> Path:
        target = root / name
        target.mkdir()
        (target / "dag.py").write_text(content)
        return target

    def test_not_pinned_by_default(self, tmp_path):
        target_old = self._make_target(tmp_path, "old", "# old")
        target_new = self._make_target(tmp_path, "new", "# new")
        link = tmp_path / "current"
        link.symlink_to(target_old, target_is_directory=True)

        bundle = LocalDagBundle(name="test", path=str(link))
        bundle.initialize()

        assert bundle.path == link
        self._swap_link(link, target_new)
        # Without pinning, reads through the path follow the swapped symlink.
        assert (bundle.path / "dag.py").read_text() == "# new"

    def test_pins_resolved_target_until_next_refresh(self, tmp_path):
        target_old = self._make_target(tmp_path, "old", "# old")
        target_new = self._make_target(tmp_path, "new", "# new")
        link = tmp_path / "current"
        link.symlink_to(target_old, target_is_directory=True)

        bundle = LocalDagBundle(name="test", path=str(link), pin_symlink_on_refresh=True)
        bundle.initialize()
        assert bundle.path == target_old.resolve()

        # The symlink is swapped while a parse round / task run is still using the bundle.
        self._swap_link(link, target_new)

        # The pinned path still serves the target that was current at refresh time...
        assert (bundle.path / "dag.py").read_text() == "# old"

        # ...and the next refresh repoints to the new target.
        bundle.refresh()
        assert bundle.path == target_new.resolve()
        assert (bundle.path / "dag.py").read_text() == "# new"

    def test_pinning_with_plain_directory(self, tmp_path):
        """A plain directory (no symlink indirection) simply resolves to itself."""
        target = self._make_target(tmp_path, "plain", "# dag")

        bundle = LocalDagBundle(name="test", path=str(target), pin_symlink_on_refresh=True)
        bundle.initialize()

        assert bundle.path == target.resolve()
        assert (bundle.path / "dag.py").read_text() == "# dag"
