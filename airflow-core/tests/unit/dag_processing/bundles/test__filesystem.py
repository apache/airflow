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

import errno
import os
from pathlib import Path
from unittest import mock

import pytest

from airflow.dag_processing.bundles import _filesystem as filesystem_module
from airflow.dag_processing.bundles._filesystem import (
    fsync_directory,
    fsync_tree,
    fsync_tree_directories,
    read_json_file,
    remove_bundle_tree_forcefully,
)


def test_read_json_file_decodes_utf8_regardless_of_locale(tmp_path):
    path = tmp_path / "payload.json"
    path.write_bytes('{"value": "valid UTF-8: ☃"}'.encode())

    with mock.patch.object(Path, "read_text", autospec=True, side_effect=Path.read_text) as mock_read:
        payload = read_json_file(path, missing_message="missing", invalid_message="invalid")

    assert payload == {"value": "valid UTF-8: ☃"}
    mock_read.assert_called_once_with(path, encoding="utf-8")


@mock.patch.object(
    filesystem_module.os, "open", autospec=True, side_effect=PermissionError("cannot open directory")
)
def test_fsync_directory_propagates_open_failure(mock_open, tmp_path):
    with pytest.raises(PermissionError, match="cannot open directory"):
        fsync_directory(tmp_path)


_real_scandir = os.scandir


def _scandir_unless_unlistable(path="."):
    """List directories, failing for any named ``unlistable`` as an I/O fault would."""
    if os.path.basename(os.fspath(path)) == "unlistable":
        raise PermissionError(errno.EACCES, "cannot list directory", os.fspath(path))
    return _real_scandir(path)


@mock.patch.object(filesystem_module.os, "scandir", autospec=True, side_effect=_scandir_unless_unlistable)
def test_fsync_tree_directories_fails_on_an_unlistable_directory(mock_scandir, tmp_path):
    (tmp_path / "tree" / "unlistable").mkdir(parents=True)

    with pytest.raises(PermissionError, match="cannot list directory"):
        fsync_tree_directories(tmp_path / "tree")


class TestRemoveBundleTreeForcefully:
    def test_removes_read_only_tree(self, tmp_path):
        tree = tmp_path / "tree"
        (tree / "dags").mkdir(parents=True)
        (tree / "dags" / "example.py").write_text("print('dag')")
        (tree / "dags" / "example.py").chmod(0o444)
        (tree / "dags").chmod(0o555)
        tree.chmod(0o555)

        remove_bundle_tree_forcefully(tree)

        assert not tree.exists()

    def test_unlinks_a_symlink_without_following_it(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        link.symlink_to(target, target_is_directory=True)

        remove_bundle_tree_forcefully(link)

        assert not link.exists()
        assert target.is_dir()


class TestFsyncTree:
    @mock.patch.object(
        filesystem_module, "fsync_file", autospec=True, side_effect=filesystem_module.fsync_file
    )
    def test_ignored_file_can_disappear_while_authoritative_files_are_synced(self, mock_fsync, tmp_path):
        tree = tmp_path / "tree"
        (tree / "dags").mkdir(parents=True)
        (tree / "dags" / "example.py").write_text("print('dag')\n")
        manifest = tree / "manifest.json"
        manifest.write_text("{}")
        temporary_bytecode = tree / "dags" / "__pycache__" / "example.cpython-312.pyc.123"
        temporary_bytecode.parent.mkdir()
        temporary_bytecode.write_bytes(b"partial bytecode")

        def ignore_file(path: Path) -> bool:
            if path != temporary_bytecode:
                return False
            temporary_bytecode.unlink()
            return True

        fsync_tree(tree, ignore_file=ignore_file)

        assert not temporary_bytecode.exists()
        assert mock_fsync.call_count == 2
        mock_fsync.assert_has_calls(
            [mock.call(manifest), mock.call(tree / "dags" / "example.py")], any_order=True
        )
