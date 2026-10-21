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
from unittest import mock

import pytest

from airflow_breeze.utils import path_utils
from airflow_breeze.utils.path_utils import cleanup_python_generated_files


@pytest.fixture
def root_with_pycache(tmp_path, monkeypatch):
    pycache = tmp_path / "pkg" / "__pycache__"
    pycache.mkdir(parents=True)
    (pycache / "mod.cpython-310.pyc").write_bytes(b"")
    monkeypatch.setattr(path_utils, "AIRFLOW_ROOT_PATH", tmp_path)
    return pycache


@mock.patch.object(path_utils.shutil, "rmtree", autospec=True)
def test_cleanup_ignores_pycache_repopulated_concurrently(mock_rmtree, root_with_pycache):
    mock_rmtree.side_effect = OSError(errno.ENOTEMPTY, "Directory not empty")

    cleanup_python_generated_files()

    mock_rmtree.assert_called_once_with(str(root_with_pycache))


@mock.patch.object(path_utils.shutil, "rmtree", autospec=True)
def test_cleanup_reraises_other_os_errors(mock_rmtree, root_with_pycache):
    mock_rmtree.side_effect = OSError(errno.EIO, "I/O error")

    with pytest.raises(OSError, match="I/O error"):
        cleanup_python_generated_files()
