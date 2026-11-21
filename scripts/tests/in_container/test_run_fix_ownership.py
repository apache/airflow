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
from types import SimpleNamespace
from unittest import mock

import run_fix_ownership as m


@mock.patch.object(m.os, "chown", autospec=True)
@mock.patch.object(m.pwd, "getpwnam", autospec=True)
def test_only_directories_owned_by_root_are_chowned(mock_getpwnam, mock_chown, tmp_path, monkeypatch):
    monkeypatch.setenv("HOST_USER_ID", "1000")
    monkeypatch.setenv("HOST_GROUP_ID", "1000")
    (tmp_path / "read_only_mount").mkdir()
    # Any uid but the owner of the directory, which then counts as not created by the container.
    mock_getpwnam.return_value = SimpleNamespace(pw_uid=os.getuid() + 1)

    m.change_ownership_of_files(tmp_path)

    mock_chown.assert_not_called()

    mock_getpwnam.return_value = SimpleNamespace(pw_uid=os.getuid())

    m.change_ownership_of_files(tmp_path)

    mock_chown.assert_called_once_with(tmp_path / "read_only_mount", 1000, 1000, follow_symlinks=False)
