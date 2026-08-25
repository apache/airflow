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

import subprocess
from contextlib import nullcontext
from unittest import mock

import ci.prek.check_supervisor_schemas_versions as check_supervisor_schemas_versions


@mock.patch.object(check_supervisor_schemas_versions, "dump_snapshot", autospec=True)
@mock.patch.object(check_supervisor_schemas_versions, "create_temporary_worktree", autospec=True)
def test_dump_snapshot_from_main_uses_target_ref(create_temporary_worktree, dump_snapshot, tmp_path):
    create_temporary_worktree.return_value = nullcontext(tmp_path)
    dump_snapshot.return_value = "main snapshot"

    assert check_supervisor_schemas_versions.dump_snapshot_from_main("upstream/main") == "main snapshot"
    create_temporary_worktree.assert_called_once_with("upstream/main")
    dump_snapshot.assert_called_once_with(tmp_path)


@mock.patch.object(check_supervisor_schemas_versions.subprocess, "run", autospec=True)
def test_upstream_has_schema_checks_target_ref(run):
    run.return_value = subprocess.CompletedProcess(args=["git", "cat-file"], returncode=0)

    assert check_supervisor_schemas_versions._upstream_has_schema("upstream/main") is True
    run.assert_called_once_with(
        [
            "git",
            "cat-file",
            "-e",
            f"upstream/main:{check_supervisor_schemas_versions.VERSIONS_PREFIX}__init__.py",
        ],
        capture_output=True,
        check=False,
    )


@mock.patch.object(check_supervisor_schemas_versions, "dump_snapshot_from_main", autospec=True)
@mock.patch.object(
    check_supervisor_schemas_versions, "_upstream_has_schema", autospec=True, return_value=False
)
@mock.patch.object(
    check_supervisor_schemas_versions, "fetch_target_branch", autospec=True, return_value="upstream/main"
)
@mock.patch.object(check_supervisor_schemas_versions, "get_changed_files", autospec=True)
def test_main_fetches_target_once(
    get_changed_files,
    fetch_target_branch,
    upstream_has_schema,
    dump_snapshot_from_main,
):
    get_changed_files.return_value = [check_supervisor_schemas_versions.TASK_SDK_COMMS_PATH]

    assert check_supervisor_schemas_versions.main() == 0
    fetch_target_branch.assert_called_once_with()
    upstream_has_schema.assert_called_once_with("upstream/main")
    dump_snapshot_from_main.assert_not_called()
