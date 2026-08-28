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

from contextlib import nullcontext
from unittest import mock

import ci.prek.check_execution_api_versions as check_execution_api_versions


@mock.patch.object(check_execution_api_versions, "generate_schema", autospec=True)
@mock.patch.object(check_execution_api_versions, "create_temporary_worktree", autospec=True)
def test_generate_schema_from_main_uses_target_ref(create_temporary_worktree, generate_schema, tmp_path):
    create_temporary_worktree.return_value = nullcontext(tmp_path)
    generate_schema.return_value = {"schema": "main"}

    assert check_execution_api_versions.generate_schema_from_main("upstream/main") == {"schema": "main"}
    create_temporary_worktree.assert_called_once_with("upstream/main")
    generate_schema.assert_called_once_with(tmp_path)


@mock.patch.object(check_execution_api_versions, "schemas_equal", autospec=True, return_value=True)
@mock.patch.object(check_execution_api_versions, "generate_schema", autospec=True)
@mock.patch.object(check_execution_api_versions, "generate_schema_from_main", autospec=True)
@mock.patch.object(
    check_execution_api_versions, "fetch_target_branch", autospec=True, return_value="upstream/main"
)
@mock.patch.object(check_execution_api_versions, "get_changed_files", autospec=True)
def test_main_fetches_target_once(
    get_changed_files,
    fetch_target_branch,
    generate_schema_from_main,
    generate_schema,
    schemas_equal,
):
    get_changed_files.return_value = [f"{check_execution_api_versions.DATAMODELS_PREFIX}taskinstance.py"]
    generate_schema_from_main.return_value = {"schema": "main"}
    generate_schema.return_value = {"schema": "current"}

    assert check_execution_api_versions.main() == 0
    fetch_target_branch.assert_called_once_with()
    generate_schema_from_main.assert_called_once_with("upstream/main")
    schemas_equal.assert_called_once_with({"schema": "current"}, {"schema": "main"})
