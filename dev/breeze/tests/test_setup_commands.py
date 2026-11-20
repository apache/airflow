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

from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.setup_commands import change_config


@pytest.mark.parametrize("enable", [False, True])
def test_setup_config_toggles_podman_for_all_worktrees(tmp_path, enable):
    shared_build = tmp_path / "main" / ".build"
    marker = shared_build / "enable_podman"
    if not enable:
        shared_build.mkdir(parents=True)
        marker.touch()
    with (
        mock.patch(
            "airflow_breeze.commands.setup_commands.get_shared_build_cache_path",
            autospec=True,
            return_value=shared_build,
        ),
        mock.patch(
            "airflow_breeze.utils.path_utils.get_shared_build_cache_path",
            autospec=True,
            return_value=shared_build,
        ),
    ):
        result = CliRunner().invoke(change_config, ["--podman" if enable else "--no-podman"])
    assert result.exit_code == 0, result.output
    assert marker.exists() is enable
    assert f"Podman: {'enabled' if enable else 'disabled'}" in result.output
