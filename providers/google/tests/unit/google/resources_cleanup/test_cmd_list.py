#
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
from unittest.mock import MagicMock, patch

import pytest
from airflow_google_provider_resource_cleanup.commands import cmd_list
from airflow_google_provider_resource_cleanup.commands.cmd_list import (
    GCPProjectConfig,
    _print_resources,
    _sync_resources,
    handle_list,
)


@pytest.mark.parametrize(
    ("asset_type", "expected_asset_arg"),
    [
        (None, ""),
        ("compute", ' --asset-types="compute.googleapis.com.*"'),
    ],
)
def test_sync_resources(asset_type, expected_asset_arg):
    with patch.object(cmd_list, "run_command") as mock_run:
        resource_file = Path("/tmp/res.json")
        _sync_resources("test-project", resource_file, asset_type)

        expected_cmd = (
            "gcloud asset search-all-resources --scope projects/test-project"
            f"{expected_asset_arg} --format json >"
            f' "{str(resource_file)}"'
        )
        mock_run.assert_called_once_with(expected_cmd)


def test_print_resources(capsys, config_mock):
    with (
        patch.object(cmd_list, "load_json") as mock_load,
        patch.object(cmd_list, "check_white_list") as mock_check,
    ):
        mock_load.return_value = [
            {
                "name": "res1_protected",
                "assetType": "type1",
                "location": "loc1",
                "labels": {"l1": "v1"},
            },
            {"name": "res2_safe", "assetType": "type2", "location": "loc2"},
        ]

        # Res1 is NOT safe (False), Res2 is safe (True)
        mock_check.side_effect = [False, True]

        _print_resources(Path("fake.json"), config_mock)

        captured = capsys.readouterr()
        assert "(res2_safe)" in captured.out
        assert "Resource Count         : 1" in captured.out
        assert "Ignored Resource Count : 1" in captured.out


def test_handle_list(mock_args):
    with (
        patch.object(cmd_list, "init_directories") as mock_init,
        patch.object(cmd_list, "get_resources_file") as mock_get_file,
        patch.object(cmd_list, "ensure_path") as mock_ensure,
        patch.object(cmd_list, "_sync_resources") as mock_sync,
        patch.object(GCPProjectConfig, "load_from_file") as mock_load_cfg,
        patch.object(cmd_list, "_print_resources") as mock_print,
    ):
        # Customize mock_args for this case
        mock_args.sync = True
        mock_args.asset_type = "compute"

        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = False
        mock_path.parent = "fake_parent"
        mock_get_file.return_value = mock_path

        handle_list(None, mock_args)

        mock_init.assert_called_once()
        mock_get_file.assert_called_once_with("test-proj", "compute", resource_file_path=None)
        mock_ensure.assert_called_once_with("fake_parent")
        mock_sync.assert_called_once_with("test-proj", mock_path, "compute")
        mock_load_cfg.assert_called_once_with("test-proj", config_path=None)
        mock_print.assert_called_once()
