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
from unittest.mock import patch

import pytest
from airflow_google_provider_resource_cleanup.commands import cmd_list_asset_types
from airflow_google_provider_resource_cleanup.commands.cmd_list_asset_types import (
    _get_asset_hierarchy,
    _print_tree,
    handle_list_asset_types,
)


def test_get_asset_hierarchy_builds_tree(tmp_path):
    resources_file = tmp_path / "resources.json"
    resources_file.write_text(
        """
        [
          {
            "assetType": "compute.googleapis.com/Instance",
            "parentAssetType": "cloudresourcemanager.googleapis.com/Project"
          },
          {
            "assetType": "compute.googleapis.com/Disk",
            "parentAssetType": "compute.googleapis.com/Instance"
          }
        ]
        """
    )

    hierarchy = _get_asset_hierarchy(resources_file)

    assert set(hierarchy) == {"cloudresourcemanager.googleapis.com/Project"}
    assert set(hierarchy["cloudresourcemanager.googleapis.com/Project"]["children"]) == {
        "compute.googleapis.com/Instance"
    }
    assert set(
        hierarchy["cloudresourcemanager.googleapis.com/Project"]["children"][
            "compute.googleapis.com/Instance"
        ]["children"]
    ) == {"compute.googleapis.com/Disk"}


def test_get_asset_hierarchy_rejects_non_list_json(tmp_path):
    resources_file = tmp_path / "resources.json"
    resources_file.write_text('{"assetType": "compute.googleapis.com/Instance"}')

    with pytest.raises(TypeError, match="to contain a JSON list"):
        _get_asset_hierarchy(resources_file)


def test_print_tree_sorts_nodes(capsys):
    _print_tree(
        {
            "b.googleapis.com/Parent": {"children": {}},
            "a.googleapis.com/Parent": {
                "children": {
                    "z.googleapis.com/Child": {"children": {}},
                    "c.googleapis.com/Child": {"children": {}},
                }
            },
        }
    )

    assert capsys.readouterr().out.splitlines() == [
        "├── a.googleapis.com/Parent",
        "│   ├── c.googleapis.com/Child",
        "│   └── z.googleapis.com/Child",
        "└── b.googleapis.com/Parent",
    ]


def test_handle_list_asset_types(mock_args, capsys):
    with (
        patch.object(cmd_list_asset_types, "get_resources_file") as mock_get_resources_file,
        patch.object(cmd_list_asset_types, "_get_asset_hierarchy") as mock_get_asset_hierarchy,
        patch.object(cmd_list_asset_types, "_print_tree") as mock_print_tree,
    ):
        mock_resource_file = Path("resources.json")
        mock_get_resources_file.return_value = mock_resource_file
        mock_get_asset_hierarchy.return_value = {"compute.googleapis.com/Instance": {"children": {}}}

        handle_list_asset_types(None, mock_args)

        mock_get_resources_file.assert_called_once_with("test-proj", None, resource_file_path=None)
        mock_get_asset_hierarchy.assert_called_once_with(mock_resource_file)
        mock_print_tree.assert_called_once_with({"compute.googleapis.com/Instance": {"children": {}}})
        assert "=" * 200 in capsys.readouterr().out


def test_handle_list_asset_types_exits_when_resource_file_is_missing(mock_args, capsys):
    with (
        patch.object(cmd_list_asset_types, "get_resources_file", return_value=Path("missing.json")),
        patch.object(cmd_list_asset_types, "_get_asset_hierarchy", side_effect=FileNotFoundError),
    ):
        with pytest.raises(SystemExit) as exc_info:
            handle_list_asset_types(None, mock_args)

    assert exc_info.value.code == 1
    assert 'The resource file "missing.json" cannot be found! Exiting...' in capsys.readouterr().out
