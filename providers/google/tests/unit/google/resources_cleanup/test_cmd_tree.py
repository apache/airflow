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
from unittest.mock import MagicMock, mock_open, patch

from airflow_google_provider_resource_cleanup.commands.cmd_tree import (
    _build_resource_tree,
    _format_node_for_template,
    handler_tree,
    print_tree,
)


def test_format_node_for_template():
    node_details = {
        "data": {
            "name": "my-instance",
            "type": "compute.googleapis.com/Instance",
            "id": "12345",
            "location": "us-central1-a",
            "is_protected": True,
        },
        "children": {
            "child1": {
                "data": {"name": "disk-b", "type": "disk"},
                "children": {},
            },
            "child2": {
                "data": {"name": "disk-a", "type": "disk"},
                "children": {},
            },
        },
    }

    formatted = _format_node_for_template(node_details)

    assert formatted["display_name"] == "my-instance"
    assert formatted["asset_type_short"] == "Instance"
    assert formatted["is_protected"] is True
    assert len(formatted["children"]) == 2
    assert formatted["children"][0]["display_name"] == "disk-a"
    assert formatted["children"][1]["display_name"] == "disk-b"


def test_build_resource_tree():
    with patch("airflow_google_provider_resource_cleanup.commands.cmd_tree.check_white_list") as mock_check:
        mock_check.return_value = True

        resources_data = [
            {
                "name": ("//compute.googleapis.com/projects/p1/zones/z1/instances/i1"),
                "displayName": "instance-1",
                "assetType": "compute.googleapis.com/Instance",
                "parentFullResourceName": ("//cloudresourcemanager.googleapis.com/projects/p1"),
            },
            {
                "name": "//cloudresourcemanager.googleapis.com/projects/p1",
                "displayName": "my-project",
                "assetType": "cloudresourcemanager.googleapis.com/Project",
            },
        ]

        cfg = MagicMock()
        tree = _build_resource_tree(resources_data, cfg)

        assert len(tree) == 1
        assert tree[0]["display_name"] == "my-project"
        assert len(tree[0]["children"]) == 1
        assert tree[0]["children"][0]["display_name"] == "instance-1"


def test_print_tree(capsys):
    nodes_list = [
        {
            "display_name": "Parent",
            "asset_type_short": "Project",
            "location": "global",
            "is_protected": True,
            "children": [
                {
                    "display_name": "Child",
                    "asset_type_short": "Instance",
                    "location": "us-central1",
                    "is_protected": False,
                    "children": [],
                }
            ],
        }
    ]

    print_tree(nodes_list)

    captured = capsys.readouterr()
    assert "- Parent (Project @ global) (Protected)" in captured.out
    assert "\t- Child (Instance @ us-central1)" in captured.out


def test_handler_tree(mock_args):
    with (
        patch("airflow_google_provider_resource_cleanup.commands.cmd_tree.init_directories") as mock_init,
        patch(
            "airflow_google_provider_resource_cleanup.commands.cmd_tree.get_resources_file"
        ) as mock_get_file,
        patch(
            "airflow_google_provider_resource_cleanup.commands.cmd_tree.GCPProjectConfig.load_from_file"
        ) as mock_load_cfg,
        patch("airflow_google_provider_resource_cleanup.commands.cmd_tree.load_json") as mock_load_json,
        patch(
            "airflow_google_provider_resource_cleanup.commands.cmd_tree._build_resource_tree"
        ) as mock_build,
        patch(
            "airflow_google_provider_resource_cleanup.commands.cmd_tree._get_html_template"
        ) as mock_get_template,
        patch(
            "airflow_google_provider_resource_cleanup.commands.cmd_tree._get_html_file"
        ) as mock_get_file_path,
        patch("airflow_google_provider_resource_cleanup.commands.cmd_tree.run_command") as mock_run,
    ):
        # Mock setup
        mock_resource_file = MagicMock(spec=Path)
        mock_resource_file.exists.return_value = True
        mock_get_file.return_value = mock_resource_file

        mock_template = MagicMock()
        mock_template.render.return_value = "<html>Content</html>"
        mock_get_template.return_value = mock_template
        mock_load_json.return_value = []

        mock_html_file = MagicMock(spec=Path)
        mock_get_file_path.return_value = mock_html_file
        m_open = mock_open()
        mock_html_file.open.return_value = m_open()

        handler_tree(None, mock_args)

        mock_init.assert_called_once()
        mock_load_cfg.assert_called_once_with("test-proj", config_path=None)
        mock_load_json.assert_called_once_with(mock_resource_file)
        mock_build.assert_called_once()
        mock_run.assert_called()
