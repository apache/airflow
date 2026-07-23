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

import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from airflow_google_provider_resource_cleanup.commands.cmd_delete import (
    check_min_age,
    handle_asset_type,
    handle_delete,
)

pytestmark = pytest.mark.anyio


async def test_handle_asset_type():
    asset_type = "ai"
    mock_check_white_list = MagicMock()
    mock_check_white_list.side_effect = [False, True]
    resources = [{"name": "res1_protected"}, {"name": "res2_safe"}]
    cfg = MagicMock()

    mock_handler_instance = MagicMock()
    mock_handler_instance.handle = AsyncMock()
    mock_handler_cls = MagicMock(return_value=mock_handler_instance)
    mock_delete_handlers = {"ai": mock_handler_cls}

    await handle_asset_type(
        asset_type,
        resources,
        cfg,
        check_white_list_fn=mock_check_white_list,
        delete_handlers=mock_delete_handlers,
    )

    mock_handler_instance.handle.assert_called_once()
    called_args = mock_handler_instance.handle.call_args[0][0]
    assert len(called_args) == 1
    assert called_args[0]["name"] == "res2_safe"


@pytest.fixture
def handle_delete_dependencies():
    mock_gcp_cfg_cls = MagicMock()
    mock_get_file = MagicMock()
    mock_load_json = MagicMock()
    mock_handle_asset = AsyncMock()
    mock_provide_auxiliary_asset = MagicMock()
    mock_get_auxiliary_asset_item = MagicMock()

    mock_resource_file = MagicMock(spec=Path)
    mock_resource_file.exists.return_value = True
    mock_resource_file.absolute.return_value = "/fake/resources.json"
    mock_get_file.return_value = mock_resource_file
    mock_load_json.return_value = [{"name": "res1", "assetType": "compute.googleapis.com/Instance"}]
    return {
        "gcp_project_config_cls": mock_gcp_cfg_cls,
        "provide_auxiliary_asset_resource_file_fn": mock_provide_auxiliary_asset,
        "get_resources_file_fn": mock_get_file,
        "load_json_fn": mock_load_json,
        "get_auxiliary_asset_data_item_fn": mock_get_auxiliary_asset_item,
        "handle_asset_type_fn": mock_handle_asset,
    }


async def test_handle_delete_loads_config(mock_args, handle_delete_dependencies):
    mock_args.asset_type = "compute"
    await handle_delete(None, mock_args, **handle_delete_dependencies)
    handle_delete_dependencies["gcp_project_config_cls"].load_from_file.assert_called_once_with(
        "test-proj", config_path=None
    )


async def test_handle_delete_gets_resources_file(mock_args, handle_delete_dependencies):
    mock_args.asset_type = "compute"
    await handle_delete(None, mock_args, **handle_delete_dependencies)
    handle_delete_dependencies["get_resources_file_fn"].assert_called_once_with(
        "test-proj", "compute", resource_file_path=None
    )


async def test_handle_delete_loads_json(mock_args, handle_delete_dependencies):
    mock_args.asset_type = "compute"
    await handle_delete(None, mock_args, **handle_delete_dependencies)
    handle_delete_dependencies["load_json_fn"].assert_called_once_with(
        handle_delete_dependencies["get_resources_file_fn"].return_value
    )


async def test_handle_delete_calls_handle_asset_type(mock_args, handle_delete_dependencies):
    mock_args.asset_type = "compute"
    await handle_delete(None, mock_args, **handle_delete_dependencies)
    handle_delete_dependencies["handle_asset_type_fn"].assert_called_once()


async def test_handle_delete_prepares_auxiliary_resource_file(mock_args, handle_delete_dependencies):
    mock_args.asset_type = "vertex_ai_raycluster"
    handle_delete_dependencies["load_json_fn"].return_value = [
        {"name": "ray-cluster", "assetType": "vertex_ai_raycluster"}
    ]

    await handle_delete(None, mock_args, **handle_delete_dependencies)

    handle_delete_dependencies["provide_auxiliary_asset_resource_file_fn"].assert_called_once_with(
        "test-proj",
        "vertex_ai_raycluster",
        resource_file_path=None,
        config_path=None,
    )
    handle_delete_dependencies["handle_asset_type_fn"].assert_called_once()


async def test_handle_delete_appends_auxiliary_resources_when_cleaning_all(
    mock_args, handle_delete_dependencies
):
    auxiliary_resource = {
        "assetType": "vertex_ai_raycluster",
        "project": "test-proj",
        "location": "us-central1",
    }
    handle_delete_dependencies["get_auxiliary_asset_data_item_fn"].return_value = auxiliary_resource

    await handle_delete(None, mock_args, **handle_delete_dependencies)

    handle_delete_dependencies["get_auxiliary_asset_data_item_fn"].assert_called_once_with(
        "test-proj", asset_type="vertex_ai_raycluster"
    )
    calls_by_asset_type = {
        call.args[0]: call.args[1]
        for call in handle_delete_dependencies["handle_asset_type_fn"].call_args_list
    }
    assert calls_by_asset_type["vertex_ai_raycluster"] == [auxiliary_resource]


def test_check_min_age():
    now = datetime.datetime(2026, 5, 20, tzinfo=datetime.timezone.utc)

    assert check_min_age({"name": "res1"}, None, now=now)
    assert not check_min_age({"name": "res1"}, 3, now=now)
    assert check_min_age({"name": "old", "createTime": "2026-05-16T00:00:00+00:00"}, 3, now=now)
    assert not check_min_age({"name": "new", "createTime": "2026-05-19T00:00:00+00:00"}, 3, now=now)


async def test_handle_asset_type_filters_by_min_age():
    asset_type = "ai"
    now = datetime.datetime.now(datetime.timezone.utc)
    resources = [
        {"name": "old", "createTime": (now - datetime.timedelta(days=4)).isoformat()},
        {"name": "new", "createTime": now.isoformat()},
    ]
    cfg = MagicMock()

    mock_handler_instance = MagicMock()
    mock_handler_instance.handle = AsyncMock()
    mock_handler_cls = MagicMock(return_value=mock_handler_instance)

    await handle_asset_type(
        asset_type,
        resources,
        cfg,
        min_age_days=3,
        check_white_list_fn=MagicMock(return_value=True),
        delete_handlers={"ai": mock_handler_cls},
    )

    mock_handler_instance.handle.assert_called_once_with([resources[0]])


async def test_handle_delete_skips_requested_asset_type(mock_args, handle_delete_dependencies):
    mock_args.asset_type = "composer"
    mock_args.skip_asset_type = ["composer"]

    await handle_delete(None, mock_args, **handle_delete_dependencies)

    handle_delete_dependencies["get_resources_file_fn"].assert_not_called()
    handle_delete_dependencies["handle_asset_type_fn"].assert_not_called()
