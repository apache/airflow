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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from airflow_google_provider_resource_cleanup import constants as c, helpers
from airflow_google_provider_resource_cleanup.helpers import (
    GCPProjectConfig,
    check_white_list,
    dump_json,
    get_auxiliary_asset_data_item,
    get_resource_name_for_compute,
    get_resource_path,
    get_resources_file,
    load_json,
    provide_auxiliary_asset_resource_file,
)


def test_get_resources_file_uses_override(tmp_path):
    override = tmp_path / "custom.json"

    assert get_resources_file("test-project", resource_file_path=override) == override


def test_get_resources_file_returns_project_resources_file():
    assert get_resources_file("test-project") == c.RESOURCES_FOLDER / "test-project" / "resources.json"


def test_get_resources_file_returns_asset_type_resources_file():
    assert (
        get_resources_file("test-project", "compute") == c.RESOURCES_FOLDER / "test-project" / "compute.json"
    )


def test_load_and_dump_json(tmp_path):
    target = tmp_path / "data.json"
    data = {"items": ["one", "two"]}

    dump_json(target, data)

    assert load_json(target) == data


def test_check_white_list_rejects_protected_asset_type(capsys, config_mock):
    assert not check_white_list({"assetType": "iam.googleapis.com/ServiceAccount"}, config_mock)

    assert (
        'The asset type: "iam.googleapis.com/ServiceAccount" is protected. Ignoring!'
        in capsys.readouterr().out
    )


def test_check_white_list_rejects_protected_resource(capsys):
    config = GCPProjectConfig(
        project_id="test-project",
        protected_resources={"compute.googleapis.com/Instance": ["protected-instance"]},
    )

    assert not check_white_list(
        {"name": "protected-instance", "assetType": "compute.googleapis.com/Instance"}, config
    )

    assert 'The resource: "protected-instance" is protected. Ignoring!' in capsys.readouterr().out


@pytest.mark.parametrize(
    "labels",
    [
        {"do-not-delete": "true"},
        {"owner": "airflow"},
        {"purpose": "dont-delete"},
    ],
)
def test_check_white_list_rejects_protected_labels(labels, config_mock):
    assert not check_white_list(
        {
            "name": "protected-by-label",
            "assetType": "compute.googleapis.com/Instance",
            "labels": labels,
        },
        config_mock,
        silent=True,
    )


def test_check_white_list_allows_unprotected_resource(config_mock):
    assert check_white_list(
        {
            "name": "test-instance",
            "assetType": "compute.googleapis.com/Instance",
            "labels": {"airflow-system-test": "true"},
        },
        config_mock,
    )


def test_get_resource_path():
    assert (
        get_resource_path(
            {"name": "//compute.googleapis.com/projects/test-project/zones/us-central1-a/disks/d1"}
        )
        == "projects/test-project/zones/us-central1-a/disks/d1"
    )


def test_get_resource_path_rejects_non_string_name():
    with pytest.raises(TypeError, match="Resource name must be a string"):
        get_resource_path({"name": None})


def test_get_resource_name_for_compute():
    assert (
        get_resource_name_for_compute(
            {"name": "//compute.googleapis.com/projects/test-project/zones/us-central1-a/instances/i1"}
        )
        == "projects/test-project/zones/us-central1-a/instances/i1"
    )


def test_gcp_project_config_load_from_file_creates_default_config(tmp_path):
    config_file = tmp_path / "config.json"

    config = GCPProjectConfig.load_from_file("test-project", config_path=config_file)

    assert config == GCPProjectConfig(project_id="test-project")
    assert load_json(config_file) == {
        "project_id": "test-project",
        "protected_resources": {},
        "default_location": "us-central1",
    }


def test_gcp_project_config_load_from_file_overrides_project_id(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
        {
          "project_id": "ignored-project",
          "protected_resources": {
            "storage.googleapis.com/Bucket": ["//storage.googleapis.com/bucket"]
          },
          "default_location": "europe-west1"
        }
        """
    )

    config = GCPProjectConfig.load_from_file("test-project", config_path=config_file)

    assert config.project_id == "test-project"
    assert config.default_location == "europe-west1"
    assert config.protected_resources == {
        "storage.googleapis.com/Bucket": ["//storage.googleapis.com/bucket"]
    }
    assert load_json(config_file)["project_id"] == "test-project"


def test_gcp_project_config_load_from_file_rejects_non_object(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text("[]")

    with pytest.raises(TypeError, match="to be a JSON object"):
        GCPProjectConfig.load_from_file("test-project", config_path=config_file)


def test_get_auxiliary_asset_data_item_uses_project_config_location(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text('{"default_location": "europe-west1"}')

    assert get_auxiliary_asset_data_item(
        "test-project", asset_type="vertex_ai_raycluster", config_path=config_file
    ) == {
        "assetType": "vertex_ai_raycluster",
        "project": "test-project",
        "location": "europe-west1",
    }


def test_provide_auxiliary_asset_resource_file(tmp_path):
    resources_file = tmp_path / "resources.json"
    config_file = tmp_path / "config.json"
    config_file.write_text('{"default_location": "europe-west1"}')

    provide_auxiliary_asset_resource_file(
        "test-project",
        "vertex_ai_raycluster",
        resource_file_path=resources_file,
        config_path=config_file,
    )

    assert load_json(resources_file) == [
        {
            "assetType": "vertex_ai_raycluster",
            "project": "test-project",
            "location": "europe-west1",
        }
    ]


def test_init_directories():
    with patch.object(helpers, "ensure_path") as mock_ensure_path:
        helpers.init_directories()

    mock_ensure_path.assert_any_call(c.RESOURCES_FOLDER)
    mock_ensure_path.assert_any_call(c.OUTPUT_FOLDER)


def test_run_command():
    with patch.object(helpers.os, "system") as mock_system:
        helpers.run_command("echo test", log_prefix="[test] ")

    mock_system.assert_called_once_with("echo test")


@pytest.mark.anyio
async def test_run_command_async():
    process = MagicMock()
    process.returncode = 0
    process.communicate = AsyncMock(return_value=(b"out", b"err"))

    with patch.object(
        helpers.asyncio, "create_subprocess_shell", AsyncMock(return_value=process)
    ) as mock_create:
        await helpers.run_command_async("echo test")

    mock_create.assert_awaited_once()
