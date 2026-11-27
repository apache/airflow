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

import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from airflow_google_provider_resource_cleanup import constants as c, helpers
from airflow_google_provider_resource_cleanup.helpers import (
    GCPProjectConfig,
    build_project_flag,
    check_white_list,
    dump_json,
    get_auxiliary_asset_data_item,
    get_resource_name_for_compute,
    get_resource_path,
    get_resources_file,
    load_json,
    provide_auxiliary_asset_resource_file,
    resolve_project_id,
    resolve_resource_path,
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


@pytest.mark.parametrize(
    ("resource", "project_id", "expected_project_id"),
    [
        (
            {"name": "//spanner.googleapis.com/projects/path-project/instances/i1"},
            "configured-project",
            "path-project",
        ),
        (
            {
                "name": "//spanner.googleapis.com/projects/123456789012/instances/i1",
                "project": "projects/resource-project",
            },
            "configured-project",
            "resource-project",
        ),
        (
            {
                "name": "//spanner.googleapis.com/projects/123456789012/instances/i1",
                "project": "projects/123456789012",
            },
            "configured-project",
            "configured-project",
        ),
        (
            {"name": "//storage.googleapis.com/projects/_/buckets/test-bucket"},
            "configured-project",
            "configured-project",
        ),
        (
            {"name": "//storage.googleapis.com/test-bucket"},
            "configured-project",
            "configured-project",
        ),
        (
            {
                "name": "//storage.googleapis.com/test-bucket",
                "project": 123456789012,
            },
            "123456789012",
            None,
        ),
        (
            {"name": "//storage.googleapis.com/test-bucket"},
            None,
            None,
        ),
    ],
)
def test_resolve_project_id(resource, project_id, expected_project_id):
    assert resolve_project_id(resource, project_id=project_id) == expected_project_id


@pytest.mark.parametrize(
    ("resource", "project_id", "expected_flag"),
    [
        (
            {"name": "//storage.googleapis.com/projects/_/buckets/test-bucket"},
            "configured-project",
            " --project=configured-project",
        ),
        (
            {"name": "//storage.googleapis.com/projects/_/buckets/test-bucket"},
            None,
            "",
        ),
    ],
)
def test_build_project_flag(resource, project_id, expected_flag):
    assert build_project_flag(resource, project_id=project_id) == expected_flag


@pytest.mark.parametrize(
    ("resource", "project_id", "expected_path"),
    [
        (
            {"name": "//dlp.googleapis.com/projects/123456789012/locations/us-central1/jobTriggers/t1"},
            "configured-project",
            "projects/configured-project/locations/us-central1/jobTriggers/t1",
        ),
        (
            {
                "name": "//dlp.googleapis.com/projects/123456789012/locations/us-central1/jobTriggers/t1",
                "project": "projects/resource-project",
            },
            "configured-project",
            "projects/resource-project/locations/us-central1/jobTriggers/t1",
        ),
        (
            {"name": "//dlp.googleapis.com/projects/123456789012/locations/us-central1/jobTriggers/t1"},
            None,
            "projects/123456789012/locations/us-central1/jobTriggers/t1",
        ),
        (
            {"name": "//dlp.googleapis.com/projects/path-project/locations/us-central1/jobTriggers/t1"},
            "configured-project",
            "projects/path-project/locations/us-central1/jobTriggers/t1",
        ),
        (
            {"name": "//storage.googleapis.com/test-bucket"},
            "configured-project",
            "test-bucket",
        ),
    ],
)
def test_resolve_resource_path(resource, project_id, expected_path):
    assert resolve_resource_path(resource, project_id=project_id) == expected_path


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
    config_content = """
        {
          "project_id": "ignored-project",
          "protected_resources": {
            "storage.googleapis.com/Bucket": ["//storage.googleapis.com/bucket"]
          },
          "default_location": "europe-west1"
        }
        """
    config_file.write_text(config_content)

    config = GCPProjectConfig.load_from_file("test-project", config_path=config_file)

    assert config.project_id == "test-project"
    assert config.default_location == "europe-west1"
    assert config.protected_resources == {
        "storage.googleapis.com/Bucket": ["//storage.googleapis.com/bucket"]
    }
    assert config_file.read_text() == config_content


def test_gcp_project_config_load_from_file_rejects_non_object(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text("[]")

    with pytest.raises(TypeError, match="to be a JSON object"):
        GCPProjectConfig.load_from_file("test-project", config_path=config_file)


def test_gcp_project_config_load_from_file_rejects_unknown_keys(tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text('{"unexpected": "value"}')

    with pytest.raises(ValueError, match="Unexpected key\\(s\\).*unexpected"):
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
    env = {"TEST_ENV": "value"}

    with patch.object(
        helpers.asyncio, "create_subprocess_shell", AsyncMock(return_value=process)
    ) as mock_create:
        result = await helpers.run_command_async("echo test", env=env)

    assert result is None
    mock_create.assert_awaited_once_with(
        "echo test",
        stdout=helpers.asyncio.subprocess.PIPE,
        stderr=helpers.asyncio.subprocess.PIPE,
        env=env,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("stdout", [b"", None])
@pytest.mark.parametrize(
    "stderr",
    [
        "NOT_FOUND: resource is gone",
        "resource was not found",
        "resource does not exist",
        "matched no objects or buckets",
        "curl: (22) The requested URL returned error: 404",
    ],
)
async def test_checked_command_accepts_resource_that_is_already_gone(stdout, stderr):
    process = MagicMock(returncode=1)
    process.communicate = AsyncMock(return_value=(stdout, stderr.encode()))

    with patch.object(helpers.asyncio, "create_subprocess_shell", return_value=process):
        result = await helpers.run_command_async("delete command", check=True, ignore_not_found=True)

    assert result is False


@pytest.mark.anyio
async def test_checked_command_raises_for_real_delete_failure():
    process = MagicMock(returncode=1)
    process.communicate = AsyncMock(return_value=(b"", b"PERMISSION_DENIED"))

    with (
        patch.object(helpers.asyncio, "create_subprocess_shell", return_value=process),
        pytest.raises(subprocess.CalledProcessError),
    ):
        await helpers.run_command_async("delete command", check=True, ignore_not_found=True)


def test_get_access_token_refreshes_and_reuses_credentials(monkeypatch):
    credentials = MagicMock(valid=False, token=None)

    def refresh(_request):
        credentials.valid = True
        credentials.token = "test-token"

    credentials.refresh.side_effect = refresh
    mock_default = MagicMock(return_value=(credentials, None))
    monkeypatch.setattr(helpers, "_google_credentials", None)
    monkeypatch.setattr(helpers.google.auth, "default", mock_default)

    assert helpers._get_access_token() == "test-token"
    assert helpers._get_access_token() == "test-token"

    mock_default.assert_called_once_with(scopes=[helpers.GOOGLE_CLOUD_PLATFORM_SCOPE])
    credentials.refresh.assert_called_once()


def test_get_access_token_requires_token(monkeypatch):
    credentials = MagicMock(valid=True, token=None)
    monkeypatch.setattr(helpers, "_google_credentials", credentials)

    with pytest.raises(RuntimeError, match="did not provide an access token"):
        helpers._get_access_token()


@pytest.mark.anyio
async def test_curl_uses_http1_and_retries():
    url = "https://dlp.googleapis.com/v2/projects/test-project/locations/global/dlpJobs/test-job"
    data = {"key": "value"}

    with (
        patch.object(helpers, "_get_access_token", return_value="test-token") as mock_get_token,
        patch.object(helpers, "run_command_async", AsyncMock(return_value=None)) as mock_run,
    ):
        await helpers.curl(url, method="PUT", log_prefix="[1/1] ", data=data)

    command = mock_run.await_args.args[0]
    assert command.startswith("curl --fail --silent --show-error --http1.1")
    assert "--retry 5 --retry-delay 5 --retry-max-time 120" in command
    assert "-X PUT" in command
    assert "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" in command
    assert "--data" in command
    assert url in command
    assert "test-token" not in command
    assert mock_run.await_args.kwargs["env"]["GOOGLE_OAUTH_ACCESS_TOKEN"] == "test-token"
    assert mock_run.await_args.kwargs["check"] is True
    assert mock_run.await_args.kwargs["ignore_not_found"] is True
    mock_get_token.assert_called_once_with()


@pytest.mark.anyio
async def test_curl_sets_quota_project_header_when_present(monkeypatch):
    url = "https://dlp.googleapis.com/v2/projects/test-project/locations/global/dlpJobs/test-job"
    monkeypatch.setattr(helpers, "_google_credentials", MagicMock(quota_project_id="quota-project"))

    with (
        patch.object(helpers, "_get_access_token", return_value="test-token"),
        patch.object(helpers, "run_command_async", AsyncMock(return_value=None)) as mock_run,
    ):
        await helpers.curl(url, log_prefix="[1/1] ", check=False, ignore_not_found=False)

    command = mock_run.await_args.args[0]
    assert "-H 'x-goog-user-project: quota-project'" in command
    assert mock_run.await_args.kwargs["check"] is False
    assert mock_run.await_args.kwargs["ignore_not_found"] is False
