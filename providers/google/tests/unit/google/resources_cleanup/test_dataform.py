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
from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import dataform
from airflow_google_provider_resource_cleanup.handlers.dataform import (
    DataformDeleteHandler,
    _delete_dataform_repository,
    _delete_dataform_resource,
)


@pytest.mark.anyio
async def test_delete_dataform_resource():
    resource = {
        "name": (
            "//dataform.googleapis.com/projects/test-project/locations/us-central1/"
            "repositories/test-repository/workflowInvocations/test-invocation"
        )
    }

    with patch.object(dataform, "curl", AsyncMock()) as mock_curl:
        await _delete_dataform_resource(resource, "[1/1] ")

    mock_curl.assert_awaited_once_with(
        "https://dataform.googleapis.com/v1/projects/test-project/locations/us-central1/"
        "repositories/test-repository/workflowInvocations/test-invocation",
        log_prefix="[1/1] ",
    )


@pytest.mark.anyio
async def test_delete_dataform_repository_with_child_resources():
    resource = {
        "name": "//dataform.googleapis.com/projects/test-project/locations/us-central1/repositories/repo"
    }

    with patch.object(dataform, "curl", AsyncMock()) as mock_curl:
        await _delete_dataform_repository(resource, "[1/1] ")

    mock_curl.assert_awaited_once_with(
        "https://dataform.googleapis.com/v1/projects/test-project/locations/us-central1/"
        "repositories/repo?force=true",
        log_prefix="[1/1] ",
    )


@pytest.mark.anyio
async def test_delete_dataform_resource_propagates_curl_failure():
    resource = {
        "name": "//dataform.googleapis.com/projects/test-project/locations/us-central1/repositories/repo"
    }

    with (
        patch.object(
            dataform,
            "curl",
            AsyncMock(side_effect=subprocess.CalledProcessError(92, "curl")),
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        await _delete_dataform_resource(resource, "[1/1] ")


def test_dataform_handler_limits_api_requests():
    assert DataformDeleteHandler.SEMAPHORE_COUNT == 1
    assert DataformDeleteHandler.SLEEP_AFTER_EACH_REQUEST == 1
    assert set(DataformDeleteHandler.DELETERS.values()) == {
        _delete_dataform_resource,
        _delete_dataform_repository,
    }


def test_dataform_handler_deletes_children_before_repository():
    assert DataformDeleteHandler.DELETION_ORDER == [
        "dataform.googleapis.com/WorkflowInvocation",
        "dataform.googleapis.com/WorkflowConfig",
        "dataform.googleapis.com/ReleaseConfig",
        "dataform.googleapis.com/Workspace",
        "dataform.googleapis.com/Repository",
    ]
