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

from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import cloudbuild
from google.api_core import exceptions

pytestmark = pytest.mark.anyio


def test_cloudbuild_handler_deleters_match_deletion_order():
    handler = cloudbuild.CloudBuildDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER == [
        "cloudbuild.googleapis.com/BuildTrigger",
        "cloudbuild.googleapis.com/WorkerPool",
    ]


async def test_delete_build_trigger_success():
    mock_client = AsyncMock()
    mock_client.delete_build_trigger.return_value = None
    with patch.object(cloudbuild, "_get_cloudbuild_client", return_value=mock_client):
        res = {
            "name": "//cloudbuild.googleapis.com/projects/p/locations/global/triggers/trig1",
            "assetType": "cloudbuild.googleapis.com/BuildTrigger",
        }
        result = await cloudbuild._delete_build_trigger(res, "[1/1] ")

    assert result is True
    mock_client.delete_build_trigger.assert_awaited_once_with(
        name="projects/p/locations/global/triggers/trig1"
    )


async def test_delete_worker_pool_success():
    mock_client = AsyncMock()
    mock_operation = AsyncMock()
    mock_client.delete_worker_pool.return_value = mock_operation
    with patch.object(cloudbuild, "_get_cloudbuild_client", return_value=mock_client):
        res = {
            "name": "//cloudbuild.googleapis.com/projects/p/locations/us-central1/workerPools/wp1",
            "assetType": "cloudbuild.googleapis.com/WorkerPool",
        }
        result = await cloudbuild._delete_worker_pool(res, "[1/1] ")

    assert result is True
    mock_client.delete_worker_pool.assert_awaited_once_with(
        name="projects/p/locations/us-central1/workerPools/wp1"
    )
    mock_operation.result.assert_awaited_once_with()


async def test_delete_resource_handles_not_found():
    mock_client = AsyncMock()
    mock_client.delete_build_trigger.side_effect = exceptions.NotFound("BuildTrigger not found")
    with patch.object(cloudbuild, "_get_cloudbuild_client", return_value=mock_client):
        res = {
            "name": "//cloudbuild.googleapis.com/projects/p/locations/global/triggers/trig1",
            "assetType": "cloudbuild.googleapis.com/BuildTrigger",
        }
        result = await cloudbuild._delete_build_trigger(res, "[1/1] ")

    assert result is True


async def test_delete_resource_handles_failed_precondition():
    mock_client = AsyncMock()
    mock_client.delete_worker_pool.side_effect = exceptions.FailedPrecondition("WorkerPool in use")
    with patch.object(cloudbuild, "_get_cloudbuild_client", return_value=mock_client):
        res = {
            "name": "//cloudbuild.googleapis.com/projects/p/locations/us-central1/workerPools/wp1",
            "assetType": "cloudbuild.googleapis.com/WorkerPool",
        }
        result = await cloudbuild._delete_worker_pool(res, "[1/1] ")

    assert result is False


async def test_delete_resource_handles_generic_exception():
    mock_client = AsyncMock()
    mock_client.delete_build_trigger.side_effect = RuntimeError("Connection error")
    with patch.object(cloudbuild, "_get_cloudbuild_client", return_value=mock_client):
        res = {
            "name": "//cloudbuild.googleapis.com/projects/p/locations/global/triggers/trig1",
            "assetType": "cloudbuild.googleapis.com/BuildTrigger",
        }
        result = await cloudbuild._delete_build_trigger(res, "[1/1] ")

    assert result is False
