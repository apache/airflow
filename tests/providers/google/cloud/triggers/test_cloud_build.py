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

import asyncio
import logging
import sys

import pytest
from google.cloud.devtools.cloudbuild_v1 import CloudBuildAsyncClient
from google.cloud.devtools.cloudbuild_v1.types import Build, BuildStep

from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildAsyncHook
from airflow.providers.google.cloud.triggers.cloud_build import CloudBuildCreateBuildTrigger
from airflow.triggers.base import TriggerEvent

if sys.version_info < (3, 8):
    from asynctest import mock
else:
    from unittest import mock

CLOUD_BUILD_PATH = "airflow.providers.google.cloud.hooks.cloud_build.{}"
TEST_PROJECT_ID = "cloud-build-project"
TEST_BUILD_ID = "test-build-id-9832662"
REPO_SOURCE = {"repo_source": {"repo_name": "test_repo", "branch_name": "main"}}
TEST_BUILD = {
    "source": REPO_SOURCE,
    "steps": [{"name": "gcr.io/cloud-builders/gcloud", "entrypoint": "/bin/sh", "args": ["-c", "ls"]}],
    "status": "SUCCESS",
}
TEST_BUILD_WORKING = {
    "source": REPO_SOURCE,
    "steps": [{"name": "gcr.io/cloud-builders/gcloud", "entrypoint": "/bin/sh", "args": ["-c", "ls"]}],
    "status": "WORKING",
}

TEST_CONN_ID = "google_cloud_default"
TEST_POLL_INTERVAL = 4.0
TEST_BUILD_INSTANCE = dict(
    id="test-build-id-9832662",
    status=3,
    steps=[
        {
            "name": "ubuntu",
            "env": [],
            "args": [],
            "dir_": "",
            "id": "",
            "wait_for": [],
            "entrypoint": "",
            "secret_env": [],
            "volumes": [],
            "status": 0,
            "script": "",
        }
    ],
    name="",
    project_id="",
    status_detail="",
    images=[],
    logs_bucket="",
    build_trigger_id="",
    log_url="",
    substitutions={},
    tags=[],
    secrets=[],
    timing={},
    service_account="",
    warnings=[],
)


@pytest.fixture
def hook():
    return CloudBuildAsyncHook(
        gcp_conn_id="google_cloud_default",
    )


def test_async_create_build_trigger_serialization_should_execute_successfully():
    """
    Asserts that the CloudBuildCreateBuildTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = CloudBuildCreateBuildTrigger(
        id_=TEST_BUILD_ID,
        project_id=TEST_PROJECT_ID,
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=None,
        delegate_to=None,
        poll_interval=TEST_POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.cloud_build.CloudBuildCreateBuildTrigger"
    assert kwargs == {
        "id_": TEST_BUILD_ID,
        "project_id": TEST_PROJECT_ID,
        "gcp_conn_id": TEST_CONN_ID,
        "impersonation_chain": None,
        "delegate_to": None,
        "poll_interval": TEST_POLL_INTERVAL,
    }


@pytest.mark.asyncio
@mock.patch.object(CloudBuildAsyncClient, "__init__", lambda self: None)
@mock.patch(CLOUD_BUILD_PATH.format("CloudBuildAsyncClient.get_build"))
async def test_async_create_build_trigger_triggers_on_success_should_execute_successfully(
    mock_get_build, hook
):
    """
    Tests the CloudBuildCreateBuildTrigger only fires once the job execution reaches a successful state.
    """
    mock_get_build.return_value = Build(
        id=TEST_BUILD_ID, status=Build.Status.SUCCESS, steps=[BuildStep(name="ubuntu")]
    )

    trigger = CloudBuildCreateBuildTrigger(
        id_=TEST_BUILD_ID,
        project_id=TEST_PROJECT_ID,
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=None,
        delegate_to=None,
        poll_interval=TEST_POLL_INTERVAL,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert (
        TriggerEvent(
            {
                "instance": TEST_BUILD_INSTANCE,
                "id_": TEST_BUILD_ID,
                "status": "success",
                "message": "Build completed",
            }
        )
        == actual
    )


@pytest.mark.asyncio
@mock.patch.object(CloudBuildAsyncClient, "__init__", lambda self: None)
@mock.patch(CLOUD_BUILD_PATH.format("CloudBuildAsyncClient.get_build"))
async def test_async_create_build_trigger_triggers_on_running_should_execute_successfully(
    mock_get_build, hook, caplog
):
    """
    Test that CloudBuildCreateBuildTrigger does not fire while a build is still running.
    """
    mock_get_build.return_value = Build(
        id=TEST_BUILD_ID, status=Build.Status.WORKING, steps=[BuildStep(name="ubuntu")]
    )
    caplog.set_level(logging.INFO)

    trigger = CloudBuildCreateBuildTrigger(
        id_=TEST_BUILD_ID,
        project_id=TEST_PROJECT_ID,
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=None,
        delegate_to=None,
        poll_interval=TEST_POLL_INTERVAL,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert "Build is still running..." in caplog.text
    assert f"Sleeping for {TEST_POLL_INTERVAL} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch.object(CloudBuildAsyncClient, "__init__", lambda self: None)
@mock.patch(CLOUD_BUILD_PATH.format("CloudBuildAsyncClient.get_build"))
async def test_async_create_build_trigger_triggers_on_error_should_execute_successfully(
    mock_get_build, hook, caplog
):
    """
    Test that CloudBuildCreateBuildTrigger fires the correct event in case of an error.
    """
    mock_get_build.return_value = Build(
        id=TEST_BUILD_ID, status=Build.Status.FAILURE, steps=[BuildStep(name="ubuntu")], status_detail="error"
    )
    caplog.set_level(logging.INFO)

    trigger = CloudBuildCreateBuildTrigger(
        id_=TEST_BUILD_ID,
        project_id=TEST_PROJECT_ID,
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=None,
        delegate_to=None,
        poll_interval=TEST_POLL_INTERVAL,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "error"}) == actual


@pytest.mark.asyncio
@mock.patch(CLOUD_BUILD_PATH.format("CloudBuildAsyncHook.get_cloud_build"))
async def test_async_create_build_trigger_triggers_on_excp_should_execute_successfully(mock_build_inst):
    """
    Test that CloudBuildCreateBuildTrigger fires the correct event in case of an error.
    """
    mock_build_inst.side_effect = Exception("Test exception")

    trigger = CloudBuildCreateBuildTrigger(
        id_=TEST_BUILD_ID,
        project_id=TEST_PROJECT_ID,
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=None,
        delegate_to=None,
        poll_interval=TEST_POLL_INTERVAL,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual
