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

from airflow.providers.google.cloud.triggers.mlengine import MLEngineStartTrainingJobTrigger
from airflow.triggers.base import TriggerEvent

if sys.version_info < (3, 8):
    from asynctest import mock
else:
    from unittest import mock

TEST_CONN_ID = "ml_default"
TEST_JOB_ID = "1234"
TEST_GCP_PROJECT_ID = "test-project"
TEST_REGION = "us-central1"
TEST_RUNTIME_VERSION = "1.15"
TEST_PYTHON_VERSION = "3.7"
TEST_JOB_DIR = "gs://example_mlengine_bucket/job-dir"
TEST_PACKAGE_URIS = ["gs://system-tests-resources/example_gcp_mlengine/trainer-0.1.tar.gz"]
TEST_TRAINING_PYTHON_MODULE = "trainer.task"
TEST_TRAINING_ARGS = []
TEST_LABELS = {"job_type": "training", "***-version": "v2-5-0-dev0"}
TEST_POLL_INTERVAL = 4.0


def test_mlengine_insert_job_trigger_serialization_should_execute_successfully():
    """
    Asserts that the MLEngineStartTrainingJobTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = MLEngineStartTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        region=TEST_REGION,
        runtime_version=TEST_RUNTIME_VERSION,
        python_version=TEST_PYTHON_VERSION,
        job_dir=TEST_JOB_DIR,
        package_uris=TEST_PACKAGE_URIS,
        training_python_module=TEST_TRAINING_PYTHON_MODULE,
        training_args=TEST_TRAINING_ARGS,
        labels=TEST_LABELS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.mlengine.MLEngineStartTrainingJobTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "job_id": TEST_JOB_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "region": TEST_REGION,
        "runtime_version": TEST_RUNTIME_VERSION,
        "python_version": TEST_PYTHON_VERSION,
        "job_dir": TEST_JOB_DIR,
        "poll_interval": TEST_POLL_INTERVAL,
        "package_uris": TEST_PACKAGE_URIS,
        "training_python_module": TEST_TRAINING_PYTHON_MODULE,
        "training_args": TEST_TRAINING_ARGS,
        "labels": TEST_LABELS,
    }


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineAsyncHook.get_job_status")
async def test_mlengine_insert_job_trigger_triggers_on_success_should_execute_successfully(mock_job_status):
    """
    Tests the MLEngineStartTrainingJobTrigger only fires once the job execution reaches a successful state.
    """
    mock_job_status.return_value = "success"

    trigger = MLEngineStartTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        region=TEST_REGION,
        runtime_version=TEST_RUNTIME_VERSION,
        python_version=TEST_PYTHON_VERSION,
        job_dir=TEST_JOB_DIR,
        package_uris=TEST_PACKAGE_URIS,
        training_python_module=TEST_TRAINING_PYTHON_MODULE,
        training_args=TEST_TRAINING_ARGS,
        labels=TEST_LABELS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "message": "Job completed", "job_id": TEST_JOB_ID}) == actual


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineAsyncHook.get_job")
async def test_mlengine_insert_job_trigger_triggers_on_running_should_execute_successfully(
    mocked_get, caplog
):
    """
    Test that MLEngineStartTrainingJobTrigger does not fire while a job is still running.
    """

    mocked_get.side_effect = OSError()
    caplog.set_level(logging.INFO)

    trigger = MLEngineStartTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        region=TEST_REGION,
        runtime_version=TEST_RUNTIME_VERSION,
        python_version=TEST_PYTHON_VERSION,
        job_dir=TEST_JOB_DIR,
        package_uris=TEST_PACKAGE_URIS,
        training_python_module=TEST_TRAINING_PYTHON_MODULE,
        training_args=TEST_TRAINING_ARGS,
        labels=TEST_LABELS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Job is still running..." in caplog.text
    assert f"Sleeping for {TEST_POLL_INTERVAL} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineAsyncHook.get_job_status")
async def test_mlengine_insert_job_trigger_triggers_on_error_should_execute_successfully(mock_job_status):
    """
    Test that MLEngineStartTrainingJobTrigger fires the correct event in case of an error.
    """
    # Set the status to a value other than success or pending
    mock_job_status.return_value = "error"

    trigger = MLEngineStartTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        region=TEST_REGION,
        runtime_version=TEST_RUNTIME_VERSION,
        python_version=TEST_PYTHON_VERSION,
        job_dir=TEST_JOB_DIR,
        package_uris=TEST_PACKAGE_URIS,
        training_python_module=TEST_TRAINING_PYTHON_MODULE,
        training_args=TEST_TRAINING_ARGS,
        labels=TEST_LABELS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "error"}) == actual


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineAsyncHook.get_job_status")
async def test_mlengine_insert_job_trigger_triggers_on_excp_should_execute_successfully(mock_job_status):
    """
    Test that MLEngineStartTrainingJobTrigger fires the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")

    trigger = MLEngineStartTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        region=TEST_REGION,
        runtime_version=TEST_RUNTIME_VERSION,
        python_version=TEST_PYTHON_VERSION,
        job_dir=TEST_JOB_DIR,
        package_uris=TEST_PACKAGE_URIS,
        training_python_module=TEST_TRAINING_PYTHON_MODULE,
        training_args=TEST_TRAINING_ARGS,
        labels=TEST_LABELS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual
