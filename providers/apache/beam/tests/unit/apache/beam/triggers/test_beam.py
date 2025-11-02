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
from unittest import mock

import pytest

from airflow.providers.apache.beam.triggers.beam import BeamJavaPipelineTrigger, BeamPythonPipelineTrigger
from airflow.triggers.base import TriggerEvent

HOOK_STATUS_STR_PYTHON = "airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.start_python_pipeline_async"
HOOK_STATUS_STR_JAVA = "airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.start_java_pipeline_async"
CLASSPATH_PYTHON = "airflow.providers.apache.beam.triggers.beam.BeamPythonPipelineTrigger"
CLASSPATH_JAVA = "airflow.providers.apache.beam.triggers.beam.BeamJavaPipelineTrigger"

TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_VARIABLES = {"output": "gs://bucket_test/output", "labels": {"airflow-version": "v2-7-0-dev0"}}
TEST_PY_FILE = "apache_beam.examples.wordcount"
TEST_PY_OPTIONS: list[str] = []
TEST_PY_INTERPRETER = "python3"
TEST_PY_REQUIREMENTS = ["apache-beam[gcp]==2.46.0"]
TEST_PY_PACKAGES = False
TEST_RUNNER = "DirectRunner"
TEST_JAR_FILE = "example.jar"
TEST_GCS_JAR_FILE = "gs://my-bucket/example/test.jar"
TEST_GCS_PY_FILE = "gs://my-bucket/my-object.py"
TEST_JOB_CLASS = "TestClass"


@pytest.fixture
def python_trigger():
    return BeamPythonPipelineTrigger(
        variables=TEST_VARIABLES,
        py_file=TEST_PY_FILE,
        py_options=TEST_PY_OPTIONS,
        py_interpreter=TEST_PY_INTERPRETER,
        py_requirements=TEST_PY_REQUIREMENTS,
        py_system_site_packages=TEST_PY_PACKAGES,
        runner=TEST_RUNNER,
        gcp_conn_id=TEST_GCP_CONN_ID,
    )


@pytest.fixture
def java_trigger():
    return BeamJavaPipelineTrigger(
        variables=TEST_VARIABLES,
        jar=TEST_JAR_FILE,
        job_class=TEST_JOB_CLASS,
        runner=TEST_RUNNER,
        gcp_conn_id=TEST_GCP_CONN_ID,
    )


class TestBeamPythonPipelineTrigger:
    def test_beam_trigger_serialization_should_execute_successfully(self, python_trigger):
        """
        Asserts that the BeamPythonPipelineTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = python_trigger.serialize()
        assert classpath == CLASSPATH_PYTHON
        assert kwargs == {
            "variables": TEST_VARIABLES,
            "py_file": TEST_PY_FILE,
            "py_options": TEST_PY_OPTIONS,
            "py_interpreter": TEST_PY_INTERPRETER,
            "py_requirements": TEST_PY_REQUIREMENTS,
            "py_system_site_packages": TEST_PY_PACKAGES,
            "runner": TEST_RUNNER,
            "gcp_conn_id": TEST_GCP_CONN_ID,
        }

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR_PYTHON)
    async def test_beam_trigger_on_success_should_execute_successfully(
        self, mock_pipeline_status, python_trigger
    ):
        """
        Tests the BeamPythonPipelineTrigger only fires once the job execution reaches a successful state.
        """
        mock_pipeline_status.return_value = 0
        generator = python_trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "status": "success",
                    "message": "Pipeline has finished SUCCESSFULLY",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR_PYTHON)
    async def test_beam_trigger_error_should_execute_successfully(self, mock_pipeline_status, python_trigger):
        """
        Test that BeamPythonPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.return_value = 1

        generator = python_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Operation failed"}) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR_PYTHON)
    async def test_beam_trigger_exception_should_execute_successfully(
        self, mock_pipeline_status, python_trigger
    ):
        """
        Test that BeamPythonPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.side_effect = Exception("Test exception")

        generator = python_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    async def test_beam_trigger_gcs_provide_file_should_execute_successfully(
        self, python_trigger, monkeypatch
    ):
        """
        Test that BeamPythonPipelineTrigger downloads GCS provide file correctly with GCSAsyncHook.
        """
        TEST_GCS_PY_FILE = "gs://bucket/path/file.py"
        python_trigger.py_file = TEST_GCS_PY_FILE

        with mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSAsyncHook") as MockAsyncHook:
            async_hook_instance = MockAsyncHook.return_value

            class DummyCM:
                def __enter__(self):
                    return "mocked_temp_file"

                def __exit__(self, exc_type, exc, tb):
                    return False

            sync_hook = mock.Mock(name="SyncGCSHook")
            sync_hook.provide_file.return_value = DummyCM()

            async_hook_instance.get_sync_hook = mock.AsyncMock(return_value=sync_hook)

            fake_loop = mock.Mock()
            fake_loop.run_in_executor = mock.AsyncMock(return_value="mocked_temp_file")
            monkeypatch.setattr(asyncio, "get_running_loop", lambda: fake_loop)

            gen = python_trigger.run()
            await gen.asend(None)

            MockAsyncHook.assert_called_once_with(gcp_conn_id=python_trigger.gcp_conn_id)
            async_hook_instance.get_sync_hook.assert_awaited_once()
            sync_hook.provide_file.assert_called_once_with(object_url=TEST_GCS_PY_FILE)
            fake_loop.run_in_executor.assert_awaited_once()


class TestBeamJavaPipelineTrigger:
    def test_beam_trigger_serialization_should_execute_successfully(self, java_trigger):
        """
        Asserts that the BeamJavaPipelineTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = java_trigger.serialize()
        assert classpath == CLASSPATH_JAVA
        assert kwargs == {
            "variables": TEST_VARIABLES,
            "jar": TEST_JAR_FILE,
            "job_class": TEST_JOB_CLASS,
            "runner": TEST_RUNNER,
            "gcp_conn_id": TEST_GCP_CONN_ID,
        }

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR_JAVA)
    async def test_beam_trigger_on_success_should_execute_successfully(
        self, mock_pipeline_status, java_trigger
    ):
        """
        Tests the BeamJavaPipelineTrigger only fires once the job execution reaches a successful state.
        """
        mock_pipeline_status.return_value = 0
        generator = java_trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "status": "success",
                    "message": "Pipeline has finished SUCCESSFULLY",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR_JAVA)
    async def test_beam_trigger_error_should_execute_successfully(self, mock_pipeline_status, java_trigger):
        """
        Test that BeamJavaPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.return_value = 1

        generator = java_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Operation failed"}) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR_JAVA)
    async def test_beam_trigger_exception_should_execute_successfully(
        self, mock_pipeline_status, java_trigger
    ):
        """
        Test that BeamJavaPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.side_effect = Exception("Test exception")

        generator = java_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    async def test_beam_trigger_gcs_provide_file_should_execute_successfully(self, java_trigger, monkeypatch):
        """
        Test that BeamJavaPipelineTrigger downloads GCS provide file correctly with GCSAsyncHook.
        """
        java_trigger.jar = TEST_GCS_JAR_FILE

        with mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSAsyncHook") as MockAsyncHook:
            async_hook_instance = MockAsyncHook.return_value

            class DummyCM:
                def __enter__(self):
                    return "mocked_temp_file"

                def __exit__(self, exc_type, exc, tb):
                    return False

            sync_hook = mock.Mock(name="SyncGCSHook")
            sync_hook.provide_file.return_value = DummyCM()

            async_hook_instance.get_sync_hook = mock.AsyncMock(return_value=sync_hook)

            fake_loop = mock.Mock()
            fake_loop.run_in_executor = mock.AsyncMock(return_value="mocked_temp_file")
            monkeypatch.setattr(asyncio, "get_running_loop", lambda: fake_loop)

            gen = java_trigger.run()
            await gen.asend(None)

            MockAsyncHook.assert_called_once_with(gcp_conn_id=java_trigger.gcp_conn_id)
            async_hook_instance.get_sync_hook.assert_awaited_once()
            sync_hook.provide_file.assert_called_once_with(object_url=TEST_GCS_JAR_FILE)
            fake_loop.run_in_executor.assert_awaited_once()
