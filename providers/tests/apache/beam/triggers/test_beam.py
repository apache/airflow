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

from unittest import mock

import pytest

from airflow.providers.apache.beam.triggers.beam import BeamJavaPipelineTrigger, BeamPythonPipelineTrigger
from airflow.triggers.base import TriggerEvent

HOOK_STATUS_STR_PYTHON = "airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.start_python_pipeline_async"
HOOK_STATUS_STR_JAVA = "airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.start_java_pipeline_async"
CLASSPATH_PYTHON = "airflow.providers.apache.beam.triggers.beam.BeamPythonPipelineTrigger"
CLASSPATH_JAVA = "airflow.providers.apache.beam.triggers.beam.BeamJavaPipelineTrigger"

TASK_ID = "test_task"
LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
PROJECT_ID = "test_project_id"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_IMPERSONATION_CHAIN = "A,B,C"
TEST_VARIABLES = {"output": "gs://bucket_test/output", "labels": {"airflow-version": "v2-7-0-dev0"}}
TEST_PY_FILE = "apache_beam.examples.wordcount"
TEST_PY_OPTIONS: list[str] = []
TEST_PY_INTERPRETER = "python3"
TEST_PY_REQUIREMENTS = ["apache-beam[gcp]==2.46.0"]
TEST_PY_PACKAGES = False
TEST_RUNNER = "DirectRunner"
TEST_JAR_FILE = "example.jar"
TEST_GCS_JAR_FILE = "gs://my-bucket/example/test.jar"
TEST_JOB_CLASS = "TestClass"
TEST_CHECK_IF_RUNNING = False
TEST_JOB_NAME = "test_job_name"
TEST_POLL_SLEEP = 10
TEST_CANCEL_TIMEOUT = 300


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
    )


@pytest.fixture
def java_trigger():
    return BeamJavaPipelineTrigger(
        variables=TEST_VARIABLES,
        jar=TEST_JAR_FILE,
        job_class=TEST_JOB_CLASS,
        runner=TEST_RUNNER,
        check_if_running=TEST_CHECK_IF_RUNNING,
        project_id=PROJECT_ID,
        location=LOCATION,
        job_name=TEST_JOB_NAME,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
        poll_sleep=TEST_POLL_SLEEP,
        cancel_timeout=TEST_CANCEL_TIMEOUT,
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
        assert TriggerEvent({"status": "success", "message": "Pipeline has finished SUCCESSFULLY"}) == actual

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
            "check_if_running": TEST_CHECK_IF_RUNNING,
            "project_id": PROJECT_ID,
            "location": LOCATION,
            "job_name": TEST_JOB_NAME,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "poll_sleep": TEST_POLL_SLEEP,
            "cancel_timeout": TEST_CANCEL_TIMEOUT,
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
        assert TriggerEvent({"status": "success", "message": "Pipeline has finished SUCCESSFULLY"}) == actual

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
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.list_jobs")
    async def test_beam_trigger_exception_list_jobs_should_execute_successfully(
        self, mock_list_jobs, java_trigger
    ):
        """
        Test that BeamJavaPipelineTrigger fires the correct event in case of an error.
        """
        mock_list_jobs.side_effect = Exception("Test exception")

        java_trigger.check_if_running = True
        generator = java_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.beam.triggers.beam.GCSHook")
    async def test_beam_trigger_gcs_provide_file_should_execute_successfully(self, gcs_hook, java_trigger):
        """
        Test that BeamJavaPipelineTrigger downloads GCS provide file correct.
        """
        gcs_provide_file = gcs_hook.return_value.provide_file
        java_trigger.jar = TEST_GCS_JAR_FILE
        generator = java_trigger.run()
        await generator.asend(None)
        gcs_provide_file.assert_called_once_with(object_url=TEST_GCS_JAR_FILE)
