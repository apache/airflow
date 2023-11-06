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

from airflow.providers.apache.beam.triggers.beam import BeamPipelineTrigger
from airflow.triggers.base import TriggerEvent

HOOK_STATUS_STR = "airflow.providers.apache.beam.hooks.beam.BeamAsyncHook.start_python_pipeline_async"
CLASSPATH = "airflow.providers.apache.beam.triggers.beam.BeamPipelineTrigger"

TASK_ID = "test_task"
LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
PROJECT_ID = "test_project_id"
TEST_VARIABLES = {"output": "gs://bucket_test/output", "labels": {"airflow-version": "v2-7-0-dev0"}}
TEST_PY_FILE = "apache_beam.examples.wordcount"
TEST_PY_OPTIONS: list[str] = []
TEST_PY_INTERPRETER = "python3"
TEST_PY_REQUIREMENTS = ["apache-beam[gcp]==2.46.0"]
TEST_PY_PACKAGES = False
TEST_RUNNER = "DirectRunner"


@pytest.fixture
def trigger():
    return BeamPipelineTrigger(
        variables=TEST_VARIABLES,
        py_file=TEST_PY_FILE,
        py_options=TEST_PY_OPTIONS,
        py_interpreter=TEST_PY_INTERPRETER,
        py_requirements=TEST_PY_REQUIREMENTS,
        py_system_site_packages=TEST_PY_PACKAGES,
        runner=TEST_RUNNER,
    )


class TestBeamPipelineTrigger:
    def test_beam_trigger_serialization_should_execute_successfully(self, trigger):
        """
        Asserts that the BeamPipelineTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = trigger.serialize()
        assert classpath == CLASSPATH
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
    @mock.patch(HOOK_STATUS_STR)
    async def test_beam_trigger_on_success_should_execute_successfully(self, mock_pipeline_status, trigger):
        """
        Tests the BeamPipelineTrigger only fires once the job execution reaches a successful state.
        """
        mock_pipeline_status.return_value = 0
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "Pipeline has finished SUCCESSFULLY"}) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR)
    async def test_beam_trigger_error_should_execute_successfully(self, mock_pipeline_status, trigger):
        """
        Test that BeamPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.return_value = 1

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Operation failed"}) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR)
    async def test_beam_trigger_exception_should_execute_successfully(self, mock_pipeline_status, trigger):
        """
        Test that BeamPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.side_effect = Exception("Test exception")

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual
