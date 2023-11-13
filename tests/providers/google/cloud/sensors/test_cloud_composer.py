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

from airflow.exceptions import AirflowException, AirflowSkipException, TaskDeferred
from airflow.providers.google.cloud.sensors.cloud_composer import CloudComposerEnvironmentSensor
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerExecutionTrigger

TEST_PROJECT_ID = "test_project_id"
TEST_OPERATION_NAME = "test_operation_name"
TEST_REGION = "region"


class TestCloudComposerEnvironmentSensor:
    @pytest.mark.db_test
    def test_cloud_composer_existence_sensor_async(self):
        """
        Asserts that a task is deferred and a CloudComposerExecutionTrigger will be fired
        when the CloudComposerEnvironmentSensor is executed.
        """
        task = CloudComposerEnvironmentSensor(
            task_id="task_id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            operation_name=TEST_OPERATION_NAME,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context={})
        assert isinstance(
            exc.value.trigger, CloudComposerExecutionTrigger
        ), "Trigger is not a CloudComposerExecutionTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_cloud_composer_existence_sensor_async_execute_failure(self, soft_fail, expected_exception):
        """Tests that an expected exception is raised in case of error event."""
        task = CloudComposerEnvironmentSensor(
            task_id="task_id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            operation_name=TEST_OPERATION_NAME,
            soft_fail=soft_fail,
        )
        with pytest.raises(expected_exception, match="No event received in trigger callback"):
            task.execute_complete(context={}, event=None)

    def test_cloud_composer_existence_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = CloudComposerEnvironmentSensor(
            task_id="task_id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            operation_name=TEST_OPERATION_NAME,
        )
        with mock.patch.object(task.log, "info"):
            task.execute_complete(
                context={}, event={"operation_done": True, "operation_name": TEST_OPERATION_NAME}
            )
