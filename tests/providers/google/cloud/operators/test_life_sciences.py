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
"""Tests for Google Life Sciences Run Pipeline operator """

import unittest
from unittest import mock

from airflow.providers.google.cloud.operators.life_sciences import LifeSciencesRunPipelineOperator

TEST_BODY = {"pipeline": {"actions": [{}], "resources": {}, "environment": {}, "timeout": '3.5s'}}

TEST_OPERATION = {
    "name": 'operation-name',
    "metadata": {"@type": 'anytype'},
    "done": True,
    "response": "response",
}
TEST_PROJECT_ID = "life-science-project-id"
TEST_LOCATION = 'test-location'


class TestLifeSciencesRunPipelineOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.life_sciences.LifeSciencesHook")
    def test_executes(self, mock_hook):
        mock_instance = mock_hook.return_value
        mock_instance.run_pipeline.return_value = TEST_OPERATION
        operator = LifeSciencesRunPipelineOperator(
            task_id='task-id', body=TEST_BODY, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        context = mock.MagicMock()
        result = operator.execute(context=context)

        assert result == TEST_OPERATION

    @mock.patch("airflow.providers.google.cloud.operators.life_sciences.LifeSciencesHook")
    def test_executes_without_project_id(self, mock_hook):
        mock_instance = mock_hook.return_value
        mock_instance.run_pipeline.return_value = TEST_OPERATION
        operator = LifeSciencesRunPipelineOperator(
            task_id='task-id',
            body=TEST_BODY,
            location=TEST_LOCATION,
        )
        context = mock.MagicMock()
        result = operator.execute(context=context)
        assert result == TEST_OPERATION
