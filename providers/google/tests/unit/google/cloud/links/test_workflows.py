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

"""Tests for Workflows links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.workflows import (
    EXECUTION_LINK,
    WORKFLOW_LINK,
    WORKFLOWS_LINK,
    WorkflowsExecutionLink,
    WorkflowsListOfWorkflowsLink,
    WorkflowsWorkflowDetailsLink,
)

TEST_EXECUTION_ID = "test-execution-id"
TEST_LOCATION_ID = "test-location-id"
TEST_PROJECT_ID = "test-project-id"
TEST_WORKFLOW_ID = "test-workflow-id"


class TestWorkflowsWorkflowDetailsLink:
    def test_class_attributes(self):
        assert WorkflowsWorkflowDetailsLink.key == "workflow_details"
        assert WorkflowsWorkflowDetailsLink.name == "Workflow details"
        assert WorkflowsWorkflowDetailsLink.format_str == WORKFLOW_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        WorkflowsWorkflowDetailsLink.persist(
            context=mock_context,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
            workflow_id=TEST_WORKFLOW_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="workflow_details",
            value={
                "location_id": TEST_LOCATION_ID,
                "project_id": TEST_PROJECT_ID,
                "workflow_id": TEST_WORKFLOW_ID,
            },
        )

    def test_format_link(self):
        link = WorkflowsWorkflowDetailsLink()

        result = link._format_link(
            location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID, workflow_id=TEST_WORKFLOW_ID
        )

        assert result == BASE_LINK + WORKFLOW_LINK.format(
            location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID, workflow_id=TEST_WORKFLOW_ID
        )


class TestWorkflowsListOfWorkflowsLink:
    def test_class_attributes(self):
        assert WorkflowsListOfWorkflowsLink.key == "list_of_workflows"
        assert WorkflowsListOfWorkflowsLink.name == "List of workflows"
        assert WorkflowsListOfWorkflowsLink.format_str == WORKFLOWS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        WorkflowsListOfWorkflowsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="list_of_workflows",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = WorkflowsListOfWorkflowsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + WORKFLOWS_LINK.format(project_id=TEST_PROJECT_ID)


class TestWorkflowsExecutionLink:
    def test_class_attributes(self):
        assert WorkflowsExecutionLink.key == "workflow_execution"
        assert WorkflowsExecutionLink.name == "Workflow Execution"
        assert WorkflowsExecutionLink.format_str == EXECUTION_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        WorkflowsExecutionLink.persist(
            context=mock_context,
            execution_id=TEST_EXECUTION_ID,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
            workflow_id=TEST_WORKFLOW_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="workflow_execution",
            value={
                "execution_id": TEST_EXECUTION_ID,
                "location_id": TEST_LOCATION_ID,
                "project_id": TEST_PROJECT_ID,
                "workflow_id": TEST_WORKFLOW_ID,
            },
        )

    def test_format_link(self):
        link = WorkflowsExecutionLink()

        result = link._format_link(
            execution_id=TEST_EXECUTION_ID,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
            workflow_id=TEST_WORKFLOW_ID,
        )

        assert result == BASE_LINK + EXECUTION_LINK.format(
            execution_id=TEST_EXECUTION_ID,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
            workflow_id=TEST_WORKFLOW_ID,
        )
