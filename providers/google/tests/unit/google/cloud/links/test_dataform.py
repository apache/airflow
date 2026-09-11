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

"""Tests for Dataform links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.dataform import (
    DATAFORM_REPOSITORY_LINK,
    DATAFORM_WORKFLOW_INVOCATION_LINK,
    DATAFORM_WORKSPACE_LINK,
    DataformRepositoryLink,
    DataformWorkflowInvocationLink,
    DataformWorkspaceLink,
)

TEST_PROJECT_ID = "test-project-id"
TEST_REGION = "test-region"
TEST_REPOSITORY_ID = "test-repository-id"
TEST_WORKFLOW_INVOCATION_ID = "test-workflow-invocation-id"
TEST_WORKSPACE_ID = "test-workspace-id"


class TestDataformWorkflowInvocationLink:
    def test_class_attributes(self):
        assert DataformWorkflowInvocationLink.key == "dataform_workflow_invocation_config"
        assert DataformWorkflowInvocationLink.name == "Dataform Workflow Invocation"
        assert DataformWorkflowInvocationLink.format_str == DATAFORM_WORKFLOW_INVOCATION_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataformWorkflowInvocationLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataform_workflow_invocation_config",
            value={
                "project_id": TEST_PROJECT_ID,
                "region": TEST_REGION,
                "repository_id": TEST_REPOSITORY_ID,
                "workflow_invocation_id": TEST_WORKFLOW_INVOCATION_ID,
            },
        )

    def test_format_link(self):
        link = DataformWorkflowInvocationLink()

        result = link._format_link(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
        )

        assert result == BASE_LINK + DATAFORM_WORKFLOW_INVOCATION_LINK.format(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
        )


class TestDataformRepositoryLink:
    def test_class_attributes(self):
        assert DataformRepositoryLink.key == "dataform_repository"
        assert DataformRepositoryLink.name == "Dataform Repository"
        assert DataformRepositoryLink.format_str == DATAFORM_REPOSITORY_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataformRepositoryLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataform_repository",
            value={"project_id": TEST_PROJECT_ID, "region": TEST_REGION, "repository_id": TEST_REPOSITORY_ID},
        )

    def test_format_link(self):
        link = DataformRepositoryLink()

        result = link._format_link(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, repository_id=TEST_REPOSITORY_ID
        )

        assert result == BASE_LINK + DATAFORM_REPOSITORY_LINK.format(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, repository_id=TEST_REPOSITORY_ID
        )


class TestDataformWorkspaceLink:
    def test_class_attributes(self):
        assert DataformWorkspaceLink.key == "dataform_workspace"
        assert DataformWorkspaceLink.name == "Dataform Workspace"
        assert DataformWorkspaceLink.format_str == DATAFORM_WORKSPACE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataformWorkspaceLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workspace_id=TEST_WORKSPACE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataform_workspace",
            value={
                "project_id": TEST_PROJECT_ID,
                "region": TEST_REGION,
                "repository_id": TEST_REPOSITORY_ID,
                "workspace_id": TEST_WORKSPACE_ID,
            },
        )

    def test_format_link(self):
        link = DataformWorkspaceLink()

        result = link._format_link(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workspace_id=TEST_WORKSPACE_ID,
        )

        assert result == BASE_LINK + DATAFORM_WORKSPACE_LINK.format(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workspace_id=TEST_WORKSPACE_ID,
        )
