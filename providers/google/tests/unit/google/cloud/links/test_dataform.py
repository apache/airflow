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

from airflow.providers.google.cloud.links.dataform import (
    DataformRepositoryLink,
    DataformWorkflowInvocationLink,
    DataformWorkspaceLink,
)

EXPECTED_DATAFORM_WORKFLOW_INVOCATION_LINK_NAME = "Dataform Workflow Invocation"
EXPECTED_DATAFORM_WORKFLOW_INVOCATION_LINK_KEY = "dataform_workflow_invocation_config"
EXPECTED_DATAFORM_WORKFLOW_INVOCATION_LINK_FORMAT_STR = "/bigquery/dataform/locations/{region}/repositories/{repository_id}/workflows/{workflow_invocation_id}?project={project_id}"
EXPECTED_DATAFORM_REPOSITORY_LINK_NAME = "Dataform Repository"
EXPECTED_DATAFORM_REPOSITORY_LINK_KEY = "dataform_repository"
EXPECTED_DATAFORM_REPOSITORY_LINK_FORMAT_STR = "/bigquery/dataform/locations/{region}/repositories/{repository_id}/details/workspaces?project={project_id}"
EXPECTED_DATAFORM_WORKSPACE_LINK_NAME = "Dataform Workspace"
EXPECTED_DATAFORM_WORKSPACE_LINK_KEY = "dataform_workspace"
EXPECTED_DATAFORM_WORKSPACE_LINK_FORMAT_STR = "/bigquery/dataform/locations/{region}/repositories/{repository_id}/workspaces/{workspace_id}/files/?project={project_id}"


class TestDataformWorkflowInvocationLink:
    def test_class_attributes(self):
        assert DataformWorkflowInvocationLink.name == EXPECTED_DATAFORM_WORKFLOW_INVOCATION_LINK_NAME
        assert DataformWorkflowInvocationLink.key == EXPECTED_DATAFORM_WORKFLOW_INVOCATION_LINK_KEY
        assert (
            DataformWorkflowInvocationLink.format_str == EXPECTED_DATAFORM_WORKFLOW_INVOCATION_LINK_FORMAT_STR
        )


class TestDataformRepositoryLink:
    def test_class_attributes(self):
        assert DataformRepositoryLink.name == EXPECTED_DATAFORM_REPOSITORY_LINK_NAME
        assert DataformRepositoryLink.key == EXPECTED_DATAFORM_REPOSITORY_LINK_KEY
        assert DataformRepositoryLink.format_str == EXPECTED_DATAFORM_REPOSITORY_LINK_FORMAT_STR


class TestDataformWorkspaceLink:
    def test_class_attributes(self):
        assert DataformWorkspaceLink.name == EXPECTED_DATAFORM_WORKSPACE_LINK_NAME
        assert DataformWorkspaceLink.key == EXPECTED_DATAFORM_WORKSPACE_LINK_KEY
        assert DataformWorkspaceLink.format_str == EXPECTED_DATAFORM_WORKSPACE_LINK_FORMAT_STR
