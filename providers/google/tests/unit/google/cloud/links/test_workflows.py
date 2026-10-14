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

from airflow.providers.google.cloud.links.workflows import (
    WorkflowsExecutionLink,
    WorkflowsListOfWorkflowsLink,
    WorkflowsWorkflowDetailsLink,
)

EXPECTED_WORKFLOWS_WORKFLOW_DETAILS_LINK_NAME = "Workflow details"
EXPECTED_WORKFLOWS_WORKFLOW_DETAILS_LINK_KEY = "workflow_details"
EXPECTED_WORKFLOWS_WORKFLOW_DETAILS_LINK_FORMAT_STR = (
    "/workflows/workflow/{location_id}/{workflow_id}/executions?project={project_id}"
)
EXPECTED_WORKFLOWS_LIST_OF_WORKFLOWS_LINK_NAME = "List of workflows"
EXPECTED_WORKFLOWS_LIST_OF_WORKFLOWS_LINK_KEY = "list_of_workflows"
EXPECTED_WORKFLOWS_LIST_OF_WORKFLOWS_LINK_FORMAT_STR = "/workflows?project={project_id}"
EXPECTED_WORKFLOWS_EXECUTION_LINK_NAME = "Workflow Execution"
EXPECTED_WORKFLOWS_EXECUTION_LINK_KEY = "workflow_execution"
EXPECTED_WORKFLOWS_EXECUTION_LINK_FORMAT_STR = (
    "/workflows/workflow/{location_id}/{workflow_id}/execution/{execution_id}?project={project_id}"
)


class TestWorkflowsWorkflowDetailsLink:
    def test_class_attributes(self):
        assert WorkflowsWorkflowDetailsLink.name == EXPECTED_WORKFLOWS_WORKFLOW_DETAILS_LINK_NAME
        assert WorkflowsWorkflowDetailsLink.key == EXPECTED_WORKFLOWS_WORKFLOW_DETAILS_LINK_KEY
        assert WorkflowsWorkflowDetailsLink.format_str == EXPECTED_WORKFLOWS_WORKFLOW_DETAILS_LINK_FORMAT_STR


class TestWorkflowsListOfWorkflowsLink:
    def test_class_attributes(self):
        assert WorkflowsListOfWorkflowsLink.name == EXPECTED_WORKFLOWS_LIST_OF_WORKFLOWS_LINK_NAME
        assert WorkflowsListOfWorkflowsLink.key == EXPECTED_WORKFLOWS_LIST_OF_WORKFLOWS_LINK_KEY
        assert WorkflowsListOfWorkflowsLink.format_str == EXPECTED_WORKFLOWS_LIST_OF_WORKFLOWS_LINK_FORMAT_STR


class TestWorkflowsExecutionLink:
    def test_class_attributes(self):
        assert WorkflowsExecutionLink.name == EXPECTED_WORKFLOWS_EXECUTION_LINK_NAME
        assert WorkflowsExecutionLink.key == EXPECTED_WORKFLOWS_EXECUTION_LINK_KEY
        assert WorkflowsExecutionLink.format_str == EXPECTED_WORKFLOWS_EXECUTION_LINK_FORMAT_STR
