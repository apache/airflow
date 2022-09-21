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

import unittest
from unittest import TestCase, mock

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)

HOOK_STR = "airflow.providers.google.cloud.operators.dataform.DataformHook"
WORKFLOW_INVOCATION_STR = "airflow.providers.google.cloud.operators.dataform.WorkflowInvocation"
COMPILATION_RESULT_STR = "airflow.providers.google.cloud.operators.dataform.CompilationResult"

PROJECT_ID = "project-id"
REGION = "region"
REPOSITORY_ID = "test_repository_id"
WORKSPACE_ID = "test_workspace_id"
WORKSPACE = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"
COMPILATION_RESULT_ID = "test_compilation_result_id"
GCP_CONN_ID = "google_cloud_default"
DELEGATE_TO = "test-delegate-to"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

WORKFLOW_INVOCATION = {
    "compilation_result": (
        f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
        f"compilationResults/{COMPILATION_RESULT_ID}"
    )
}
WORKFLOW_INVOCATION_ID = "test_workflow_invocation"


class TestDataformCreateCompilationResult(unittest.TestCase):
    @mock.patch(HOOK_STR)
    @mock.patch(COMPILATION_RESULT_STR)
    def test_execute(self, compilation_result_mock, hook_mock):
        compilation_result = {
            "git_commitish": "main",
            "workspace": WORKSPACE,
        }
        op = DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result=compilation_result,
        )
        compilation_result_mock.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())
        hook_mock.return_value.create_compilation_result.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result=compilation_result,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformGetCompilationResultOperator(TestCase):
    @mock.patch(HOOK_STR)
    @mock.patch(COMPILATION_RESULT_STR)
    def test_execute(self, compilation_result_mock, hook_mock):
        op = DataformGetCompilationResultOperator(
            task_id="get_compilation_result",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result_id=COMPILATION_RESULT_ID,
        )
        compilation_result_mock.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())
        hook_mock.return_value.get_compilation_result.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result_id=COMPILATION_RESULT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformCreateWorkflowInvocationOperator(TestCase):
    @mock.patch(HOOK_STR)
    @mock.patch(WORKFLOW_INVOCATION_STR)
    def test_execute(self, workflow_invocation_str, hook_mock):
        op = DataformCreateWorkflowInvocationOperator(
            task_id="create_workflow_invocation",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation=WORKFLOW_INVOCATION,
        )
        workflow_invocation_str.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())

        hook_mock.return_value.create_workflow_invocation.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation=WORKFLOW_INVOCATION,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformGetWorkflowInvocationOperator(TestCase):
    @mock.patch(HOOK_STR)
    @mock.patch(WORKFLOW_INVOCATION_STR)
    def test_execute(self, workflow_invocation_str, hook_mock):
        op = DataformGetWorkflowInvocationOperator(
            task_id="get_workflow_invocation",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
        )

        workflow_invocation_str.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())

        hook_mock.return_value.get_workflow_invocation.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformCancelWorkflowInvocationOperator(TestCase):
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataformCancelWorkflowInvocationOperator(
            task_id="cancel_workflow_invocation",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.return_value.cancel_workflow_invocation.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
