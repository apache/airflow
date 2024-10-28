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

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateRepositoryOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformCreateWorkspaceOperator,
    DataformDeleteRepositoryOperator,
    DataformDeleteWorkspaceOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
    DataformInstallNpmPackagesOperator,
    DataformMakeDirectoryOperator,
    DataformQueryWorkflowInvocationActionsOperator,
    DataformRemoveDirectoryOperator,
    DataformRemoveFileOperator,
    DataformWriteFileOperator,
)

HOOK_STR = "airflow.providers.google.cloud.operators.dataform.DataformHook"
WORKFLOW_INVOCATION_STR = (
    "airflow.providers.google.cloud.operators.dataform.WorkflowInvocation"
)
WORKFLOW_INVOCATION_ACTION_STR = (
    "airflow.providers.google.cloud.operators.dataform.WorkflowInvocationAction"
)
COMPILATION_RESULT_STR = (
    "airflow.providers.google.cloud.operators.dataform.CompilationResult"
)
REPOSITORY_STR = "airflow.providers.google.cloud.operators.dataform.Repository"
WORKSPACE_STR = "airflow.providers.google.cloud.operators.dataform.Workspace"
WRITE_FILE_RESPONSE_STR = (
    "airflow.providers.google.cloud.operators.dataform.WriteFileResponse"
)
MAKE_DIRECTORY_RESPONSE_STR = (
    "airflow.providers.google.cloud.operators.dataform.MakeDirectoryResponse"
)
INSTALL_NPM_PACKAGES_RESPONSE_STR = (
    "airflow.providers.google.cloud.operators.dataform.InstallNpmPackagesResponse"
)

PROJECT_ID = "project-id"
REGION = "region"
REPOSITORY_ID = "test_repository_id"
WORKSPACE_ID = "test_workspace_id"
WORKSPACE = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"
COMPILATION_RESULT_ID = "test_compilation_result_id"
GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
FILEPATH = "path/to/file.txt"
FILE_CONTENT = b"test content"
DIRECTORY_PATH = "path/to/directory"

WORKFLOW_INVOCATION = {
    "compilation_result": (
        f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
        f"compilationResults/{COMPILATION_RESULT_ID}"
    )
}
WORKFLOW_INVOCATION_ID = "test_workflow_invocation"


class TestDataformCreateCompilationResult:
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


class TestDataformGetCompilationResultOperator:
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


class TestDataformCreateWorkflowInvocationOperator:
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


class TestDataformGetWorkflowInvocationOperator:
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


class TestDataformQueryWorkflowInvocationActionsOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(WORKFLOW_INVOCATION_ACTION_STR)
    def test_execute(self, workflow_invocation_action_str, hook_mock):
        op = DataformQueryWorkflowInvocationActionsOperator(
            task_id="query_workflow_invocation_action",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
        )

        workflow_invocation_action_str.return_value.to_dict.return_value = None
        op.execute(context=mock.MagicMock())

        hook_mock.return_value.query_workflow_invocation_actions.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformCancelWorkflowInvocationOperator:
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


class TestDataformCreateRepositoryOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(REPOSITORY_STR)
    def test_execute(self, _, hook_mock):
        op = DataformCreateRepositoryOperator(
            task_id="create-repository",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
        )
        op.execute(context=mock.MagicMock())
        hook_mock.return_value.create_repository.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformDeleteRepositoryOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        force = True
        op = DataformDeleteRepositoryOperator(
            task_id="delete-repository",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            force=force,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.delete_repository.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            force=force,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformCreateWorkspaceOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(WORKSPACE_STR)
    def test_execute(self, _, hook_mock):
        op = DataformCreateWorkspaceOperator(
            task_id="create-workspace",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.create_workspace.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformDeleteWorkspaceOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataformDeleteWorkspaceOperator(
            task_id="delete-workspace",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.delete_workspace.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformWriteFileOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(WRITE_FILE_RESPONSE_STR)
    def test_execute(self, _, hook_mock):
        op = DataformWriteFileOperator(
            task_id="write-file",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            filepath=FILEPATH,
            contents=FILE_CONTENT,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.write_file.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            filepath=FILEPATH,
            contents=FILE_CONTENT,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformMakeDirectoryOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(MAKE_DIRECTORY_RESPONSE_STR)
    def test_execute(self, _, hook_mock):
        op = DataformMakeDirectoryOperator(
            task_id="write-file",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            directory_path=DIRECTORY_PATH,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.make_directory.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            path=DIRECTORY_PATH,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformRemoveFileOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataformRemoveFileOperator(
            task_id="remove-file",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            filepath=FILEPATH,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.remove_file.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            filepath=FILEPATH,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformRemoveDirectoryOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, hook_mock):
        op = DataformRemoveDirectoryOperator(
            task_id="remove-directory",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            directory_path=DIRECTORY_PATH,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.remove_directory.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            path=DIRECTORY_PATH,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestDataformInstallNpmPackagesOperator:
    @mock.patch(HOOK_STR)
    @mock.patch(INSTALL_NPM_PACKAGES_RESPONSE_STR)
    def test_execute(self, _, hook_mock):
        op = DataformInstallNpmPackagesOperator(
            task_id="remove-directory",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
        )

        op.execute(context=mock.MagicMock())
        hook_mock.return_value.install_npm_packages.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
