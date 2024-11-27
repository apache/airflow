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

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.dataform_v1beta1 import DataformClient
from google.cloud.dataform_v1beta1.types import (
    CompilationResult,
    InstallNpmPackagesResponse,
    Repository,
    WorkflowInvocation,
    Workspace,
    WriteFileResponse,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.dataform_v1beta1.services.dataform.pagers import QueryWorkflowInvocationActionsPager


class DataformHook(GoogleBaseHook):
    """Hook for Google Cloud DataForm APIs."""

    def get_dataform_client(self) -> DataformClient:
        """Retrieve client library object that allow access to Cloud Dataform service."""
        return DataformClient(credentials=self.get_credentials())

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_workflow_invocation(
        self,
        workflow_invocation_id: str,
        repository_id: str,
        project_id: str,
        region: str,
        wait_time: int = 10,
        timeout: int | None = None,
    ) -> None:
        """
        Poll a job to check if it finishes.

        :param workflow_invocation_id: Id of the Workflow Invocation
        :param repository_id: Id of the Dataform repository
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param wait_time: Number of seconds between checks
        :param timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
        """
        if region is None:
            raise TypeError("missing 1 required keyword argument: 'region'")
        state = None
        start = time.monotonic()
        while state not in (
            WorkflowInvocation.State.FAILED,
            WorkflowInvocation.State.SUCCEEDED,
            WorkflowInvocation.State.CANCELLED,
        ):
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(
                    f"Timeout: workflow invocation {workflow_invocation_id} is not ready after {timeout}s"
                )
            time.sleep(wait_time)
            try:
                workflow_invocation = self.get_workflow_invocation(
                    project_id=project_id,
                    region=region,
                    repository_id=repository_id,
                    workflow_invocation_id=workflow_invocation_id,
                )
                state = workflow_invocation.state
            except Exception as err:
                self.log.info(
                    "Retrying. Dataform API returned error when waiting for workflow invocation: %s", err
                )

        if state == WorkflowInvocation.State.FAILED:
            raise AirflowException(f"Workflow Invocation failed:\n{workflow_invocation}")
        if state == WorkflowInvocation.State.CANCELLED:
            raise AirflowException(f"Workflow Invocation was cancelled:\n{workflow_invocation}")

    @GoogleBaseHook.fallback_to_default_project_id
    def create_compilation_result(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        compilation_result: CompilationResult | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> CompilationResult:
        """
        Create a new CompilationResult in a given project and location.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
        :param compilation_result:  Required. The compilation result to create.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
        return client.create_compilation_result(
            request={
                "parent": parent,
                "compilation_result": compilation_result,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_compilation_result(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        compilation_result_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> CompilationResult:
        """
        Fetch a single CompilationResult.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
        :param compilation_result_id:  The Id of the Dataform Compilation Result
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        name = (
            f"projects/{project_id}/locations/{region}/repositories/"
            f"{repository_id}/compilationResults/{compilation_result_id}"
        )
        return client.get_compilation_result(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_workflow_invocation(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation: WorkflowInvocation | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> WorkflowInvocation:
        """
        Create a new WorkflowInvocation in a given Repository.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
        :param workflow_invocation:  Required. The workflow invocation resource to create.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
        return client.create_workflow_invocation(
            request={"parent": parent, "workflow_invocation": workflow_invocation},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_workflow_invocation(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> WorkflowInvocation:
        """
        Fetch a single WorkflowInvocation.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
        :param workflow_invocation_id:  Required. The workflow invocation resource's id.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        name = (
            f"projects/{project_id}/locations/{region}/repositories/"
            f"{repository_id}/workflowInvocations/{workflow_invocation_id}"
        )
        return client.get_workflow_invocation(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def query_workflow_invocation_actions(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> QueryWorkflowInvocationActionsPager:
        """
        Fetch WorkflowInvocation actions.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
        :param workflow_invocation_id:  Required. The workflow invocation resource's id.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        name = (
            f"projects/{project_id}/locations/{region}/repositories/"
            f"{repository_id}/workflowInvocations/{workflow_invocation_id}"
        )
        response = client.query_workflow_invocation_actions(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_workflow_invocation(
        self,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Request cancellation of a running WorkflowInvocation.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
        :param workflow_invocation_id:  Required. The workflow invocation resource's id.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        name = (
            f"projects/{project_id}/locations/{region}/repositories/"
            f"{repository_id}/workflowInvocations/{workflow_invocation_id}"
        )
        try:
            workflow_invocation = self.get_workflow_invocation(
                project_id=project_id,
                region=region,
                repository_id=repository_id,
                workflow_invocation_id=workflow_invocation_id,
            )
            state = workflow_invocation.state
        except Exception as err:
            raise AirflowException(
                f"Dataform API returned error when waiting for workflow invocation:\n{err}"
            )

        if state == WorkflowInvocation.State.RUNNING:
            client.cancel_workflow_invocation(
                request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
            )
        else:
            self.log.info(
                "Workflow is not active. Either the execution has already finished or has been canceled. "
                "Please check the logs above for more details."
            )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_repository(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Repository:
        """
        Create repository.

        :param project_id: Required. The ID of the Google Cloud project where repository should be.
        :param region: Required. The ID of the Google Cloud region where repository should be.
        :param repository_id: Required. The ID of the new Dataform repository.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        parent = f"projects/{project_id}/locations/{region}"
        request = {
            "parent": parent,
            "repository_id": repository_id,
        }

        repository = client.create_repository(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return repository

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_repository(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        force: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete repository.

        :param project_id: Required. The ID of the Google Cloud project where repository located.
        :param region: Required. The ID of the Google Cloud region where repository located.
        :param repository_id: Required. The ID of the Dataform repository that should be deleted.
        :param force: If set to true, any child resources of this repository will also be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        name = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
        request = {
            "name": name,
            "force": force,
        }

        client.delete_repository(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_workspace(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Workspace:
        """
        Create workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace should be.
        :param region: Required. The ID of the Google Cloud region where workspace should be.
        :param repository_id: Required. The ID of the Dataform repository where workspace should be.
        :param workspace_id: Required. The ID of the new Dataform workspace.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"

        request = {"parent": parent, "workspace_id": workspace_id}

        workspace = client.create_workspace(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return workspace

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_workspace(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Delete workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace located.
        :param region: Required. The ID of the Google Cloud region where workspace located.
        :param repository_id: Required. The ID of the Dataform repository where workspace located.
        :param workspace_id: Required. The ID of the Dataform workspace that should be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        workspace_path = (
            f"projects/{project_id}/locations/{region}/"
            f"repositories/{repository_id}/workspaces/{workspace_id}"
        )
        request = {
            "name": workspace_path,
        }

        client.delete_workspace(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def write_file(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        filepath: str,
        contents: bytes,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> WriteFileResponse:
        """
        Write a new file to the specified workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace located.
        :param region: Required. The ID of the Google Cloud region where workspace located.
        :param repository_id: Required. The ID of the Dataform repository where workspace located.
        :param workspace_id: Required. The ID of the Dataform workspace where files should be created.
        :param filepath: Required. Path to file including name of the file relative to workspace root.
        :param contents: Required. Content of the file to be written.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        workspace_path = (
            f"projects/{project_id}/locations/{region}/"
            f"repositories/{repository_id}/workspaces/{workspace_id}"
        )
        request = {
            "workspace": workspace_path,
            "path": filepath,
            "contents": contents,
        }

        response = client.write_file(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def make_directory(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        path: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> dict:
        """
        Make new directory in specified workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace located.
        :param region: Required. The ID of the Google Cloud region where workspace located.
        :param repository_id: Required. The ID of the Dataform repository where workspace located.
        :param workspace_id: Required. The ID of the Dataform workspace where directory should be created.
        :param path: Required. The directory's full path including new directory name,
            relative to the workspace root.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        workspace_path = (
            f"projects/{project_id}/locations/{region}/"
            f"repositories/{repository_id}/workspaces/{workspace_id}"
        )
        request = {
            "workspace": workspace_path,
            "path": path,
        }

        response = client.make_directory(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def remove_directory(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        path: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Remove directory in specified workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace located.
        :param region: Required. The ID of the Google Cloud region where workspace located.
        :param repository_id: Required. The ID of the Dataform repository where workspace located.
        :param workspace_id: Required. The ID of the Dataform workspace where directory located.
        :param path: Required. The directory's full path including directory name,
            relative to the workspace root.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        workspace_path = (
            f"projects/{project_id}/locations/{region}/"
            f"repositories/{repository_id}/workspaces/{workspace_id}"
        )
        request = {
            "workspace": workspace_path,
            "path": path,
        }

        client.remove_directory(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def remove_file(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        filepath: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Remove file in specified workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace located.
        :param region: Required. The ID of the Google Cloud region where workspace located.
        :param repository_id: Required. The ID of the Dataform repository where workspace located.
        :param workspace_id: Required. The ID of the Dataform workspace where directory located.
        :param filepath: Required. The full path including name of the file, relative to the workspace root.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        workspace_path = (
            f"projects/{project_id}/locations/{region}/"
            f"repositories/{repository_id}/workspaces/{workspace_id}"
        )
        request = {
            "workspace": workspace_path,
            "path": filepath,
        }

        client.remove_file(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def install_npm_packages(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workspace_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> InstallNpmPackagesResponse:
        """
        Install NPM dependencies in the provided workspace.

        Requires "package.json" to be created in the workspace.

        :param project_id: Required. The ID of the Google Cloud project where workspace located.
        :param region: Required. The ID of the Google Cloud region where workspace located.
        :param repository_id: Required. The ID of the Dataform repository where workspace located.
        :param workspace_id: Required. The ID of the Dataform workspace.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataform_client()
        workspace_path = (
            f"projects/{project_id}/locations/{region}/"
            f"repositories/{repository_id}/workspaces/{workspace_id}"
        )
        request = {
            "workspace": workspace_path,
        }

        response = client.install_npm_packages(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return response
