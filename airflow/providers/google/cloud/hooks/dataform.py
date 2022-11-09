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
from typing import Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.dataform_v1beta1 import DataformClient
from google.cloud.dataform_v1beta1.types import CompilationResult, WorkflowInvocation

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class DataformHook(GoogleBaseHook):
    """Hook for Google Cloud DataForm APIs."""

    def get_dataform_client(self) -> DataformClient:
        """Retrieves client library object that allow access to Cloud Dataform service."""
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
        Helper method which polls a job to check if it finishes.

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
        Creates a new CompilationResult in a given project and location.

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
        Fetches a single CompilationResult.

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
        Creates a new WorkflowInvocation in a given Repository.

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
        Fetches a single WorkflowInvocation.

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
        Requests cancellation of a running WorkflowInvocation.

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
        client.cancel_workflow_invocation(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )
