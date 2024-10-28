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

from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.workflows.executions_v1beta import Execution, ExecutionsClient
from google.cloud.workflows_v1beta import Workflow, WorkflowsClient

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.workflows.executions_v1beta.services.executions.pagers import (
        ListExecutionsPager,
    )
    from google.cloud.workflows_v1beta.services.workflows.pagers import ListWorkflowsPager
    from google.protobuf.field_mask_pb2 import FieldMask


class WorkflowsHook(GoogleBaseHook):
    """
    Hook for Google GCP APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(self, **kwargs):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)

    def get_workflows_client(self) -> WorkflowsClient:
        """Return WorkflowsClient object."""
        return WorkflowsClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO
        )

    def get_executions_client(self) -> ExecutionsClient:
        """Return ExecutionsClient object."""
        return ExecutionsClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_workflow(
        self,
        workflow: dict,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a new workflow.

        If a workflow with the specified name already exists in the
        specified project and location, the long running operation will
        return [ALREADY_EXISTS][google.rpc.Code.ALREADY_EXISTS] error.

        :param workflow: Required. Workflow to be created.
        :param workflow_id: Required. The ID of the workflow to be created.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        parent = f"projects/{project_id}/locations/{location}"
        return client.create_workflow(
            request={"parent": parent, "workflow": workflow, "workflow_id": workflow_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_workflow(
        self,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Workflow:
        """
        Get details of a single Workflow.

        :param workflow_id: Required. The ID of the workflow to be created.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        return client.get_workflow(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )

    def update_workflow(
        self,
        workflow: dict | Workflow,
        update_mask: FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Update an existing workflow.

        Running this method has no impact on already running
        executions of the workflow. A new revision of the
        workflow may be created as a result of a successful
        update operation. In that case, such revision will be
        used in new workflow executions.

        :param workflow: Required. Workflow to be created.
        :param update_mask: List of fields to be updated. If not present,
            the entire workflow will be updated.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        return client.update_workflow(
            request={"workflow": workflow, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_workflow(
        self,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete a workflow with the specified name and all running executions of the workflow.

        :param workflow_id: Required. The ID of the workflow to be created.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        return client.delete_workflow(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_workflows(
        self,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListWorkflowsPager:
        """
        List Workflows in a given project and location; the default order is not specified.

        :param filter_: Filter to restrict results to specific workflows.
        :param order_by: Comma-separated list of fields that
            specifies the order of the results. Default sorting order for a field is ascending.
            To specify descending order for a field, append a "desc" suffix.
            If not specified, the results will be returned in an unspecified order.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        parent = f"projects/{project_id}/locations/{location}"

        return client.list_workflows(
            request={"parent": parent, "filter": filter_, "order_by": order_by},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_execution(
        self,
        workflow_id: str,
        location: str,
        execution: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Execution:
        """
        Create a new execution using the latest revision of the given workflow.

        :param execution: Required. Input parameters of the execution represented as a dictionary.
        :param workflow_id: Required. The ID of the workflow.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        parent = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        execution = {
            k: str(v) if isinstance(v, dict) else v for k, v in execution.items()
        }
        return client.create_execution(
            request={"parent": parent, "execution": execution},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_execution(
        self,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Execution:
        """
        Return an execution for the given ``workflow_id`` and ``execution_id``.

        :param workflow_id: Required. The ID of the workflow.
        :param execution_id: Required. The ID of the execution.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}/executions/{execution_id}"
        return client.get_execution(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_execution(
        self,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Execution:
        """
        Cancel an execution using the given ``workflow_id`` and ``execution_id``.

        :param workflow_id: Required. The ID of the workflow.
        :param execution_id: Required. The ID of the execution.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}/executions/{execution_id}"
        return client.cancel_execution(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_executions(
        self,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListExecutionsPager:
        """
        Return a list of executions which belong to the workflow with the given name.

        The method returns executions of all workflow revisions. Returned
        executions are ordered by their start time (newest first).

        :param workflow_id: Required. The ID of the workflow to be created.
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param location: Required. The GCP region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        parent = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        return client.list_executions(
            request={"parent": parent}, retry=retry, timeout=timeout, metadata=metadata
        )
