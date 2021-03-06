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

from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.operation import Operation
from google.api_core.retry import Retry

# pylint: disable=no-name-in-module
from google.cloud.workflows.executions_v1beta import Execution, ExecutionsClient
from google.cloud.workflows.executions_v1beta.services.executions.pagers import ListExecutionsPager
from google.cloud.workflows_v1beta import Workflow, WorkflowsClient
from google.cloud.workflows_v1beta.services.workflows.pagers import ListWorkflowsPager
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# pylint: enable=no-name-in-module


class WorkflowsHook(GoogleBaseHook):
    """
    Hook for Google GCP APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def get_workflows_client(self) -> WorkflowsClient:
        """Returns WorkflowsClient."""
        return WorkflowsClient(credentials=self._get_credentials(), client_info=self.client_info)

    def get_executions_client(self) -> ExecutionsClient:
        """Returns ExecutionsClient."""
        return ExecutionsClient(credentials=self._get_credentials(), client_info=self.client_info)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_workflow(
        self,
        workflow: Dict,
        workflow_id: str,
        location: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Operation:
        """
        Creates a new workflow. If a workflow with the specified name
        already exists in the specified project and location, the long
        running operation will return
        [ALREADY_EXISTS][google.rpc.Code.ALREADY_EXISTS] error.

        :param workflow: Required. Workflow to be created.
        :type workflow: Dict
        :param workflow_id: Required. The ID of the workflow to be created.
        :type workflow_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
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
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Workflow:
        """
        Gets details of a single Workflow.

        :param workflow_id: Required. The ID of the workflow to be created.
        :type workflow_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        return client.get_workflow(request={"name": name}, retry=retry, timeout=timeout, metadata=metadata)

    def update_workflow(
        self,
        workflow: Union[Dict, Workflow],
        update_mask: Optional[FieldMask] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Operation:
        """
        Updates an existing workflow.
        Running this method has no impact on already running
        executions of the workflow. A new revision of the
        workflow may be created as a result of a successful
        update operation. In that case, such revision will be
        used in new workflow executions.

        :param workflow: Required. Workflow to be created.
        :type workflow: Dict
        :param update_mask: List of fields to be updated. If not present,
            the entire workflow will be updated.
        :type update_mask: FieldMask
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
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
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Operation:
        """
        Deletes a workflow with the specified name.
        This method also cancels and deletes all running
        executions of the workflow.

        :param workflow_id: Required. The ID of the workflow to be created.
        :type workflow_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_workflows_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        return client.delete_workflow(request={"name": name}, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_workflows(
        self,
        location: str,
        project_id: str,
        filter_: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ListWorkflowsPager:
        """
        Lists Workflows in a given project and location.
        The default order is not specified.

        :param filter_: Filter to restrict results to specific workflows.
        :type filter_: str
        :param order_by: Comma-separated list of fields that that
            specify the order of the results. Default sorting order for a field is ascending.
            To specify descending order for a field, append a "desc" suffix.
            If not specified, the results will be returned in an unspecified order.
        :type order_by: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
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
        project_id: str,
        execution: Dict,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Execution:
        """
        Creates a new execution using the latest revision of
        the given workflow.

        :param execution: Required. Input parameters of the execution represented as a dictionary.
        :type execution: Dict
        :param workflow_id: Required. The ID of the workflow.
        :type workflow_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        parent = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
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
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Execution:
        """
        Returns an execution for the given ``workflow_id`` and ``execution_id``.

        :param workflow_id: Required. The ID of the workflow.
        :type workflow_id: str
        :param execution_id: Required. The ID of the execution.
        :type execution_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        name = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}/executions/{execution_id}"
        return client.get_execution(request={"name": name}, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_execution(
        self,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Execution:
        """
        Cancels an execution using the given ``workflow_id`` and ``execution_id``.

        :param workflow_id: Required. The ID of the workflow.
        :type workflow_id: str
        :param execution_id: Required. The ID of the execution.
        :type execution_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
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
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ListExecutionsPager:
        """
        Returns a list of executions which belong to the
        workflow with the given name. The method returns
        executions of all workflow revisions. Returned
        executions are ordered by their start time (newest
        first).

        :param workflow_id: Required. The ID of the workflow to be created.
        :type workflow_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The GCP region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_executions_client()
        parent = f"projects/{project_id}/locations/{location}/workflows/{workflow_id}"
        return client.list_executions(
            request={"parent": parent}, retry=retry, timeout=timeout, metadata=metadata
        )
