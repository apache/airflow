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
import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, Sequence, Tuple, Union

import pytz
from google.api_core.exceptions import AlreadyExists
from google.api_core.retry import Retry

# pylint: disable=no-name-in-module
from google.cloud.workflows.executions_v1beta import Execution
from google.cloud.workflows_v1beta import Workflow

# pylint: enable=no-name-in-module
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.workflows import WorkflowsHook


class WorkflowsCreateWorkflowOperator(BaseOperator):
    """
    Creates a new workflow. If a workflow with the specified name
    already exists in the specified project and location, the long
    running operation will return
    [ALREADY_EXISTS][google.rpc.Code.ALREADY_EXISTS] error.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsCreateWorkflowOperator`

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

    template_fields = ("location", "workflow", "workflow_id")
    template_fields_renderers = {"workflow": "json"}

    def __init__(
        self,
        *,
        workflow: Dict,
        workflow_id: str,
        location: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        force_rerun: bool = False,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow = workflow
        self.workflow_id = workflow_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.force_rerun = force_rerun

    def _workflow_id(self, context):
        if self.workflow_id and not self.force_rerun:
            # If users provide workflow id then assuring the idempotency
            # is on their side
            return self.workflow_id

        if self.force_rerun:
            hash_base = str(uuid.uuid4())
        else:
            hash_base = json.dumps(self.workflow, sort_keys=True)

        # We are limited by allowed length of workflow_id so
        # we use hash of whole information
        exec_date = context['execution_date'].isoformat()
        base = f"airflow_{self.dag_id}_{self.task_id}_{exec_date}_{hash_base}"
        workflow_id = hashlib.md5(base.encode()).hexdigest()
        return re.sub(r"[:\-+.]", "_", workflow_id)

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        workflow_id = self._workflow_id(context)

        self.log.info("Creating workflow")
        try:
            operation = hook.create_workflow(
                workflow=self.workflow,
                workflow_id=workflow_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            workflow = operation.result()
        except AlreadyExists:
            workflow = hook.get_workflow(
                workflow_id=workflow_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return Workflow.to_dict(workflow)


class WorkflowsUpdateWorkflowOperator(BaseOperator):
    """
    Updates an existing workflow.
    Running this method has no impact on already running
    executions of the workflow. A new revision of the
    workflow may be created as a result of a successful
    update operation. In that case, such revision will be
    used in new workflow executions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsUpdateWorkflowOperator`

    :param workflow_id: Required. The ID of the workflow to be updated.
    :type workflow_id: str
    :param location: Required. The GCP region in which to handle the request.
    :type location: str
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :type project_id: str
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

    template_fields = ("workflow_id", "update_mask")
    template_fields_renderers = {"update_mask": "json"}

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        project_id: Optional[str] = None,
        update_mask: Optional[FieldMask] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.location = location
        self.project_id = project_id
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        workflow = hook.get_workflow(
            workflow_id=self.workflow_id,
            project_id=self.project_id,
            location=self.location,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Updating workflow")
        operation = hook.update_workflow(
            workflow=workflow,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        workflow = operation.result()
        return Workflow.to_dict(workflow)


class WorkflowsDeleteWorkflowOperator(BaseOperator):
    """
    Deletes a workflow with the specified name.
    This method also cancels and deletes all running
    executions of the workflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsDeleteWorkflowOperator`

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

    template_fields = ("location", "workflow_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Deleting workflow %s", self.workflow_id)
        operation = hook.delete_workflow(
            workflow_id=self.workflow_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation.result()


class WorkflowsListWorkflowsOperator(BaseOperator):
    """
    Lists Workflows in a given project and location.
    The default order is not specified.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsListWorkflowsOperator`

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

    template_fields = ("location", "order_by", "filter_")

    def __init__(
        self,
        *,
        location: str,
        project_id: Optional[str] = None,
        filter_: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.filter_ = filter_
        self.order_by = order_by
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Retrieving workflows")
        workflows_iter = hook.list_workflows(
            filter_=self.filter_,
            order_by=self.order_by,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [Workflow.to_dict(w) for w in workflows_iter]


class WorkflowsGetWorkflowOperator(BaseOperator):
    """
    Gets details of a single Workflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsGetWorkflowOperator`

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

    template_fields = ("location", "workflow_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Retrieving workflow")
        workflow = hook.get_workflow(
            workflow_id=self.workflow_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return Workflow.to_dict(workflow)


class WorkflowsCreateExecutionOperator(BaseOperator):
    """
    Creates a new execution using the latest revision of
    the given workflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsCreateExecutionOperator`

    :param execution: Required. Execution to be created.
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

    template_fields = ("location", "workflow_id", "execution")
    template_fields_renderers = {"execution": "json"}

    def __init__(
        self,
        *,
        workflow_id: str,
        execution: Dict,
        location: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.execution = execution
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Creating execution")
        execution = hook.create_execution(
            workflow_id=self.workflow_id,
            execution=self.execution,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        execution_id = execution.name.split("/")[-1]
        self.xcom_push(context, key="execution_id", value=execution_id)
        return Execution.to_dict(execution)


class WorkflowsCancelExecutionOperator(BaseOperator):
    """
    Cancels an execution using the given ``workflow_id`` and ``execution_id``.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsCancelExecutionOperator`

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

    template_fields = ("location", "workflow_id", "execution_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.execution_id = execution_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Canceling execution %s", self.execution_id)
        execution = hook.cancel_execution(
            workflow_id=self.workflow_id,
            execution_id=self.execution_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return Execution.to_dict(execution)


class WorkflowsListExecutionsOperator(BaseOperator):
    """
    Returns a list of executions which belong to the
    workflow with the given name. The method returns
    executions of all workflow revisions. Returned
    executions are ordered by their start time (newest
    first).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsListExecutionsOperator`

    :param workflow_id: Required. The ID of the workflow to be created.
    :type workflow_id: str
    :param start_date_filter: If passed only executions older that this date will be returned.
        By default operators return executions from last 60 minutes
    :type start_date_filter: datetime
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

    template_fields = ("location", "workflow_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        start_date_filter: Optional[datetime] = None,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.location = location
        self.start_date_filter = start_date_filter or datetime.now(tz=pytz.UTC) - timedelta(minutes=60)
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Retrieving executions for workflow %s", self.workflow_id)
        execution_iter = hook.list_executions(
            workflow_id=self.workflow_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        return [Execution.to_dict(e) for e in execution_iter if e.start_time > self.start_date_filter]


class WorkflowsGetExecutionOperator(BaseOperator):
    """
    Returns an execution for the given ``workflow_id`` and ``execution_id``.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsGetExecutionOperator`

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

    template_fields = ("location", "workflow_id", "execution_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.execution_id = execution_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = WorkflowsHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Retrieving execution %s for workflow %s", self.execution_id, self.workflow_id)
        execution = hook.get_execution(
            workflow_id=self.workflow_id,
            execution_id=self.execution_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return Execution.to_dict(execution)
