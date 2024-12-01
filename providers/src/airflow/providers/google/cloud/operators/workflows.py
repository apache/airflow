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

import datetime
import json
import re
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.workflows.executions_v1beta import Execution
from google.cloud.workflows_v1beta import Workflow

from airflow.providers.google.cloud.hooks.workflows import WorkflowsHook
from airflow.providers.google.cloud.links.workflows import (
    WorkflowsExecutionLink,
    WorkflowsListOfWorkflowsLink,
    WorkflowsWorkflowDetailsLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.utils.context import Context

from airflow.utils.hashlib_wrapper import md5


class WorkflowsCreateWorkflowOperator(GoogleCloudBaseOperator):
    """
    Creates a new workflow.

    If a workflow with the specified name already exists in the specified
    project and location, the long-running operation will return
    [ALREADY_EXISTS][google.rpc.Code.ALREADY_EXISTS] error.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsCreateWorkflowOperator`

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

    template_fields: Sequence[str] = ("location", "workflow", "workflow_id")
    template_fields_renderers = {"workflow": "json"}
    operator_extra_links = (WorkflowsWorkflowDetailsLink(),)

    def __init__(
        self,
        *,
        workflow: dict,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        force_rerun: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
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
        exec_date = context["logical_date"].isoformat()
        base = f"airflow_{self.dag_id}_{self.task_id}_{exec_date}_{hash_base}"
        workflow_id = md5(base.encode()).hexdigest()
        return re.sub(r"[:\-+.]", "_", workflow_id)

    def execute(self, context: Context):
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

        WorkflowsWorkflowDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            project_id=self.project_id or hook.project_id,
        )

        return Workflow.to_dict(workflow)


class WorkflowsUpdateWorkflowOperator(GoogleCloudBaseOperator):
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
    :param location: Required. The GCP region in which to handle the request.
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param update_mask: List of fields to be updated. If not present,
        the entire workflow will be updated.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("workflow_id", "update_mask")
    template_fields_renderers = {"update_mask": "json"}
    operator_extra_links = (WorkflowsWorkflowDetailsLink(),)

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        update_mask: FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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

        WorkflowsWorkflowDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            project_id=self.project_id or hook.project_id,
        )

        return Workflow.to_dict(workflow)


class WorkflowsDeleteWorkflowOperator(GoogleCloudBaseOperator):
    """
    Delete a workflow with the specified name and all running executions of the workflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsDeleteWorkflowOperator`

    :param workflow_id: Required. The ID of the workflow to be created.
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param location: Required. The GCP region in which to handle the request.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("location", "workflow_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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


class WorkflowsListWorkflowsOperator(GoogleCloudBaseOperator):
    """
    Lists Workflows in a given project and location; the default order is not specified.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsListWorkflowsOperator`

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

    template_fields: Sequence[str] = ("location", "order_by", "filter_")
    operator_extra_links = (WorkflowsListOfWorkflowsLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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

        WorkflowsListOfWorkflowsLink.persist(
            context=context,
            task_instance=self,
            project_id=self.project_id or hook.project_id,
        )

        return [Workflow.to_dict(w) for w in workflows_iter]


class WorkflowsGetWorkflowOperator(GoogleCloudBaseOperator):
    """
    Gets details of a single Workflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsGetWorkflowOperator`

    :param workflow_id: Required. The ID of the workflow to be created.
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param location: Required. The GCP region in which to handle the request.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("location", "workflow_id")
    operator_extra_links = (WorkflowsWorkflowDetailsLink(),)

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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

        WorkflowsWorkflowDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            project_id=self.project_id or hook.project_id,
        )

        return Workflow.to_dict(workflow)


class WorkflowsCreateExecutionOperator(GoogleCloudBaseOperator):
    """
    Creates a new execution using the latest revision of the given workflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsCreateExecutionOperator`

    :param execution: Required. Execution to be created.
    :param workflow_id: Required. The ID of the workflow.
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param location: Required. The GCP region in which to handle the request.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("location", "workflow_id", "execution")
    template_fields_renderers = {"execution": "json"}
    operator_extra_links = (WorkflowsExecutionLink(),)

    def __init__(
        self,
        *,
        workflow_id: str,
        execution: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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

        WorkflowsExecutionLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            execution_id=execution_id,
            project_id=self.project_id or hook.project_id,
        )

        return Execution.to_dict(execution)


class WorkflowsCancelExecutionOperator(GoogleCloudBaseOperator):
    """
    Cancels an execution using the given ``workflow_id`` and ``execution_id``.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsCancelExecutionOperator`

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

    template_fields: Sequence[str] = ("location", "workflow_id", "execution_id")
    operator_extra_links = (WorkflowsExecutionLink(),)

    def __init__(
        self,
        *,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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

        WorkflowsExecutionLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            execution_id=self.execution_id,
            project_id=self.project_id or hook.project_id,
        )

        return Execution.to_dict(execution)


class WorkflowsListExecutionsOperator(GoogleCloudBaseOperator):
    """
    Returns a list of executions which belong to the workflow with the given name.

    The method returns executions of all workflow revisions. Returned
    executions are ordered by their start time (newest first).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsListExecutionsOperator`

    :param workflow_id: Required. The ID of the workflow to be created.
    :param start_date_filter: If passed only executions older that this date will be returned.
        By default, operators return executions from last 60 minutes.
        Note that datetime object must specify a time zone, e.g. ``datetime.timezone.utc``.
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param location: Required. The GCP region in which to handle the request.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("location", "workflow_id")
    operator_extra_links = (WorkflowsWorkflowDetailsLink(),)

    def __init__(
        self,
        *,
        workflow_id: str,
        location: str,
        start_date_filter: datetime.datetime | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.workflow_id = workflow_id
        self.location = location
        self.start_date_filter = start_date_filter or datetime.datetime.now(
            tz=datetime.timezone.utc
        ) - datetime.timedelta(minutes=60)
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
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

        WorkflowsWorkflowDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            project_id=self.project_id or hook.project_id,
        )

        return [
            Execution.to_dict(e)
            for e in execution_iter
            if e.start_time > self.start_date_filter  # type: ignore
        ]


class WorkflowsGetExecutionOperator(GoogleCloudBaseOperator):
    """
    Returns an execution for the given ``workflow_id`` and ``execution_id``.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WorkflowsGetExecutionOperator`

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

    template_fields: Sequence[str] = ("location", "workflow_id", "execution_id")
    operator_extra_links = (WorkflowsExecutionLink(),)

    def __init__(
        self,
        *,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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

    def execute(self, context: Context):
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

        WorkflowsExecutionLink.persist(
            context=context,
            task_instance=self,
            location_id=self.location,
            workflow_id=self.workflow_id,
            execution_id=self.execution_id,
            project_id=self.project_id or hook.project_id,
        )

        return Execution.to_dict(execution)
