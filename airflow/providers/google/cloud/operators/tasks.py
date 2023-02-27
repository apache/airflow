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
"""
This module contains various Google Cloud Tasks operators
which allow you to perform basic operations using
Cloud Tasks queues/tasks.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence, Tuple

from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue, Task
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.providers.google.cloud.hooks.tasks import CloudTasksHook
from airflow.providers.google.cloud.links.cloud_tasks import CloudTasksLink, CloudTasksQueueLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


MetaData = Sequence[Tuple[str, str]]


class CloudTasksQueueCreateOperator(GoogleCloudBaseOperator):
    """
    Creates a queue in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueueCreateOperator`

    :param location: The location name in which the queue will be created.
    :param task_queue: The task queue to create.
        Queue's name cannot be the same as an existing queue.
        If a dict is provided, it must be of the same form as the protobuf message Queue.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param queue_name: (Optional) The queue's name.
        If provided, it will be used to construct the full queue path.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "task_queue",
        "project_id",
        "location",
        "queue_name",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        task_queue: Queue,
        project_id: str | None = None,
        queue_name: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.task_queue = task_queue
        self.project_id = project_id
        self.queue_name = queue_name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            queue = hook.create_queue(
                location=self.location,
                task_queue=self.task_queue,
                project_id=self.project_id,
                queue_name=self.queue_name,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            if self.queue_name is None:
                raise RuntimeError("The queue name should be set here!")
            queue = hook.get_queue(
                location=self.location,
                project_id=self.project_id,
                queue_name=self.queue_name,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=queue.name,
        )
        return Queue.to_dict(queue)


class CloudTasksQueueUpdateOperator(GoogleCloudBaseOperator):
    """
    Updates a queue in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueueUpdateOperator`

    :param task_queue: The task queue to update.
        This method creates the queue if it does not exist and updates the queue if
        it does exist. The queue's name must be specified.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: (Optional) The location name in which the queue will be updated.
        If provided, it will be used to construct the full queue path.
    :param queue_name: (Optional) The queue's name.
        If provided, it will be used to construct the full queue path.
    :param update_mask: A mast used to specify which fields of the queue are being updated.
        If empty, then all fields will be updated.
        If a dict is provided, it must be of the same form as the protobuf message.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "task_queue",
        "project_id",
        "location",
        "queue_name",
        "update_mask",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        task_queue: Queue,
        project_id: str | None = None,
        location: str | None = None,
        queue_name: str | None = None,
        update_mask: FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.task_queue = task_queue
        self.project_id = project_id
        self.location = location
        self.queue_name = queue_name
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        queue = hook.update_queue(
            task_queue=self.task_queue,
            project_id=self.project_id,
            location=self.location,
            queue_name=self.queue_name,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=queue.name,
        )
        return Queue.to_dict(queue)


class CloudTasksQueueGetOperator(GoogleCloudBaseOperator):
    """
    Gets a queue from Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueueGetOperator`

    :param location: The location name in which the queue was created.
    :param queue_name: The queue's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        queue = hook.get_queue(
            location=self.location,
            queue_name=self.queue_name,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=queue.name,
        )
        return Queue.to_dict(queue)


class CloudTasksQueuesListOperator(GoogleCloudBaseOperator):
    """
    Lists queues from Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueuesListOperator`

    :param location: The location name in which the queues were created.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param results_filter: (Optional) Filter used to specify a subset of queues.
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str | None = None,
        results_filter: str | None = None,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.results_filter = results_filter
        self.page_size = page_size
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        queues = hook.list_queues(
            location=self.location,
            project_id=self.project_id,
            results_filter=self.results_filter,
            page_size=self.page_size,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksLink.persist(
            operator_instance=self,
            context=context,
            project_id=self.project_id or hook.project_id,
        )
        return [Queue.to_dict(q) for q in queues]


class CloudTasksQueueDeleteOperator(GoogleCloudBaseOperator):
    """
    Deletes a queue from Cloud Tasks, even if it has tasks in it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueueDeleteOperator`

    :param location: The location name in which the queue will be deleted.
    :param queue_name: The queue's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_queue(
            location=self.location,
            queue_name=self.queue_name,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudTasksQueuePurgeOperator(GoogleCloudBaseOperator):
    """
    Purges a queue by deleting all of its tasks from Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueuePurgeOperator`

    :param location: The location name in which the queue will be purged.
    :param queue_name: The queue's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        queue = hook.purge_queue(
            location=self.location,
            queue_name=self.queue_name,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=queue.name,
        )
        return Queue.to_dict(queue)


class CloudTasksQueuePauseOperator(GoogleCloudBaseOperator):
    """
    Pauses a queue in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueuePauseOperator`

    :param location: The location name in which the queue will be paused.
    :param queue_name: The queue's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        queue = hook.pause_queue(
            location=self.location,
            queue_name=self.queue_name,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=queue.name,
        )
        return Queue.to_dict(queue)


class CloudTasksQueueResumeOperator(GoogleCloudBaseOperator):
    """
    Resumes a queue in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksQueueResumeOperator`

    :param location: The location name in which the queue will be resumed.
    :param queue_name: The queue's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        queue = hook.resume_queue(
            location=self.location,
            queue_name=self.queue_name,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=queue.name,
        )
        return Queue.to_dict(queue)


class CloudTasksTaskCreateOperator(GoogleCloudBaseOperator):
    """
    Creates a task in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksTaskCreateOperator`

    :param location: The location name in which the task will be created.
    :param queue_name: The queue's name.
    :param task: The task to add.
        If a dict is provided, it must be of the same form as the protobuf message Task.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param task_name: (Optional) The task's name.
        If provided, it will be used to construct the full task path.
    :param response_view: (Optional) This field specifies which subset of the Task will
        be returned.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "task",
        "project_id",
        "location",
        "queue_name",
        "task_name",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        task: dict | Task,
        project_id: str | None = None,
        task_name: str | None = None,
        response_view: Task.View | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.task = task
        self.project_id = project_id
        self.task_name = task_name
        self.response_view = response_view
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        task = hook.create_task(
            location=self.location,
            queue_name=self.queue_name,
            task=self.task,
            project_id=self.project_id,
            task_name=self.task_name,
            response_view=self.response_view,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=task.name,
        )
        return Task.to_dict(task)


class CloudTasksTaskGetOperator(GoogleCloudBaseOperator):
    """
    Gets a task from Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksTaskGetOperator`

    :param location: The location name in which the task was created.
    :param queue_name: The queue's name.
    :param task_name: The task's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param response_view: (Optional) This field specifies which subset of the Task will
        be returned.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "task_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        task_name: str,
        project_id: str | None = None,
        response_view: Task.View | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.task_name = task_name
        self.project_id = project_id
        self.response_view = response_view
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        task = hook.get_task(
            location=self.location,
            queue_name=self.queue_name,
            task_name=self.task_name,
            project_id=self.project_id,
            response_view=self.response_view,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=task.name,
        )
        return Task.to_dict(task)


class CloudTasksTasksListOperator(GoogleCloudBaseOperator):
    """
    Lists the tasks in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksTasksListOperator`

    :param location: The location name in which the tasks were created.
    :param queue_name: The queue's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param response_view: (Optional) This field specifies which subset of the Task will
        be returned.
    :param page_size: (Optional) The maximum number of resources contained in the
        underlying API response.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        project_id: str | None = None,
        response_view: Task.View | None = None,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.project_id = project_id
        self.response_view = response_view
        self.page_size = page_size
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        tasks = hook.list_tasks(
            location=self.location,
            queue_name=self.queue_name,
            project_id=self.project_id,
            response_view=self.response_view,
            page_size=self.page_size,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=f"projects/{self.project_id or hook.project_id}/"
            f"locations/{self.location}/queues/{self.queue_name}",
        )
        return [Task.to_dict(t) for t in tasks]


class CloudTasksTaskDeleteOperator(GoogleCloudBaseOperator):
    """
    Deletes a task from Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksTaskDeleteOperator`

    :param location: The location name in which the task will be deleted.
    :param queue_name: The queue's name.
    :param task_name: The task's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "task_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        task_name: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.task_name = task_name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_task(
            location=self.location,
            queue_name=self.queue_name,
            task_name=self.task_name,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudTasksTaskRunOperator(GoogleCloudBaseOperator):
    """
    Forces to run a task in Cloud Tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTasksTaskRunOperator`

    :param location: The location name in which the task was created.
    :param queue_name: The queue's name.
    :param task_name: The task's name.
    :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param response_view: (Optional) This field specifies which subset of the Task will
        be returned.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "location",
        "queue_name",
        "task_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (CloudTasksQueueLink(),)

    def __init__(
        self,
        *,
        location: str,
        queue_name: str,
        task_name: str,
        project_id: str | None = None,
        response_view: Task.View | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.queue_name = queue_name
        self.task_name = task_name
        self.project_id = project_id
        self.response_view = response_view
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudTasksHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        task = hook.run_task(
            location=self.location,
            queue_name=self.queue_name,
            task_name=self.task_name,
            project_id=self.project_id,
            response_view=self.response_view,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        CloudTasksQueueLink.persist(
            operator_instance=self,
            context=context,
            queue_name=task.name,
        )
        return Task.to_dict(task)
