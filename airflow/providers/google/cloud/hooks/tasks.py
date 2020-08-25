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
This module contains a CloudTasksHook
which allows you to connect to GCP Cloud Tasks service,
performing actions to queues or tasks.
"""
from typing import Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.tasks_v2 import CloudTasksClient, enums
from google.cloud.tasks_v2.types import FieldMask, Queue, Task

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudTasksHook(GoogleBaseHook):
    """
    Hook for Google Cloud Tasks APIs. Cloud Tasks allows developers to manage
    the execution of background work in their applications.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to, impersonation_chain=impersonation_chain,
        )
        self._client = None

    def get_conn(self):
        """
        Provides a client for interacting with the Cloud Tasks API.

        :return: GCP Cloud Tasks API Client
        :rtype: google.cloud.tasks_v2.CloudTasksClient
        """
        if not self._client:
            self._client = CloudTasksClient(credentials=self._get_credentials(), client_info=self.client_info)
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def create_queue(
        self,
        location: str,
        task_queue: Union[Dict, Queue],
        project_id: str,
        queue_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Queue:
        """
        Creates a queue in Cloud Tasks.

        :param location: The location name in which the queue will be created.
        :type location: str
        :param task_queue: The task queue to create.
            Queue's name cannot be the same as an existing queue.
            If a dict is provided, it must be of the same form as the protobuf message Queue.
        :type task_queue: dict or google.cloud.tasks_v2.types.Queue
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param queue_name: (Optional) The queue's name.
            If provided, it will be used to construct the full queue path.
        :type queue_name: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.tasks_v2.types.Queue
        """

        client = self.get_conn()

        if queue_name:
            full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
            if isinstance(task_queue, Queue):
                task_queue.name = full_queue_name
            elif isinstance(task_queue, dict):
                task_queue['name'] = full_queue_name
            else:
                raise AirflowException('Unable to set queue_name.')
        full_location_path = CloudTasksClient.location_path(project_id, location)
        return client.create_queue(
            parent=full_location_path, queue=task_queue, retry=retry, timeout=timeout, metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_queue(
        self,
        task_queue: Queue,
        project_id: str,
        location: Optional[str] = None,
        queue_name: Optional[str] = None,
        update_mask: Optional[FieldMask] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Queue:
        """
        Updates a queue in Cloud Tasks.

        :param task_queue: The task queue to update.
            This method creates the queue if it does not exist and updates the queue if
            it does exist. The queue's name must be specified.
        :type task_queue: dict or google.cloud.tasks_v2.types.Queue
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param location: (Optional) The location name in which the queue will be updated.
            If provided, it will be used to construct the full queue path.
        :type location: str
        :param queue_name: (Optional) The queue's name.
            If provided, it will be used to construct the full queue path.
        :type queue_name: str
        :param update_mask: A mast used to specify which fields of the queue are being updated.
            If empty, then all fields will be updated.
            If a dict is provided, it must be of the same form as the protobuf message.
        :type update_mask: dict or google.cloud.tasks_v2.types.FieldMask
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.tasks_v2.types.Queue
        """

        client = self.get_conn()

        if queue_name and location:
            full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
            if isinstance(task_queue, Queue):
                task_queue.name = full_queue_name
            elif isinstance(task_queue, dict):
                task_queue['name'] = full_queue_name
            else:
                raise AirflowException('Unable to set queue_name.')
        return client.update_queue(
            queue=task_queue, update_mask=update_mask, retry=retry, timeout=timeout, metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_queue(
        self,
        location: str,
        queue_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Queue:
        """
        Gets a queue from Cloud Tasks.

        :param location: The location name in which the queue was created.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.tasks_v2.types.Queue
        """

        client = self.get_conn()

        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        return client.get_queue(name=full_queue_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_queues(
        self,
        location: str,
        project_id: str,
        results_filter: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Queue]:
        """
        Lists queues from Cloud Tasks.

        :param location: The location name in which the queues were created.
        :type location: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param results_filter: (Optional) Filter used to specify a subset of queues.
        :type results_filter: str
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :type page_size: int
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: list[google.cloud.tasks_v2.types.Queue]
        """

        client = self.get_conn()

        full_location_path = CloudTasksClient.location_path(project_id, location)
        queues = client.list_queues(
            parent=full_location_path,
            filter_=results_filter,
            page_size=page_size,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(queues)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_queue(
        self,
        location: str,
        queue_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a queue from Cloud Tasks, even if it has tasks in it.

        :param location: The location name in which the queue will be deleted.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        """

        client = self.get_conn()

        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        client.delete_queue(name=full_queue_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def purge_queue(
        self,
        location: str,
        queue_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Queue]:
        """
        Purges a queue by deleting all of its tasks from Cloud Tasks.

        :param location: The location name in which the queue will be purged.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: list[google.cloud.tasks_v2.types.Queue]
        """

        client = self.get_conn()

        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        return client.purge_queue(name=full_queue_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def pause_queue(
        self,
        location: str,
        queue_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Queue]:
        """
        Pauses a queue in Cloud Tasks.

        :param location: The location name in which the queue will be paused.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: list[google.cloud.tasks_v2.types.Queue]
        """

        client = self.get_conn()

        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        return client.pause_queue(name=full_queue_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def resume_queue(
        self,
        location: str,
        queue_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Queue]:
        """
        Resumes a queue in Cloud Tasks.

        :param location: The location name in which the queue will be resumed.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: list[google.cloud.tasks_v2.types.Queue]
        """

        client = self.get_conn()

        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        return client.resume_queue(name=full_queue_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_task(
        self,
        location: str,
        queue_name: str,
        task: Union[Dict, Task],
        project_id: str,
        task_name: Optional[str] = None,
        response_view: Optional[enums.Task.View] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Task:
        """
        Creates a task in Cloud Tasks.

        :param location: The location name in which the task will be created.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param task: The task to add.
            If a dict is provided, it must be of the same form as the protobuf message Task.
        :type task: dict or google.cloud.tasks_v2.types.Task
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param task_name: (Optional) The task's name.
            If provided, it will be used to construct the full task path.
        :type task_name: str
        :param response_view: (Optional) This field specifies which subset of the Task will
            be returned.
        :type response_view: google.cloud.tasks_v2.enums.Task.View
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.tasks_v2.types.Task
        """

        client = self.get_conn()

        if task_name:
            full_task_name = CloudTasksClient.task_path(project_id, location, queue_name, task_name)
            if isinstance(task, Task):
                task.name = full_task_name
            elif isinstance(task, dict):
                task['name'] = full_task_name
            else:
                raise AirflowException('Unable to set task_name.')
        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        return client.create_task(
            parent=full_queue_name,
            task=task,
            response_view=response_view,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_task(
        self,
        location: str,
        queue_name: str,
        task_name: str,
        project_id: str,
        response_view: Optional[enums.Task.View] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Task:
        """
        Gets a task from Cloud Tasks.

        :param location: The location name in which the task was created.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param task_name: The task's name.
        :type task_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param response_view: (Optional) This field specifies which subset of the Task will
            be returned.
        :type response_view: google.cloud.tasks_v2.enums.Task.View
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.tasks_v2.types.Task
        """

        client = self.get_conn()

        full_task_name = CloudTasksClient.task_path(project_id, location, queue_name, task_name)
        return client.get_task(
            name=full_task_name, response_view=response_view, retry=retry, timeout=timeout, metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tasks(
        self,
        location: str,
        queue_name: str,
        project_id: str,
        response_view: Optional[enums.Task.View] = None,
        page_size: Optional[int] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[Task]:
        """
        Lists the tasks in Cloud Tasks.

        :param location: The location name in which the tasks were created.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param response_view: (Optional) This field specifies which subset of the Task will
            be returned.
        :type response_view: google.cloud.tasks_v2.enums.Task.View
        :param page_size: (Optional) The maximum number of resources contained in the
            underlying API response.
        :type page_size: int
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: list[google.cloud.tasks_v2.types.Task]
        """

        client = self.get_conn()
        full_queue_name = CloudTasksClient.queue_path(project_id, location, queue_name)
        tasks = client.list_tasks(
            parent=full_queue_name,
            response_view=response_view,
            page_size=page_size,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return list(tasks)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_task(
        self,
        location: str,
        queue_name: str,
        task_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a task from Cloud Tasks.

        :param location: The location name in which the task will be deleted.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param task_name: The task's name.
        :type task_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        """

        client = self.get_conn()

        full_task_name = CloudTasksClient.task_path(project_id, location, queue_name, task_name)
        client.delete_task(name=full_task_name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def run_task(
        self,
        location: str,
        queue_name: str,
        task_name: str,
        project_id: str,
        response_view: Optional[enums.Task.View] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Task:
        """
        Forces to run a task in Cloud Tasks.

        :param location: The location name in which the task was created.
        :type location: str
        :param queue_name: The queue's name.
        :type queue_name: str
        :param task_name: The task's name.
        :type task_name: str
        :param project_id: (Optional) The ID of the  GCP project that owns the Cloud Tasks.
            If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param response_view: (Optional) This field specifies which subset of the Task will
            be returned.
        :type response_view: google.cloud.tasks_v2.enums.Task.View
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.tasks_v2.types.Task
        """

        client = self.get_conn()

        full_task_name = CloudTasksClient.task_path(project_id, location, queue_name, task_name)
        return client.run_task(
            name=full_task_name, response_view=response_view, retry=retry, timeout=timeout, metadata=metadata,
        )
