:mod:`airflow.providers.google.cloud.hooks.tasks`
=================================================

.. py:module:: airflow.providers.google.cloud.hooks.tasks

.. autoapi-nested-parse::

   This module contains a CloudTasksHook
   which allows you to connect to Google Cloud Tasks service,
   performing actions to queues or tasks.



Module Contents
---------------

.. py:class:: CloudTasksHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

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

   
   .. method:: get_conn(self)

      Provides a client for interacting with the Google Cloud Tasks API.

      :return: Google Cloud Tasks API Client
      :rtype: google.cloud.tasks_v2.CloudTasksClient



   
   .. method:: create_queue(self, location: str, task_queue: Union[dict, Queue], project_id: str, queue_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a queue in Cloud Tasks.

      :param location: The location name in which the queue will be created.
      :type location: str
      :param task_queue: The task queue to create.
          Queue's name cannot be the same as an existing queue.
          If a dict is provided, it must be of the same form as the protobuf message Queue.
      :type task_queue: dict or google.cloud.tasks_v2.types.Queue
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: update_queue(self, task_queue: Queue, project_id: str, location: Optional[str] = None, queue_name: Optional[str] = None, update_mask: Optional[FieldMask] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates a queue in Cloud Tasks.

      :param task_queue: The task queue to update.
          This method creates the queue if it does not exist and updates the queue if
          it does exist. The queue's name must be specified.
      :type task_queue: dict or google.cloud.tasks_v2.types.Queue
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: get_queue(self, location: str, queue_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets a queue from Cloud Tasks.

      :param location: The location name in which the queue was created.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: list_queues(self, location: str, project_id: str, results_filter: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists queues from Cloud Tasks.

      :param location: The location name in which the queues were created.
      :type location: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: delete_queue(self, location: str, queue_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a queue from Cloud Tasks, even if it has tasks in it.

      :param location: The location name in which the queue will be deleted.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: purge_queue(self, location: str, queue_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Purges a queue by deleting all of its tasks from Cloud Tasks.

      :param location: The location name in which the queue will be purged.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: pause_queue(self, location: str, queue_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Pauses a queue in Cloud Tasks.

      :param location: The location name in which the queue will be paused.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: resume_queue(self, location: str, queue_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Resumes a queue in Cloud Tasks.

      :param location: The location name in which the queue will be resumed.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: create_task(self, location: str, queue_name: str, task: Union[Dict, Task], project_id: str, task_name: Optional[str] = None, response_view: Optional[enums.Task.View] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a task in Cloud Tasks.

      :param location: The location name in which the task will be created.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param task: The task to add.
          If a dict is provided, it must be of the same form as the protobuf message Task.
      :type task: dict or google.cloud.tasks_v2.types.Task
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: get_task(self, location: str, queue_name: str, task_name: str, project_id: str, response_view: Optional[enums.Task.View] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets a task from Cloud Tasks.

      :param location: The location name in which the task was created.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param task_name: The task's name.
      :type task_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: list_tasks(self, location: str, queue_name: str, project_id: str, response_view: Optional[enums.Task.View] = None, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists the tasks in Cloud Tasks.

      :param location: The location name in which the tasks were created.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: delete_task(self, location: str, queue_name: str, task_name: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a task from Cloud Tasks.

      :param location: The location name in which the task will be deleted.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param task_name: The task's name.
      :type task_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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



   
   .. method:: run_task(self, location: str, queue_name: str, task_name: str, project_id: str, response_view: Optional[enums.Task.View] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Forces to run a task in Cloud Tasks.

      :param location: The location name in which the task was created.
      :type location: str
      :param queue_name: The queue's name.
      :type queue_name: str
      :param task_name: The task's name.
      :type task_name: str
      :param project_id: (Optional) The ID of the Google Cloud project that owns the Cloud Tasks.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
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




