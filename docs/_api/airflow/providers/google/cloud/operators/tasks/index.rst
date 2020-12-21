:mod:`airflow.providers.google.cloud.operators.tasks`
=====================================================

.. py:module:: airflow.providers.google.cloud.operators.tasks

.. autoapi-nested-parse::

   This module contains various Google Cloud Tasks operators
   which allow you to perform basic operations using
   Cloud Tasks queues/tasks.



Module Contents
---------------

.. data:: MetaData
   

   

.. py:class:: CloudTasksQueueCreateOperator(*, location: str, task_queue: Queue, project_id: Optional[str] = None, queue_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.tasks_v2.types.Queue

   .. attribute:: template_fields
      :annotation: = ['task_queue', 'project_id', 'location', 'queue_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueueUpdateOperator(*, task_queue: Queue, project_id: Optional[str] = None, location: Optional[str] = None, queue_name: Optional[str] = None, update_mask: Union[Dict, FieldMask] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.tasks_v2.types.Queue

   .. attribute:: template_fields
      :annotation: = ['task_queue', 'project_id', 'location', 'queue_name', 'update_mask', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueueGetOperator(*, location: str, queue_name: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.tasks_v2.types.Queue

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueuesListOperator(*, location: str, project_id: Optional[str] = None, results_filter: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: list[google.cloud.tasks_v2.types.Queue]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueueDeleteOperator(*, location: str, queue_name: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueuePurgeOperator(*, location: str, queue_name: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: list[google.cloud.tasks_v2.types.Queue]

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueuePauseOperator(*, location: str, queue_name: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: list[google.cloud.tasks_v2.types.Queue]

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksQueueResumeOperator(*, location: str, queue_name: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: list[google.cloud.tasks_v2.types.Queue]

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksTaskCreateOperator(*, location: str, queue_name: str, task: Union[Dict, Task], project_id: Optional[str] = None, task_name: Optional[str] = None, response_view: Optional[enums.Task.View] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.tasks_v2.types.Task

   .. attribute:: template_fields
      :annotation: = ['task', 'project_id', 'location', 'queue_name', 'task_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksTaskGetOperator(*, location: str, queue_name: str, task_name: str, project_id: Optional[str] = None, response_view: Optional[enums.Task.View] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.tasks_v2.types.Task

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'task_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksTasksListOperator(*, location: str, queue_name: str, project_id: Optional[str] = None, response_view: Optional[enums.Task.View] = None, page_size: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: list[google.cloud.tasks_v2.types.Task]

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksTaskDeleteOperator(*, location: str, queue_name: str, task_name: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'task_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudTasksTaskRunOperator(*, location: str, queue_name: str, task_name: str, project_id: Optional[str] = None, response_view: Optional[enums.Task.View] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.tasks_v2.types.Task

   .. attribute:: template_fields
      :annotation: = ['location', 'queue_name', 'task_name', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




