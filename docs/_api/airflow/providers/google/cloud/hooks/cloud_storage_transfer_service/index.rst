:mod:`airflow.providers.google.cloud.hooks.cloud_storage_transfer_service`
==========================================================================

.. py:module:: airflow.providers.google.cloud.hooks.cloud_storage_transfer_service

.. autoapi-nested-parse::

   This module contains a Google Storage Transfer Service Hook.



Module Contents
---------------

.. data:: log
   

   

.. data:: TIME_TO_SLEEP_IN_SECONDS
   :annotation: = 10

   

.. py:class:: GcpTransferJobsStatus

   Class with Google Cloud Transfer jobs statuses.

   .. attribute:: ENABLED
      :annotation: = ENABLED

      

   .. attribute:: DISABLED
      :annotation: = DISABLED

      

   .. attribute:: DELETED
      :annotation: = DELETED

      


.. py:class:: GcpTransferOperationStatus

   Class with Google Cloud Transfer operations statuses.

   .. attribute:: IN_PROGRESS
      :annotation: = IN_PROGRESS

      

   .. attribute:: PAUSED
      :annotation: = PAUSED

      

   .. attribute:: SUCCESS
      :annotation: = SUCCESS

      

   .. attribute:: FAILED
      :annotation: = FAILED

      

   .. attribute:: ABORTED
      :annotation: = ABORTED

      


.. data:: ACCESS_KEY_ID
   :annotation: = accessKeyId

   

.. data:: ALREADY_EXISTING_IN_SINK
   :annotation: = overwriteObjectsAlreadyExistingInSink

   

.. data:: AWS_ACCESS_KEY
   :annotation: = awsAccessKey

   

.. data:: AWS_S3_DATA_SOURCE
   :annotation: = awsS3DataSource

   

.. data:: BODY
   :annotation: = body

   

.. data:: BUCKET_NAME
   :annotation: = bucketName

   

.. data:: COUNTERS
   :annotation: = counters

   

.. data:: DAY
   :annotation: = day

   

.. data:: DESCRIPTION
   :annotation: = description

   

.. data:: FILTER
   :annotation: = filter

   

.. data:: FILTER_JOB_NAMES
   :annotation: = job_names

   

.. data:: FILTER_PROJECT_ID
   :annotation: = project_id

   

.. data:: GCS_DATA_SINK
   :annotation: = gcsDataSink

   

.. data:: GCS_DATA_SOURCE
   :annotation: = gcsDataSource

   

.. data:: HOURS
   :annotation: = hours

   

.. data:: HTTP_DATA_SOURCE
   :annotation: = httpDataSource

   

.. data:: JOB_NAME
   :annotation: = name

   

.. data:: LIST_URL
   :annotation: = list_url

   

.. data:: METADATA
   :annotation: = metadata

   

.. data:: MINUTES
   :annotation: = minutes

   

.. data:: MONTH
   :annotation: = month

   

.. data:: NAME
   :annotation: = name

   

.. data:: OBJECT_CONDITIONS
   :annotation: = object_conditions

   

.. data:: OPERATIONS
   :annotation: = operations

   

.. data:: PROJECT_ID
   :annotation: = projectId

   

.. data:: SCHEDULE
   :annotation: = schedule

   

.. data:: SCHEDULE_END_DATE
   :annotation: = scheduleEndDate

   

.. data:: SCHEDULE_START_DATE
   :annotation: = scheduleStartDate

   

.. data:: SECONDS
   :annotation: = seconds

   

.. data:: SECRET_ACCESS_KEY
   :annotation: = secretAccessKey

   

.. data:: START_TIME_OF_DAY
   :annotation: = startTimeOfDay

   

.. data:: STATUS
   :annotation: = status

   

.. data:: STATUS1
   :annotation: = status

   

.. data:: TRANSFER_JOB
   :annotation: = transfer_job

   

.. data:: TRANSFER_JOBS
   :annotation: = transferJobs

   

.. data:: TRANSFER_JOB_FIELD_MASK
   :annotation: = update_transfer_job_field_mask

   

.. data:: TRANSFER_OPERATIONS
   :annotation: = transferOperations

   

.. data:: TRANSFER_OPTIONS
   :annotation: = transfer_options

   

.. data:: TRANSFER_SPEC
   :annotation: = transferSpec

   

.. data:: YEAR
   :annotation: = year

   

.. data:: ALREADY_EXIST_CODE
   :annotation: = 409

   

.. data:: NEGATIVE_STATUSES
   

   

.. function:: gen_job_name(job_name: str) -> str
   Adds unique suffix to job name. If suffix already exists, updates it.
   Suffix — current timestamp

   :param job_name:
   :rtype job_name: str
   :return: job_name with suffix
   :rtype: str


.. py:class:: CloudDataTransferServiceHook(api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Storage Transfer Service.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Retrieves connection to Google Storage Transfer service.

      :return: Google Storage Transfer service object
      :rtype: dict



   
   .. method:: create_transfer_job(self, body: dict)

      Creates a transfer job that runs periodically.

      :param body: (Required) A request body, as described in
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
      :type body: dict
      :return: transfer job.
          See:
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
      :rtype: dict



   
   .. method:: get_transfer_job(self, job_name: str, project_id: str)

      Gets the latest state of a long-running operation in Google Storage
      Transfer Service.

      :param job_name: (Required) Name of the job to be fetched
      :type job_name: str
      :param project_id: (Optional) the ID of the project that owns the Transfer
          Job. If set to None or missing, the default project_id from the Google Cloud
          connection is used.
      :type project_id: str
      :return: Transfer Job
      :rtype: dict



   
   .. method:: list_transfer_job(self, request_filter: Optional[dict] = None, **kwargs)

      Lists long-running operations in Google Storage Transfer
      Service that match the specified filter.

      :param request_filter: (Required) A request filter, as described in
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
      :type request_filter: dict
      :return: List of Transfer Jobs
      :rtype: list[dict]



   
   .. method:: enable_transfer_job(self, job_name: str, project_id: str)

      New transfers will be performed based on the schedule.

      :param job_name: (Required) Name of the job to be updated
      :type job_name: str
      :param project_id: (Optional) the ID of the project that owns the Transfer
          Job. If set to None or missing, the default project_id from the Google Cloud
          connection is used.
      :type project_id: str
      :return: If successful, TransferJob.
      :rtype: dict



   
   .. method:: update_transfer_job(self, job_name: str, body: dict)

      Updates a transfer job that runs periodically.

      :param job_name: (Required) Name of the job to be updated
      :type job_name: str
      :param body: A request body, as described in
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
      :type body: dict
      :return: If successful, TransferJob.
      :rtype: dict



   
   .. method:: delete_transfer_job(self, job_name: str, project_id: str)

      Deletes a transfer job. This is a soft delete. After a transfer job is
      deleted, the job and all the transfer executions are subject to garbage
      collection. Transfer jobs become eligible for garbage collection
      30 days after soft delete.

      :param job_name: (Required) Name of the job to be deleted
      :type job_name: str
      :param project_id: (Optional) the ID of the project that owns the Transfer
          Job. If set to None or missing, the default project_id from the Google Cloud
          connection is used.
      :type project_id: str
      :rtype: None



   
   .. method:: cancel_transfer_operation(self, operation_name: str)

      Cancels an transfer operation in Google Storage Transfer Service.

      :param operation_name: Name of the transfer operation.
      :type operation_name: str
      :rtype: None



   
   .. method:: get_transfer_operation(self, operation_name: str)

      Gets an transfer operation in Google Storage Transfer Service.

      :param operation_name: (Required) Name of the transfer operation.
      :type operation_name: str
      :return: transfer operation
          See:
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/Operation
      :rtype: dict



   
   .. method:: list_transfer_operations(self, request_filter: Optional[dict] = None, **kwargs)

      Gets an transfer operation in Google Storage Transfer Service.

      :param request_filter: (Required) A request filter, as described in
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
          With one additional improvement:

          * project_id is optional if you have a project id defined
            in the connection
            See: :ref:`howto/connection:gcp`

      :type request_filter: dict
      :return: transfer operation
      :rtype: list[dict]



   
   .. method:: pause_transfer_operation(self, operation_name: str)

      Pauses an transfer operation in Google Storage Transfer Service.

      :param operation_name: (Required) Name of the transfer operation.
      :type operation_name: str
      :rtype: None



   
   .. method:: resume_transfer_operation(self, operation_name: str)

      Resumes an transfer operation in Google Storage Transfer Service.

      :param operation_name: (Required) Name of the transfer operation.
      :type operation_name: str
      :rtype: None



   
   .. method:: wait_for_transfer_job(self, job: dict, expected_statuses: Optional[Set[str]] = None, timeout: Optional[Union[float, timedelta]] = None)

      Waits until the job reaches the expected state.

      :param job: Transfer job
          See:
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
      :type job: dict
      :param expected_statuses: State that is expected
          See:
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
      :type expected_statuses: set[str]
      :param timeout: Time in which the operation must end in seconds. If not specified, defaults to 60
          seconds.
      :type timeout: Optional[Union[float, timedelta]]
      :rtype: None



   
   .. method:: _inject_project_id(self, body: dict, param_name: str, target_key: str)



   
   .. staticmethod:: operations_contain_expected_statuses(operations: List[dict], expected_statuses: Union[Set[str], str])

      Checks whether the operation list has an operation with the
      expected status, then returns true
      If it encounters operations in FAILED or ABORTED state
      throw :class:`airflow.exceptions.AirflowException`.

      :param operations: (Required) List of transfer operations to check.
      :type operations: list[dict]
      :param expected_statuses: (Required) status that is expected
          See:
          https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
      :type expected_statuses: set[str]
      :return: If there is an operation with the expected state
          in the operation list, returns true,
      :raises: airflow.exceptions.AirflowException If it encounters operations
          with a state in the list,
      :rtype: bool




