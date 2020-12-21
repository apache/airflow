:mod:`airflow.providers.amazon.aws.hooks.datasync`
==================================================

.. py:module:: airflow.providers.amazon.aws.hooks.datasync

.. autoapi-nested-parse::

   Interact with AWS DataSync, using the AWS ``boto3`` library.



Module Contents
---------------

.. py:class:: AWSDataSyncHook(wait_interval_seconds: int = 5, *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS DataSync.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator`

   :param int wait_for_task_execution: Time to wait between two
       consecutive calls to check TaskExecution status.
   :raises ValueError: If wait_interval_seconds is not between 0 and 15*60 seconds.

   .. attribute:: TASK_EXECUTION_INTERMEDIATE_STATES
      :annotation: = ['INITIALIZING', 'QUEUED', 'LAUNCHING', 'PREPARING', 'TRANSFERRING', 'VERIFYING']

      

   .. attribute:: TASK_EXECUTION_FAILURE_STATES
      :annotation: = ['ERROR']

      

   .. attribute:: TASK_EXECUTION_SUCCESS_STATES
      :annotation: = ['SUCCESS']

      

   
   .. method:: create_location(self, location_uri: str, **create_location_kwargs)

      Creates a new location.

      :param str location_uri: Location URI used to determine the location type (S3, SMB, NFS, EFS).
      :param \**create_location_kwargs: Passed to ``boto.create_location_xyz()``.
          See AWS boto3 datasync documentation.
      :return str: LocationArn of the created Location.
      :raises AirflowException: If location type (prefix from ``location_uri``) is invalid.



   
   .. method:: get_location_arns(self, location_uri: str, case_sensitive: bool = False, ignore_trailing_slash: bool = True)

      Return all LocationArns which match a LocationUri.

      :param str location_uri: Location URI to search for, eg ``s3://mybucket/mypath``
      :param bool case_sensitive: Do a case sensitive search for location URI.
      :param bool ignore_trailing_slash: Ignore / at the end of URI when matching.
      :return: List of LocationArns.
      :rtype: list(str)
      :raises AirflowBadRequest: if ``location_uri`` is empty



   
   .. method:: _refresh_locations(self)

      Refresh the local list of Locations.



   
   .. method:: create_task(self, source_location_arn: str, destination_location_arn: str, **create_task_kwargs)

      Create a Task between the specified source and destination LocationArns.

      :param str source_location_arn: Source LocationArn. Must exist already.
      :param str destination_location_arn: Destination LocationArn. Must exist already.
      :param \**create_task_kwargs: Passed to ``boto.create_task()``. See AWS boto3 datasync documentation.
      :return: TaskArn of the created Task
      :rtype: str



   
   .. method:: update_task(self, task_arn: str, **update_task_kwargs)

      Update a Task.

      :param str task_arn: The TaskArn to update.
      :param \**update_task_kwargs: Passed to ``boto.update_task()``, See AWS boto3 datasync documentation.



   
   .. method:: delete_task(self, task_arn: str)

      Delete a Task.

      :param str task_arn: The TaskArn to delete.



   
   .. method:: _refresh_tasks(self)

      Refreshes the local list of Tasks



   
   .. method:: get_task_arns_for_location_arns(self, source_location_arns: list, destination_location_arns: list)

      Return list of TaskArns for which use any one of the specified
      source LocationArns and any one of the specified destination LocationArns.

      :param list source_location_arns: List of source LocationArns.
      :param list destination_location_arns: List of destination LocationArns.
      :return: list
      :rtype: list(TaskArns)
      :raises AirflowBadRequest: if ``source_location_arns`` or ``destination_location_arns`` are empty.



   
   .. method:: start_task_execution(self, task_arn: str, **kwargs)

      Start a TaskExecution for the specified task_arn.
      Each task can have at most one TaskExecution.

      :param str task_arn: TaskArn
      :return: TaskExecutionArn
      :param \**kwargs: kwargs sent to ``boto3.start_task_execution()``
      :rtype: str
      :raises ClientError: If a TaskExecution is already busy running for this ``task_arn``.
      :raises AirflowBadRequest: If ``task_arn`` is empty.



   
   .. method:: cancel_task_execution(self, task_execution_arn: str)

      Cancel a TaskExecution for the specified ``task_execution_arn``.

      :param str task_execution_arn: TaskExecutionArn.
      :raises AirflowBadRequest: If ``task_execution_arn`` is empty.



   
   .. method:: get_task_description(self, task_arn: str)

      Get description for the specified ``task_arn``.

      :param str task_arn: TaskArn
      :return: AWS metadata about a task.
      :rtype: dict
      :raises AirflowBadRequest: If ``task_arn`` is empty.



   
   .. method:: describe_task_execution(self, task_execution_arn: str)

      Get description for the specified ``task_execution_arn``.

      :param str task_execution_arn: TaskExecutionArn
      :return: AWS metadata about a task execution.
      :rtype: dict
      :raises AirflowBadRequest: If ``task_execution_arn`` is empty.



   
   .. method:: get_current_task_execution_arn(self, task_arn: str)

      Get current TaskExecutionArn (if one exists) for the specified ``task_arn``.

      :param str task_arn: TaskArn
      :return: CurrentTaskExecutionArn for this ``task_arn`` or None.
      :rtype: str
      :raises AirflowBadRequest: if ``task_arn`` is empty.



   
   .. method:: wait_for_task_execution(self, task_execution_arn: str, max_iterations: int = 2 * 180)

      Wait for Task Execution status to be complete (SUCCESS/ERROR).
      The ``task_execution_arn`` must exist, or a boto3 ClientError will be raised.

      :param str task_execution_arn: TaskExecutionArn
      :param int max_iterations: Maximum number of iterations before timing out.
      :return: Result of task execution.
      :rtype: bool
      :raises AirflowTaskTimeout: If maximum iterations is exceeded.
      :raises AirflowBadRequest: If ``task_execution_arn`` is empty.




