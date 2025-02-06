 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.hooks.datasync`
=====================================================

.. py:module:: airflow.providers.amazon.aws.hooks.datasync

.. autoapi-nested-parse::

   Interact with AWS DataSync, using the AWS ``boto3`` library.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.datasync.DataSyncHook




.. py:class:: DataSyncHook(wait_interval_seconds = 30, *args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS DataSync.

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("datasync") <DataSync.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   :param wait_interval_seconds: Time to wait between two
       consecutive calls to check TaskExecution status. Defaults to 30 seconds.
   :raises ValueError: If wait_interval_seconds is not between 0 and 15*60 seconds.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: TASK_EXECUTION_INTERMEDIATE_STATES
      :value: ('INITIALIZING', 'QUEUED', 'LAUNCHING', 'PREPARING', 'TRANSFERRING', 'VERIFYING')



   .. py:attribute:: TASK_EXECUTION_FAILURE_STATES
      :value: ('ERROR',)



   .. py:attribute:: TASK_EXECUTION_SUCCESS_STATES
      :value: ('SUCCESS',)



   .. py:method:: create_location(location_uri, **create_location_kwargs)

      Create a new location.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.create_location_s3`
          - :external+boto3:py:meth:`DataSync.Client.create_location_smb`
          - :external+boto3:py:meth:`DataSync.Client.create_location_nfs`
          - :external+boto3:py:meth:`DataSync.Client.create_location_efs`

      :param location_uri: Location URI used to determine the location type (S3, SMB, NFS, EFS).
      :param create_location_kwargs: Passed to ``DataSync.Client.create_location_*`` methods.
      :return: LocationArn of the created Location.
      :raises AirflowException: If location type (prefix from ``location_uri``) is invalid.


   .. py:method:: get_location_arns(location_uri, case_sensitive = False, ignore_trailing_slash = True)

      Return all LocationArns which match a LocationUri.

      :param location_uri: Location URI to search for, eg ``s3://mybucket/mypath``
      :param case_sensitive: Do a case sensitive search for location URI.
      :param ignore_trailing_slash: Ignore / at the end of URI when matching.
      :return: List of LocationArns.
      :raises AirflowBadRequest: if ``location_uri`` is empty


   .. py:method:: create_task(source_location_arn, destination_location_arn, **create_task_kwargs)

      Create a Task between the specified source and destination LocationArns.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.create_task`

      :param source_location_arn: Source LocationArn. Must exist already.
      :param destination_location_arn: Destination LocationArn. Must exist already.
      :param create_task_kwargs: Passed to ``boto.create_task()``. See AWS boto3 datasync documentation.
      :return: TaskArn of the created Task


   .. py:method:: update_task(task_arn, **update_task_kwargs)

      Update a Task.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.update_task`

      :param task_arn: The TaskArn to update.
      :param update_task_kwargs: Passed to ``boto.update_task()``, See AWS boto3 datasync documentation.


   .. py:method:: delete_task(task_arn)

      Delete a Task.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.delete_task`

      :param task_arn: The TaskArn to delete.


   .. py:method:: get_task_arns_for_location_arns(source_location_arns, destination_location_arns)

      Return list of TaskArns which use both a specified source and destination LocationArns.

      :param source_location_arns: List of source LocationArns.
      :param destination_location_arns: List of destination LocationArns.
      :raises AirflowBadRequest: if ``source_location_arns`` or ``destination_location_arns`` are empty.


   .. py:method:: start_task_execution(task_arn, **kwargs)

      Start a TaskExecution for the specified task_arn.

      Each task can have at most one TaskExecution.
      Additional keyword arguments send to ``start_task_execution`` boto3 method.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.start_task_execution`

      :param task_arn: TaskArn
      :return: TaskExecutionArn
      :raises ClientError: If a TaskExecution is already busy running for this ``task_arn``.
      :raises AirflowBadRequest: If ``task_arn`` is empty.


   .. py:method:: cancel_task_execution(task_execution_arn)

      Cancel a TaskExecution for the specified ``task_execution_arn``.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.cancel_task_execution`

      :param task_execution_arn: TaskExecutionArn.
      :raises AirflowBadRequest: If ``task_execution_arn`` is empty.


   .. py:method:: get_task_description(task_arn)

      Get description for the specified ``task_arn``.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.describe_task`

      :param task_arn: TaskArn
      :return: AWS metadata about a task.
      :raises AirflowBadRequest: If ``task_arn`` is empty.


   .. py:method:: describe_task_execution(task_execution_arn)

      Get description for the specified ``task_execution_arn``.

      .. seealso::
          - :external+boto3:py:meth:`DataSync.Client.describe_task_execution`

      :param task_execution_arn: TaskExecutionArn
      :return: AWS metadata about a task execution.
      :raises AirflowBadRequest: If ``task_execution_arn`` is empty.


   .. py:method:: get_current_task_execution_arn(task_arn)

      Get current TaskExecutionArn (if one exists) for the specified ``task_arn``.

      :param task_arn: TaskArn
      :return: CurrentTaskExecutionArn for this ``task_arn`` or None.
      :raises AirflowBadRequest: if ``task_arn`` is empty.


   .. py:method:: wait_for_task_execution(task_execution_arn, max_iterations = 60)

      Wait for Task Execution status to be complete (SUCCESS/ERROR).

      The ``task_execution_arn`` must exist, or a boto3 ClientError will be raised.

      :param task_execution_arn: TaskExecutionArn
      :param max_iterations: Maximum number of iterations before timing out.
      :return: Result of task execution.
      :raises AirflowTaskTimeout: If maximum iterations is exceeded.
      :raises AirflowBadRequest: If ``task_execution_arn`` is empty.
