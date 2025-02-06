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

:py:mod:`airflow.providers.amazon.aws.operators.datasync`
=========================================================

.. py:module:: airflow.providers.amazon.aws.operators.datasync

.. autoapi-nested-parse::

   Create, get, update, execute and delete an AWS DataSync Task.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.datasync.DataSyncOperator




.. py:class:: DataSyncOperator(*, aws_conn_id = 'aws_default', wait_interval_seconds = 30, max_iterations = 60, wait_for_completion = True, task_arn = None, source_location_uri = None, destination_location_uri = None, allow_random_task_choice = False, allow_random_location_choice = False, create_task_kwargs = None, create_source_location_kwargs = None, create_destination_location_kwargs = None, update_task_kwargs = None, task_execution_kwargs = None, delete_task_after_execution = False, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Find, Create, Update, Execute and Delete AWS DataSync Tasks.

   If ``do_xcom_push`` is True, then the DataSync TaskArn and TaskExecutionArn
   which were executed will be pushed to an XCom.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DataSyncOperator`

   .. note:: There may be 0, 1, or many existing DataSync Tasks defined in your AWS
       environment. The default behavior is to create a new Task if there are 0, or
       execute the Task if there was 1 Task, or fail if there were many Tasks.

   :param aws_conn_id: AWS connection to use.
   :param wait_interval_seconds: Time to wait between two
       consecutive calls to check TaskExecution status.
   :param max_iterations: Maximum number of
       consecutive calls to check TaskExecution status.
   :param wait_for_completion: If True, wait for the task execution to reach a final state
   :param task_arn: AWS DataSync TaskArn to use. If None, then this operator will
       attempt to either search for an existing Task or attempt to create a new Task.
   :param source_location_uri: Source location URI to search for. All DataSync
       Tasks with a LocationArn with this URI will be considered.
       Example: ``smb://server/subdir``
   :param destination_location_uri: Destination location URI to search for.
       All DataSync Tasks with a LocationArn with this URI will be considered.
       Example: ``s3://airflow_bucket/stuff``
   :param allow_random_task_choice: If multiple Tasks match, one must be chosen to
       execute. If allow_random_task_choice is True then a random one is chosen.
   :param allow_random_location_choice: If multiple Locations match, one must be chosen
       when creating a task. If allow_random_location_choice is True then a random one is chosen.
   :param create_task_kwargs: If no suitable TaskArn is identified,
       it will be created if ``create_task_kwargs`` is defined.
       ``create_task_kwargs`` is then used internally like this:
       ``boto3.create_task(**create_task_kwargs)``
       Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ..., 'Tags': ...}``
   :param create_source_location_kwargs: If no suitable LocationArn is found,
       a Location will be created if ``create_source_location_kwargs`` is defined.
       ``create_source_location_kwargs`` is then used internally like this:
       ``boto3.create_location_xyz(**create_source_location_kwargs)``
       The xyz is determined from the prefix of source_location_uri, eg ``smb:/...`` or ``s3:/...``
       Example:  ``{'Subdirectory': ..., 'ServerHostname': ..., ...}``
   :param create_destination_location_kwargs: If no suitable LocationArn is found,
       a Location will be created if ``create_destination_location_kwargs`` is defined.
       ``create_destination_location_kwargs`` is used internally like this:
       ``boto3.create_location_xyz(**create_destination_location_kwargs)``
       The xyz is determined from the prefix of destination_location_uri, eg ``smb:/...` or ``s3:/...``
       Example:  ``{'S3BucketArn': ..., 'S3Config': {'BucketAccessRoleArn': ...}, ...}``
   :param update_task_kwargs:  If a suitable TaskArn is found or created,
       it will be updated if ``update_task_kwargs`` is defined.
       ``update_task_kwargs`` is used internally like this:
       ``boto3.update_task(TaskArn=task_arn, **update_task_kwargs)``
       Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ...}``
   :param task_execution_kwargs: Additional kwargs passed directly when starting the
       Task execution, used internally like this:
       ``boto3.start_task_execution(TaskArn=task_arn, **task_execution_kwargs)``
   :param  delete_task_after_execution: If True then the TaskArn which was executed
       will be deleted from AWS DataSync on successful completion.
   :raises AirflowException: If ``task_arn`` was not specified, or if
       either ``source_location_uri`` or ``destination_location_uri`` were
       not specified.
   :raises AirflowException: If source or destination Location were not found
       and could not be created.
   :raises AirflowException: If ``choose_task`` or ``choose_location`` fails.
   :raises AirflowException: If Task creation, update, execution or delete fails.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('task_arn', 'source_location_uri', 'destination_location_uri', 'create_task_kwargs',...



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#44b5e2'



   .. py:method:: hook()

      Create and return DataSyncHook.

      :return DataSyncHook: An DataSyncHook instance.


   .. py:method:: get_hook()

      Create and return DataSyncHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: choose_task(task_arn_list)

      Select 1 DataSync TaskArn from a list.


   .. py:method:: choose_location(location_arn_list)

      Select 1 DataSync LocationArn from a list.


   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.
