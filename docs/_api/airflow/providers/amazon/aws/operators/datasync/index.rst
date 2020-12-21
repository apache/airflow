:mod:`airflow.providers.amazon.aws.operators.datasync`
======================================================

.. py:module:: airflow.providers.amazon.aws.operators.datasync

.. autoapi-nested-parse::

   Create, get, update, execute and delete an AWS DataSync Task.



Module Contents
---------------

.. py:class:: AWSDataSyncOperator(*, aws_conn_id: str = 'aws_default', wait_interval_seconds: int = 5, task_arn: Optional[str] = None, source_location_uri: Optional[str] = None, destination_location_uri: Optional[str] = None, allow_random_task_choice: bool = False, allow_random_location_choice: bool = False, create_task_kwargs: Optional[dict] = None, create_source_location_kwargs: Optional[dict] = None, create_destination_location_kwargs: Optional[dict] = None, update_task_kwargs: Optional[dict] = None, task_execution_kwargs: Optional[dict] = None, delete_task_after_execution: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Find, Create, Update, Execute and Delete AWS DataSync Tasks.

   If ``do_xcom_push`` is True, then the DataSync TaskArn and TaskExecutionArn
   which were executed will be pushed to an XCom.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AWSDataSyncOperator`

   .. note:: There may be 0, 1, or many existing DataSync Tasks defined in your AWS
       environment. The default behavior is to create a new Task if there are 0, or
       execute the Task if there was 1 Task, or fail if there were many Tasks.

   :param str aws_conn_id: AWS connection to use.
   :param int wait_interval_seconds: Time to wait between two
       consecutive calls to check TaskExecution status.
   :param str task_arn: AWS DataSync TaskArn to use. If None, then this operator will
       attempt to either search for an existing Task or attempt to create a new Task.
   :param str source_location_uri: Source location URI to search for. All DataSync
       Tasks with a LocationArn with this URI will be considered.
       Example: ``smb://server/subdir``
   :param str destination_location_uri: Destination location URI to search for.
       All DataSync Tasks with a LocationArn with this URI will be considered.
       Example: ``s3://airflow_bucket/stuff``
   :param bool allow_random_task_choice: If multiple Tasks match, one must be chosen to
       execute. If allow_random_task_choice is True then a random one is chosen.
   :param bool allow_random_location_choice: If multiple Locations match, one must be chosen
       when creating a task. If allow_random_location_choice is True then a random one is chosen.
   :param dict create_task_kwargs: If no suitable TaskArn is identified,
       it will be created if ``create_task_kwargs`` is defined.
       ``create_task_kwargs`` is then used internally like this:
       ``boto3.create_task(**create_task_kwargs)``
       Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ..., 'Tags': ...}``
   :param dict create_source_location_kwargs: If no suitable LocationArn is found,
       a Location will be created if ``create_source_location_kwargs`` is defined.
       ``create_source_location_kwargs`` is then used internally like this:
       ``boto3.create_location_xyz(**create_source_location_kwargs)``
       The xyz is determined from the prefix of source_location_uri, eg ``smb:/...`` or ``s3:/...``
       Example:  ``{'Subdirectory': ..., 'ServerHostname': ..., ...}``
   :param dict create_destination_location_kwargs: If no suitable LocationArn is found,
       a Location will be created if ``create_destination_location_kwargs`` is defined.
       ``create_destination_location_kwargs`` is used internally like this:
       ``boto3.create_location_xyz(**create_destination_location_kwargs)``
       The xyz is determined from the prefix of destination_location_uri, eg ``smb:/...` or ``s3:/...``
       Example:  ``{'S3BucketArn': ..., 'S3Config': {'BucketAccessRoleArn': ...}, ...}``
   :param dict update_task_kwargs:  If a suitable TaskArn is found or created,
       it will be updated if ``update_task_kwargs`` is defined.
       ``update_task_kwargs`` is used internally like this:
       ``boto3.update_task(TaskArn=task_arn, **update_task_kwargs)``
       Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ...}``
   :param dict task_execution_kwargs: Additional kwargs passed directly when starting the
       Task execution, used internally like this:
       ``boto3.start_task_execution(TaskArn=task_arn, **task_execution_kwargs)``
   :param bool delete_task_after_execution: If True then the TaskArn which was executed
       will be deleted from AWS DataSync on successful completion.
   :raises AirflowException: If ``task_arn`` was not specified, or if
       either ``source_location_uri`` or ``destination_location_uri`` were
       not specified.
   :raises AirflowException: If source or destination Location were not found
       and could not be created.
   :raises AirflowException: If ``choose_task`` or ``choose_location`` fails.
   :raises AirflowException: If Task creation, update, execution or delete fails.

   .. attribute:: template_fields
      :annotation: = ['task_arn', 'source_location_uri', 'destination_location_uri', 'create_task_kwargs', 'create_source_location_kwargs', 'create_destination_location_kwargs', 'update_task_kwargs', 'task_execution_kwargs']

      

   .. attribute:: ui_color
      :annotation: = #44b5e2

      

   
   .. method:: get_hook(self)

      Create and return AWSDataSyncHook.

      :return AWSDataSyncHook: An AWSDataSyncHook instance.



   
   .. method:: execute(self, context)



   
   .. method:: _get_tasks_and_locations(self)

      Find existing DataSync Task based on source and dest Locations.



   
   .. method:: choose_task(self, task_arn_list: list)

      Select 1 DataSync TaskArn from a list



   
   .. method:: choose_location(self, location_arn_list: List[str])

      Select 1 DataSync LocationArn from a list



   
   .. method:: _create_datasync_task(self)

      Create a AWS DataSyncTask.



   
   .. method:: _update_datasync_task(self)

      Update a AWS DataSyncTask.



   
   .. method:: _execute_datasync_task(self)

      Create and monitor an AWSDataSync TaskExecution for a Task.



   
   .. method:: on_kill(self)

      Cancel the submitted DataSync task.



   
   .. method:: _delete_datasync_task(self)

      Deletes an AWS DataSync Task.



   
   .. method:: _get_location_arns(self, location_uri)




