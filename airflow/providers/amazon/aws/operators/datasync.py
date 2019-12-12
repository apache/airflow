# -*- coding: utf-8 -*-
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
Get, Create, Update, Delete and execute an AWS DataSync Task.
"""

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.datasync import AWSDataSyncHook
from airflow.utils.decorators import apply_defaults


class AWSDataSyncCreateTaskOperator(BaseOperator):
    r"""Create an AWS DataSync Task.

    If there are existing Locations which match the specified
    source and destination URIs then these will be used for the Task.
    Otherwise, new Locations can be created automatically,
    depending on input parameters.

    If ``do_xcom_push`` is True, the TaskArn which is created
    will be pushed to an XCom.

    :param str aws_conn_id: AWS connection to use.
    :param str source_location_uri: Source location URI.
        Example: ``smb://server/subdir``
    :param str destination_location_uri: Destination location URI.
        Example: ``s3://airflow_bucket/stuff``
    :param bool case_sensitive_location_search: Whether or not to do a
        case-sensitive search for each Location URI.
    :param dict create_task_kwargs: If no suitable TaskArn is found,
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

    :raises AirflowException: If neither ``source_location_uri`` nor
        ``destination_location_uri`` were specified.
    :raises AirflowException: If source or destination Location weren't found
        and could not be created.
    :raises AirflowException: If Task creation fails.
    """
    template_fields = ('source_location_uri',
                       'destination_location_uri')
    ui_color = '#44b5e2'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id='aws_default',
        source_location_uri=None,
        destination_location_uri=None,
        case_sensitive_location_search=True,
        create_task_kwargs=None,
        create_source_location_kwargs=None,
        create_destination_location_kwargs=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.source_location_uri = source_location_uri
        self.destination_location_uri = destination_location_uri
        self.case_sensitive_location_search = case_sensitive_location_search
        if create_task_kwargs:
            self.create_task_kwargs = create_task_kwargs
        else:
            self.create_task_kwargs = dict()
        if create_source_location_kwargs:
            self.create_source_location_kwargs = create_source_location_kwargs
        else:
            self.create_source_location_kwargs = dict()
        if create_destination_location_kwargs:
            self.create_destination_location_kwargs = create_destination_location_kwargs
        else:
            self.create_destination_location_kwargs = dict()

        # Validations
        if not (self.source_location_uri and self.destination_location_uri):
            raise AirflowException(
                'Specify both source_location_uri and destination_location_uri')

        # Others
        self.hook = None
        self.source_location_arn = None
        self.destination_location_arn = None
        self.task_arn = None

    def get_hook(self):
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if not self.hook:
            self.hook = AWSDataSyncHook(
                aws_conn_id=self.aws_conn_id
            )
        return self.hook

    def _create_or_get_location_arn(
            self,
            location_uri,
            **create_location_kwargs):
        location_arns = self.get_hook().get_location_arns(
            location_uri,
            self.case_sensitive_location_search)
        if not location_arns:
            self.log.info('Creating location for LocationUri %s',
                          location_uri)
            location_arn = self.get_hook().create_location(
                location_uri,
                **create_location_kwargs)
            self.log.info(
                'Created a Location with LocationArn %s', location_arn)
            return location_arn
        else:
            self.log.info('Found LocationArns %s for LocationUri %s',
                          location_arns, location_uri)
            if len(location_arns) != 1:
                raise AirflowException(
                    'More than 1 LocationArn was found for LocationUri %s' % location_uri)
            return location_arns[0]

    def execute(self, context):
        """Create a new Task (and Locations) if necessary."""
        hook = self.get_hook()

        self.source_location_arn = self._create_or_get_location_arn(
            self.source_location_uri,
            **self.create_source_location_kwargs
        )

        self.destination_location_arn = self._create_or_get_location_arn(
            self.destination_location_uri,
            **self.create_destination_location_kwargs
        )

        self.log.info('Creating a Task.')
        self.task_arn = hook.create_task(
            self.source_location_arn,
            self.destination_location_arn,
            **self.create_task_kwargs
        )
        if not self.task_arn:
            raise AirflowException('Task could not be created')
        self.log.info('Created a Task with TaskArn %s', self.task_arn)
        return self.task_arn


class AWSDataSyncGetTasksOperator(BaseOperator):
    r"""Get AWS DataSync Tasks.

    Finds AWS DataSync Tasks which have a source and destination
    location corresponding to the specified source and destination
    URIs.

    If ``do_xcom_push`` is True, the TaskArns which are found
    will be pushed to an XCom.

    note:: There may be 0, 1, or many matching Tasks. The calling
        application will need to deal with these scenarios.

    :param str aws_conn_id: AWS connection to use.
    :param str source_location_uri: Source location URI.
        Example: ``smb://server/subdir``
    :param str destination_location_uri: Destination location URI.
        Example: ``s3://airflow_bucket/stuff``
    :param bool case_sensitive_location_search: Whether or not to do a
        case-sensitive search for each Location URI.

    :raises AirflowException: If neither ``source_location_uri`` nor
        ``destination_location_uri`` were specified.
    """
    template_fields = ('source_location_uri',
                       'destination_location_uri')
    ui_color = '#44b5e2'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id='aws_default',
        source_location_uri=None,
        destination_location_uri=None,
        case_sensitive_location_search=True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.source_location_uri = source_location_uri
        self.destination_location_uri = destination_location_uri
        self.case_sensitive_location_search = case_sensitive_location_search

        # Validations
        if not (self.source_location_uri and self.destination_location_uri):
            raise AirflowException(
                'Specify both source_location_uri and destination_location_uri')

        # Others
        self.hook = None
        self.source_location_arns = None
        self.destination_location_arns = None
        self.task_arns = None

    def get_hook(self):
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if not self.hook:
            self.hook = AWSDataSyncHook(
                aws_conn_id=self.aws_conn_id
            )
        return self.hook

    def _get_location_arns(
            self, location_uri
    ):
        location_arns = self.get_hook().get_location_arns(
            location_uri,
            self.case_sensitive_location_search)
        self.log.info('Found LocationArns %s for LocationUri %s',
                      location_arns, location_uri)
        return location_arns

    def execute(self, context):
        """Create a new Task (and Locations) if necessary."""
        hook = self.get_hook()

        self.source_location_arns = self._get_location_arns(
            self.source_location_uri
        )

        self.destination_location_arns = self._get_location_arns(
            self.destination_location_uri
        )

        if not (self.source_location_arns and self.destination_location_arns):
            self.log.info('Insufficient Locations to search for Task')
            return []

        self.log.info('Searching for TaskArns')
        self.task_arns = hook.get_task_arns_for_location_arns(
            self.source_location_arns,
            self.destination_location_arns)
        self.log.info('Found %s matching TaskArns', len(self.task_arns))
        return self.task_arns


class AWSDataSyncUpdateTaskOperator(BaseOperator):
    """
    Update an AWS DataSyncTask

    If ``do_xcom_push`` is True, the TaskArns which were updated
    will be pushed to an XCom.

    :param str aws_conn_id: AWS connection to use.
    :param str task_arn: The TaskArn to update. If ``None``, the operator will
        look in xcom_pull for a TaskArn.
    :param dict update_task_kwargs: The TaskArn will be updated with ``update_task_kwargs``.
        ``update_task_kwargs`` is used internally like this:
        ``boto3.update_task(TaskArn=task_arn, **update_task_kwargs)``
        Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ...}``
    :raises AirflowException: If ``task_arn`` is None.
    :raises AirflowException: If ``update_task_kwargs`` is None.
    """
    template_fields = ('task_arn',)
    ui_color = '#44b5e2'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id='aws_default',
        task_arn=None,
        update_task_kwargs=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.task_arn = task_arn
        self.update_task_kwargs = update_task_kwargs

        # Validations
        if not self.task_arn:
            raise AirflowException(
                'task_arn must be specified')

        if not self.update_task_kwargs:
            raise AirflowException(
                'update_task_kwargs must be specified')

        # Others
        self.hook = None

    def get_hook(self):
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if not self.hook:
            self.hook = AWSDataSyncHook(
                aws_conn_id=self.aws_conn_id,
            )
        return self.hook

    def execute(self, context):
        hook = self.get_hook()
        self.log.info('Updating TaskArn %s', self.task_arn)
        hook.update_task(self.task_arn, **self.update_task_kwargs)
        self.log.info('Updated TaskArn %s', self.task_arn)
        return self.task_arn


class AWSDataSyncTaskOperator(BaseOperator):
    r"""
    An operator that starts an AWS DataSync TaskExecution for a Task.

    If ``do_xcom_push`` is True, the TaskExecutionArn used
    will be pushed to an XCom when it successfuly completes.

    :param str aws_conn_id: AWS connection to use.
    :param int wait_interval_seconds: Time to wait between two consecutive calls
        to check TaskExecution status.
    :param str task_arn: The TaskArn to start. If ``None``, the operator will
        look in xcom_pull for a TaskArn.
        Example: ``arn:aws:datasync:eu-west-1:111122233444:task/task-0dfafff11dc43f1ab``

    :raises AirflowException: If neither ``task_arn`` nor
        (``source_location_uri`` and ``destination_location_uri``) is specified.
    :raises AirflowException: If ``task_arn`` is None.
    :raises AirflowException: If ``task_arn`` is None and TaskArn was not found in xcom_pull.
    :raises AirflowException: If TaskExecution fails.
    """

    template_fields = ('task_arn',)
    ui_color = '#44b5e2'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id='aws_default',
        wait_interval_seconds=30,
        task_arn=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.wait_interval_seconds = wait_interval_seconds
        self.task_arn = task_arn

        # Validations
        if not self.task_arn:
            raise AirflowException(
                'task_arn must be specified')

        # Others
        self.hook = None
        self.task_execution_arn = None

    def get_hook(self):
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if not self.hook:
            self.hook = AWSDataSyncHook(
                aws_conn_id=self.aws_conn_id,
                wait_interval_seconds=self.wait_interval_seconds
            )
        return self.hook

    def execute(self, context):
        """Create and monitor an AWSDataSync TaskExecution for a Task."""
        hook = self.get_hook()

        # Create a task execution:
        self.log.info('Starting execution for TaskArn %s', self.task_arn)
        self.task_execution_arn = hook.start_task_execution(self.task_arn)
        self.log.info('Started TaskExecutionArn %s', self.task_execution_arn)

        # Wait for task execution to complete
        self.log.info('Waiting for TaskExecutionArn %s',
                      self.task_execution_arn)
        result = hook.wait_for_task_execution(self.task_execution_arn)
        self.log.info('Completed TaskExecutionArn %s', self.task_execution_arn)
        if not result:
            raise AirflowException(
                'Failed TaskExecutionArn %s' % self.task_execution_arn)
        return self.task_execution_arn

    def on_kill(self):
        """Cancel the submitted DataSync task."""
        hook = self.get_hook()
        if self.task_execution_arn:
            self.log.info('Cancelling TaskExecutionArn %s',
                          self.task_execution_arn)
            hook.cancel_task_execution(
                task_execution_arn=self.task_execution_arn)
            self.log.info('Cancelled TaskExecutionArn %s',
                          self.task_execution_arn)


class AWSDataSyncDeleteTaskOperator(BaseOperator):
    r"""
    An operator that deletes an AWS DataSync Task.

    If ``do_xcom_push`` is True, the TaskArn used
    will be pushed to an XCom when it successfuly completes.

    :param str aws_conn_id: AWS connection to use.
    :param str task_arn: The TaskArn to start. If ``None``, the operator will
        look in xcom_pull for a TaskArn.
        Example: ``arn:aws:datasync:eu-west-1:111122233444:task/task-0dfafff11dc43f1ab``

    :raises AirflowException: If ``task_arn`` is None.
    :raises AirflowException: If ``task_arn`` is None and TaskArn was not found in xcom_pull.
    :raises AirflowException: If Task deletion fails.
    """

    template_fields = ('task_arn',)
    ui_color = '#44b5e2'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id='aws_default',
        task_arn=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.task_arn = task_arn

        # Validations
        if not self.task_arn:
            raise AirflowException(
                'task_arn must be specified')

        # Others
        self.hook = None

    def get_hook(self):
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if not self.hook:
            self.hook = AWSDataSyncHook(
                aws_conn_id=self.aws_conn_id,
            )
        return self.hook

    def execute(self, context):
        """Deletes an AWS DataSync Task."""
        hook = self.get_hook()
        # Delete task:
        self.log.info('Deleting Task with TaskArn %s', self.task_arn)
        hook.delete_task(self.task_arn)
        self.log.info('Task Deleted')
        return self.task_arn
