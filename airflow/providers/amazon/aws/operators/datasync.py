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
Create, get, update, execute and delete an AWS DataSync Task.
"""

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.datasync import AWSDataSyncHook
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes, too-many-arguments
class AWSDataSyncOperator(BaseOperator):
    r"""Find, Create, Update, Execute and Delete AWS DataSync Tasks.

    If ``do_xcom_push`` is True, then the TaskArn and TaskExecutionArn which
    were executed will be pushed to an XCom.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AWSDataSyncOperator`

    .. note:: There may be 0, 1, or many existing DataSync Tasks. The default
        behavior is to create a new Task if there are 0, or execute the Task
        if there was 1 Task, or fail if there were many Tasks.

    :param str aws_conn_id: AWS connection to use.
    :param int wait_for_task_execution: Time to wait between two
        consecutive calls to check TaskExecution status.
    :param str task_arn: AWS DataSync TaskArn to use. If None, then this operator will
        attempt to either search for an existing Task or create a new Task.
    :param str source_location_uri: Source location URI to search for. All DataSync
        Tasks with a LocationArn with this URI will be considered.
        Example: ``smb://server/subdir``
    :param str destination_location_uri: Destination location URI to search for.
        All DataSync Tasks with a LocationArn with this URI will be considered.
        Example: ``s3://airflow_bucket/stuff``
    :param bool location_search_case_sensitive: Whether or not to do a
        case-sensitive search for each Location URI.
    :param bool location_search_ignore_trailing_slash: Whether or not to strip
        trailing / from locations when comparing them.
    :param choose_task_callable: Optional callable which is passed a list
        of 0, 1 or many DataSync TaskArns and returns 1 to be used.
        If set to None, then default behaviour is to raise ``AirflowException``
        if more than one Task was identified.
    :param choose_location_callable: Optional callable which is passed a list
        of 0, 1 or many DataSync LocationArns and returns 1 to be used.
        If set to None, then default behaviour is to raise ``AirflowException``
        if more than one Location was identified.
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

    :raises AirflowException: If ``task_arn`` was not specified, or if
        either ``source_location_uri`` or ``destination_location_uri`` were
        not specified.
    :raises AirflowException: If source or destination Location weren't found
        and could not be created.
    :raises AirflowException: If Task creation, update, execution or delete fails.
    """
    template_fields = (
        "task_arn",
        "source_location_uri",
        "destination_location_uri",
        "create_task_kwargs",
        "create_source_location_kwargs",
        "create_destination_location_kwargs",
        "update_task_kwargs",
    )
    ui_color = "#44b5e2"

    @apply_defaults
    def __init__(
        self,
        aws_conn_id="aws_default",
        wait_interval_seconds=0,
        task_arn=None,
        source_location_uri=None,
        destination_location_uri=None,
        location_search_case_sensitive=True,
        location_search_ignore_trailing_slash=True,
        choose_task_callable=None,
        choose_location_callable=None,
        create_task_kwargs=None,
        create_source_location_kwargs=None,
        create_destination_location_kwargs=None,
        update_task_kwargs=None,
        delete_task_after_execution=False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.wait_interval_seconds = wait_interval_seconds

        self.task_arn = task_arn

        self.source_location_uri = source_location_uri
        self.destination_location_uri = destination_location_uri

        self.location_search_case_sensitive = location_search_case_sensitive
        self.location_search_ignore_trailing_slash = (
            location_search_ignore_trailing_slash
        )

        self.choose_task_callable = choose_task_callable
        self.choose_location_callable = choose_location_callable

        self.create_task_kwargs = create_task_kwargs if create_task_kwargs else dict()
        self.create_source_location_kwargs = dict()
        if create_source_location_kwargs:
            self.create_source_location_kwargs = create_source_location_kwargs
        self.create_destination_location_kwargs = dict()
        if create_destination_location_kwargs:
            self.create_destination_location_kwargs = create_destination_location_kwargs

        self.update_task_kwargs = update_task_kwargs if update_task_kwargs else dict()
        self.delete_task_after_execution = delete_task_after_execution

        # Validations
        valid = False
        if self.task_arn:
            valid = True
        if self.source_location_uri and self.destination_location_uri:
            valid = True
        if not valid:
            raise AirflowException(
                "Specify task_arn or both source_location_uri and destination_location_uri"
            )

        # Others
        self.hook = None
        # Candidates - these are found in AWS as possible things
        # for us to use
        self.candidate_source_location_arns = None
        self.candidate_destination_location_arns = None
        self.candidate_task_arns = None
        # Actuals
        self.source_location_arn = None
        self.destination_location_arn = None
        self.task_execution_arn = None

    def get_hook(self):
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if not self.hook:
            self.hook = AWSDataSyncHook(
                aws_conn_id=self.aws_conn_id,
                wait_interval_seconds=self.wait_interval_seconds,
            )
        return self.hook

    def execute(self, context):
        # If task_arn was not specified then try to
        # find 0, 1 or many candidate DataSync Tasks to run
        if not self.task_arn:
            self._get_tasks_and_locations()

        # If some were found, identify which one to run
        if self.candidate_task_arns:
            self.task_arn = self._choose_task_from_list(self.candidate_task_arns)

        # If we couldnt find one then try create one
        if not self.task_arn and self.create_task_kwargs:
            self._create_datasync_task()

        if not self.task_arn:
            raise AirflowException("DataSync TaskArn could be identified or created.")

        self.log.info("Using DataSync TaskArn %s", self.task_arn)

        # Update the DataSync Task
        if self.update_task_kwargs:
            self._update_datasync_task()

        # Execute the DataSync Task
        self._execute_datasync_task()

        if not self.task_execution_arn:
            raise AirflowException("Nothing was executed")

        # Delete the DataSyncTask
        if self.delete_task_after_execution:
            self._delete_datasync_task()

        return {"TaskArn": self.task_arn, "TaskExecutionArn": self.task_execution_arn}

    def _get_tasks_and_locations(self):
        """Find existing DataSync Task based on source and dest Locations."""
        hook = self.get_hook()

        self.candidate_source_location_arns = self._get_location_arns(
            self.source_location_uri
        )

        self.candidate_destination_location_arns = self._get_location_arns(
            self.destination_location_uri
        )

        if not self.candidate_source_location_arns:
            self.log.info("No matching source Locations")
            return

        if not self.candidate_destination_location_arns:
            self.log.info("No matching destination Locations")
            return

        self.log.info("Finding DataSync TaskArns that have these LocationArns")
        self.candidate_task_arns = hook.get_task_arns_for_location_arns(
            self.candidate_source_location_arns,
            self.candidate_destination_location_arns,
        )
        self.log.info("Found candidate DataSync TaskArns %s", self.candidate_task_arns)

    def _choose_task_from_list(self, task_arn_list):
        """Select 1 DataSync TaskArn from a list"""
        if self.choose_task_callable:
            return self.choose_task_callable(task_arn_list)
        if not task_arn_list:
            return None
        if len(task_arn_list) == 1:
            return task_arn_list[0]
        raise AirflowException("Too many DataSync Tasks to choose from.")

    def _choose_location_from_list(self, location_arn_list):
        """Select 1 DataSync LocationArn from a list"""
        if self.choose_location_callable:
            return self.choose_location_callable(location_arn_list)
        if not location_arn_list:
            return None
        if len(location_arn_list) == 1:
            return location_arn_list[0]
        raise AirflowException("Too many DataSync Locations to choose from.")

    def _create_datasync_task(self):
        """Create a AWS DataSyncTask."""
        hook = self.get_hook()

        self.source_location_arn = self._choose_location_from_list(
            self.candidate_source_location_arns
        )
        if not self.source_location_arn:
            self.source_location_arn = hook.create_location(
                self.source_location_uri, **self.create_source_location_kwargs
            )
        if not self.source_location_arn:
            raise AirflowException("Unable to determine source_location_arn")

        self.destination_location_arn = self._choose_location_from_list(
            self.candidate_destination_location_arns
        )
        if not self.destination_location_arn:
            self.destination_location_arn = hook.create_location(
                self.destination_location_uri, **self.create_destination_location_kwargs
            )
        if not self.destination_location_arn:
            raise AirflowException("Unable to determine destination_location_arn")

        self.log.info("Creating a Task.")
        self.task_arn = hook.create_task(
            self.source_location_arn,
            self.destination_location_arn,
            **self.create_task_kwargs
        )
        if not self.task_arn:
            raise AirflowException("Task could not be created")
        self.log.info("Created a Task with TaskArn %s", self.task_arn)
        return self.task_arn

    def _update_datasync_task(self):
        """Update a AWS DataSyncTask."""
        hook = self.get_hook()
        self.log.info("Updating TaskArn %s", self.task_arn)
        hook.update_task(self.task_arn, **self.update_task_kwargs)
        self.log.info("Updated TaskArn %s", self.task_arn)
        return self.task_arn

    def _execute_datasync_task(self):
        """Create and monitor an AWSDataSync TaskExecution for a Task."""
        hook = self.get_hook()

        # Create a task execution:
        self.log.info("Starting execution for TaskArn %s", self.task_arn)
        self.task_execution_arn = hook.start_task_execution(self.task_arn)
        self.log.info("Started TaskExecutionArn %s", self.task_execution_arn)

        # Wait for task execution to complete
        self.log.info("Waiting for TaskExecutionArn %s", self.task_execution_arn)
        result = hook.wait_for_task_execution(self.task_execution_arn)
        self.log.info("Completed TaskExecutionArn %s", self.task_execution_arn)
        if not result:
            raise AirflowException(
                "Failed TaskExecutionArn %s" % self.task_execution_arn
            )
        return self.task_execution_arn

    def on_kill(self):
        """Cancel the submitted DataSync task."""
        hook = self.get_hook()
        if self.task_execution_arn:
            self.log.info("Cancelling TaskExecutionArn %s", self.task_execution_arn)
            hook.cancel_task_execution(task_execution_arn=self.task_execution_arn)
            self.log.info("Cancelled TaskExecutionArn %s", self.task_execution_arn)

    def _delete_datasync_task(self):
        """Deletes an AWS DataSync Task."""
        hook = self.get_hook()
        # Delete task:
        self.log.info("Deleting Task with TaskArn %s", self.task_arn)
        hook.delete_task(self.task_arn)
        self.log.info("Task Deleted")
        return self.task_arn

    def _get_location_arns(self, location_uri):
        location_arns = self.get_hook().get_location_arns(
            location_uri,
            self.location_search_case_sensitive,
            self.location_search_ignore_trailing_slash,
        )
        self.log.info(
            "Found LocationArns %s for LocationUri %s", location_arns, location_uri
        )
        return location_arns
