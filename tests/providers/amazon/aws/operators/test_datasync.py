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
from __future__ import annotations

from unittest import mock

import boto3
import pytest
from moto import mock_datasync

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.datasync import DataSyncHook
from airflow.providers.amazon.aws.operators.datasync import DataSyncOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = datetime(2018, 1, 1)

SOURCE_HOST_NAME = "airflow.host"
SOURCE_SUBDIR = "airflow_subdir"
DESTINATION_BUCKET_NAME = "airflow_bucket"

SOURCE_LOCATION_URI = f"smb://{SOURCE_HOST_NAME}/{SOURCE_SUBDIR}"
DESTINATION_LOCATION_URI = f"s3://{DESTINATION_BUCKET_NAME}"
DESTINATION_LOCATION_ARN = f"arn:aws:s3:::{DESTINATION_BUCKET_NAME}"
CREATE_TASK_KWARGS = {"Options": {"VerifyMode": "NONE", "Atime": "NONE"}}
UPDATE_TASK_KWARGS = {"Options": {"VerifyMode": "BEST_EFFORT", "Atime": "NONE"}}

MOCK_DATA = {
    "task_id": "test_datasync_task_operator",
    "create_task_id": "test_datasync_create_task_operator",
    "get_task_id": "test_datasync_get_tasks_operator",
    "update_task_id": "test_datasync_update_task_operator",
    "delete_task_id": "test_datasync_delete_task_operator",
    "source_location_uri": SOURCE_LOCATION_URI,
    "destination_location_uri": DESTINATION_LOCATION_URI,
    "create_task_kwargs": CREATE_TASK_KWARGS,
    "update_task_kwargs": UPDATE_TASK_KWARGS,
    "create_source_location_kwargs": {
        "Subdirectory": SOURCE_SUBDIR,
        "ServerHostname": SOURCE_HOST_NAME,
        "User": "airflow",
        "Password": "airflow_password",
        "AgentArns": ["some_agent"],
    },
    "create_destination_location_kwargs": {
        "S3BucketArn": DESTINATION_LOCATION_ARN,
        "S3Config": {"BucketAccessRoleArn": "myrole"},
    },
}


@mock_datasync
@mock.patch.object(DataSyncHook, "get_conn")
class DataSyncTestCaseBase:
    # Runs once for each test
    def setup_method(self, method):
        args = {
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        }

        self.dag = DAG(
            TEST_DAG_ID + "test_schedule_dag_once",
            default_args=args,
            schedule="@once",
        )

        self.client = boto3.client("datasync", region_name="us-east-1")
        self.datasync = None

        self.source_location_arn = self.client.create_location_smb(
            **MOCK_DATA["create_source_location_kwargs"]
        )["LocationArn"]
        self.destination_location_arn = self.client.create_location_s3(
            **MOCK_DATA["create_destination_location_kwargs"]
        )["LocationArn"]
        self.task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

    def teardown_method(self, method):
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])
        # Delete all locations:
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            self.client.delete_location(LocationArn=location["LocationArn"])
        self.client = None


@mock_datasync
@mock.patch.object(DataSyncHook, "get_conn")
class TestDataSyncOperatorCreate(DataSyncTestCaseBase):
    def set_up_operator(
        self,
        task_id="test_datasync_create_task_operator",
        task_arn=None,
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
        allow_random_location_choice=False,
    ):
        # Create operator
        self.datasync = DataSyncOperator(
            task_id=task_id,
            dag=self.dag,
            task_arn=task_arn,
            source_location_uri=source_location_uri,
            destination_location_uri=destination_location_uri,
            create_task_kwargs={"Options": {"VerifyMode": "NONE", "Atime": "NONE"}},
            create_source_location_kwargs={
                "Subdirectory": SOURCE_SUBDIR,
                "ServerHostname": SOURCE_HOST_NAME,
                "User": "airflow",
                "Password": "airflow_password",
                "AgentArns": ["some_agent"],
            },
            create_destination_location_kwargs={
                "S3BucketArn": DESTINATION_LOCATION_ARN,
                "S3Config": {"BucketAccessRoleArn": "myrole"},
            },
            allow_random_location_choice=allow_random_location_choice,
            wait_interval_seconds=0,
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        assert self.datasync.task_id == MOCK_DATA["create_task_id"]
        # Defaults
        assert self.datasync.aws_conn_id == "aws_default"
        assert not self.datasync.allow_random_task_choice
        assert not self.datasync.task_execution_kwargs  # Empty dict
        # Assignments
        assert self.datasync.source_location_uri == MOCK_DATA["source_location_uri"]
        assert self.datasync.destination_location_uri == MOCK_DATA["destination_location_uri"]
        assert self.datasync.create_task_kwargs == MOCK_DATA["create_task_kwargs"]
        assert self.datasync.create_source_location_kwargs == MOCK_DATA["create_source_location_kwargs"]
        assert (
            self.datasync.create_destination_location_kwargs
            == MOCK_DATA["create_destination_location_kwargs"]
        )
        assert not self.datasync.allow_random_location_choice
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with pytest.raises(AirflowException):
            self.set_up_operator(source_location_uri=None)
        with pytest.raises(AirflowException):
            self.set_up_operator(destination_location_uri=None)
        with pytest.raises(AirflowException):
            self.set_up_operator(source_location_uri=None, destination_location_uri=None)
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_create_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 0
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None
        task_arn = result["TaskArn"]

        # Assert 1 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 1
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2

        # Check task metadata
        task = self.client.describe_task(TaskArn=task_arn)
        assert task["Options"] == CREATE_TASK_KWARGS["Options"]
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_create_task_and_location(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])
        # Delete all locations:
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            self.client.delete_location(LocationArn=location["LocationArn"])

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 0
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 0

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None

        # Assert 1 additional task and 2 additional locations
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 1
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_dont_create_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        tasks = self.client.list_tasks()
        tasks_before = len(tasks["Tasks"])

        self.set_up_operator(task_arn=self.task_arn)
        self.datasync.execute(None)

        tasks = self.client.list_tasks()
        tasks_after = len(tasks["Tasks"])
        assert tasks_before == tasks_after
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_create_task_many_locations(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])
        # Create duplicate source location to choose from
        self.client.create_location_smb(**MOCK_DATA["create_source_location_kwargs"])

        self.set_up_operator(task_id="datasync_task1")
        with pytest.raises(AirflowException):
            self.datasync.execute(None)

        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])

        self.set_up_operator(task_id="datasync_task2", allow_random_location_choice=True)
        self.datasync.execute(None)
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_execute_specific_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:
        task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

        self.set_up_operator(task_arn=task_arn)
        result = self.datasync.execute(None)

        assert result["TaskArn"] == task_arn
        assert self.datasync.task_arn == task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()

    @pytest.mark.db_test
    def test_return_value(self, mock_get_conn):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.datasync)
        ti.dag_run = dag_run
        assert self.datasync.execute(ti.get_template_context()) is not None
        # ### Check mocks:
        mock_get_conn.assert_called()


@mock_datasync
@mock.patch.object(DataSyncHook, "get_conn")
class TestDataSyncOperatorGetTasks(DataSyncTestCaseBase):
    def set_up_operator(
        self,
        task_id="test_datasync_get_tasks_operator",
        task_arn=None,
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
        allow_random_task_choice=False,
    ):
        # Create operator
        self.datasync = DataSyncOperator(
            task_id=task_id,
            dag=self.dag,
            task_arn=task_arn,
            source_location_uri=source_location_uri,
            destination_location_uri=destination_location_uri,
            create_source_location_kwargs=MOCK_DATA["create_source_location_kwargs"],
            create_destination_location_kwargs=MOCK_DATA["create_destination_location_kwargs"],
            create_task_kwargs=MOCK_DATA["create_task_kwargs"],
            allow_random_task_choice=allow_random_task_choice,
            wait_interval_seconds=0,
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        assert self.datasync.task_id == MOCK_DATA["get_task_id"]
        # Defaults
        assert self.datasync.aws_conn_id == "aws_default"
        assert not self.datasync.allow_random_location_choice
        # Assignments
        assert self.datasync.source_location_uri == MOCK_DATA["source_location_uri"]
        assert self.datasync.destination_location_uri == MOCK_DATA["destination_location_uri"]
        assert not self.datasync.allow_random_task_choice
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with pytest.raises(AirflowException):
            self.set_up_operator(source_location_uri=None)
        with pytest.raises(AirflowException):
            self.set_up_operator(destination_location_uri=None)
        with pytest.raises(AirflowException):
            self.set_up_operator(source_location_uri=None, destination_location_uri=None)
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_get_no_location(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            self.client.delete_location(LocationArn=location["LocationArn"])

        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 0

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None

        locations = self.client.list_locations()
        assert result is not None
        assert len(locations) == 2
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_get_no_tasks2(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])

        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 0

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_get_one_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Make sure we don't cheat
        self.set_up_operator()
        assert self.datasync.task_arn is None

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 1
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None

        task_arn = result["TaskArn"]
        assert task_arn is not None
        assert task_arn
        assert task_arn == self.task_arn

        # Assert 0 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 1
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_get_many_tasks(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator(task_id="datasync_task1")

        self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 2
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2

        # Execute the task
        with pytest.raises(AirflowException):
            self.datasync.execute(None)

        # Assert 0 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 2
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2

        self.set_up_operator(task_id="datasync_task2", task_arn=self.task_arn, allow_random_task_choice=True)
        self.datasync.execute(None)
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_execute_specific_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:
        task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

        self.set_up_operator(task_arn=task_arn)
        result = self.datasync.execute(None)

        assert result["TaskArn"] == task_arn
        assert self.datasync.task_arn == task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()

    @pytest.mark.db_test
    def test_return_value(self, mock_get_conn):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.datasync)
        ti.dag_run = dag_run
        result = self.datasync.execute(ti.get_template_context())
        assert result["TaskArn"] == self.task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()


@mock_datasync
@mock.patch.object(DataSyncHook, "get_conn")
class TestDataSyncOperatorUpdate(DataSyncTestCaseBase):
    def set_up_operator(
        self, task_id="test_datasync_update_task_operator", task_arn="self", update_task_kwargs="default"
    ):
        if task_arn == "self":
            task_arn = self.task_arn
        if update_task_kwargs == "default":
            update_task_kwargs = {"Options": {"VerifyMode": "BEST_EFFORT", "Atime": "NONE"}}
        # Create operator
        self.datasync = DataSyncOperator(
            task_id=task_id,
            dag=self.dag,
            task_arn=task_arn,
            update_task_kwargs=update_task_kwargs,
            wait_interval_seconds=0,
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        assert self.datasync.task_id == MOCK_DATA["update_task_id"]
        # Defaults
        assert self.datasync.aws_conn_id == "aws_default"
        # Assignments
        assert self.datasync.task_arn == self.task_arn
        assert self.datasync.update_task_kwargs == MOCK_DATA["update_task_kwargs"]
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with pytest.raises(AirflowException):
            self.set_up_operator(task_arn=None)
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_update_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()

        # Check task before update
        task = self.client.describe_task(TaskArn=self.task_arn)
        assert "Options" not in task

        # Execute the task
        result = self.datasync.execute(None)

        assert result is not None
        assert result["TaskArn"] == self.task_arn

        assert self.datasync.task_arn is not None
        # Check it was updated
        task = self.client.describe_task(TaskArn=self.task_arn)
        assert task["Options"] == UPDATE_TASK_KWARGS["Options"]
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_execute_specific_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:
        task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

        self.set_up_operator(task_arn=task_arn)
        result = self.datasync.execute(None)

        assert result["TaskArn"] == task_arn
        assert self.datasync.task_arn == task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()

    @pytest.mark.db_test
    def test_return_value(self, mock_get_conn):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.datasync)
        ti.dag_run = dag_run
        result = self.datasync.execute(ti.get_template_context())
        assert result["TaskArn"] == self.task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()


@mock_datasync
@mock.patch.object(DataSyncHook, "get_conn")
class TestDataSyncOperator(DataSyncTestCaseBase):
    def set_up_operator(
        self, task_id="test_datasync_task_operator", task_arn="self", wait_for_completion=True
    ):
        if task_arn == "self":
            task_arn = self.task_arn
        # Create operator
        self.datasync = DataSyncOperator(
            task_id=task_id,
            dag=self.dag,
            wait_interval_seconds=0,
            wait_for_completion=wait_for_completion,
            task_arn=task_arn,
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        assert self.datasync.task_id == MOCK_DATA["task_id"]
        # Defaults
        assert self.datasync.aws_conn_id == "aws_default"
        assert self.datasync.wait_interval_seconds == 0
        # Assignments
        assert self.datasync.task_arn == self.task_arn
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with pytest.raises(AirflowException):
            self.set_up_operator(task_arn=None)
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_execute_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Configure the Operator with the specific task_arn
        self.set_up_operator()
        assert self.datasync.task_arn == self.task_arn

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        len_tasks_before = len(tasks["Tasks"])
        locations = self.client.list_locations()
        len_locations_before = len(locations["Locations"])

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None
        task_execution_arn = result["TaskExecutionArn"]
        assert task_execution_arn is not None

        # Assert 0 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == len_tasks_before
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == len_locations_before

        # Check with the DataSync client what happened
        task_execution = self.client.describe_task_execution(TaskExecutionArn=task_execution_arn)
        assert task_execution["Status"] == "SUCCESS"

        # Insist that this specific task was executed, not anything else
        task_execution_arn = task_execution["TaskExecutionArn"]
        # format of task_execution_arn:
        # arn:aws:datasync:us-east-1:111222333444:task/task-00000000000000003/execution/exec-00000000000000004
        # format of task_arn:
        # arn:aws:datasync:us-east-1:111222333444:task/task-00000000000000003
        assert "/".join(task_execution_arn.split("/")[:2]) == self.task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()

    @mock.patch.object(DataSyncHook, "wait_for_task_execution")
    def test_execute_task_without_wait_for_completion(self, mock_wait, mock_get_conn):
        self.set_up_operator(wait_for_completion=False)

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None
        task_execution_arn = result["TaskExecutionArn"]
        assert task_execution_arn is not None

        mock_wait.assert_not_called()

    @mock.patch.object(DataSyncHook, "wait_for_task_execution")
    def test_failed_task(self, mock_wait, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        mock_wait.return_value = False
        # ### Begin tests:

        self.set_up_operator()

        # Execute the task
        with pytest.raises(AirflowException):
            self.datasync.execute(None)
        # ### Check mocks:
        mock_get_conn.assert_called()

    @mock.patch.object(DataSyncHook, "wait_for_task_execution")
    def test_killed_task(self, mock_wait, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Kill the task when doing wait_for_task_execution
        def kill_task(*args, **kwargs):
            self.datasync.on_kill()
            return True

        mock_wait.side_effect = kill_task

        self.set_up_operator()

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None

        task_execution_arn = result["TaskExecutionArn"]
        assert task_execution_arn is not None

        # Verify the task was killed
        task = self.client.describe_task(TaskArn=self.task_arn)
        assert task["Status"] == "AVAILABLE"
        task_execution = self.client.describe_task_execution(TaskExecutionArn=task_execution_arn)
        assert task_execution["Status"] == "ERROR"
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_execute_specific_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:
        task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

        self.set_up_operator(task_arn=task_arn)
        result = self.datasync.execute(None)

        assert result["TaskArn"] == task_arn
        assert self.datasync.task_arn == task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()

    @pytest.mark.db_test
    def test_return_value(self, mock_get_conn):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.datasync)
        ti.dag_run = dag_run
        assert self.datasync.execute(ti.get_template_context()) is not None
        # ### Check mocks:
        mock_get_conn.assert_called()


@mock_datasync
@mock.patch.object(DataSyncHook, "get_conn")
class TestDataSyncOperatorDelete(DataSyncTestCaseBase):
    def set_up_operator(self, task_id="test_datasync_delete_task_operator", task_arn="self"):
        if task_arn == "self":
            task_arn = self.task_arn
        # Create operator
        self.datasync = DataSyncOperator(
            task_id=task_id,
            dag=self.dag,
            task_arn=task_arn,
            delete_task_after_execution=True,
            wait_interval_seconds=0,
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        assert self.datasync.task_id == MOCK_DATA["delete_task_id"]
        # Defaults
        assert self.datasync.aws_conn_id == "aws_default"
        # Assignments
        assert self.datasync.task_arn == self.task_arn
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with pytest.raises(AirflowException):
            self.set_up_operator(task_arn=None)
        # ### Check mocks:
        mock_get_conn.assert_not_called()

    def test_delete_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 1
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2

        # Execute the task
        result = self.datasync.execute(None)
        assert result is not None
        assert result["TaskArn"] == self.task_arn

        # Assert -1 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 0
        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 2
        # ### Check mocks:
        mock_get_conn.assert_called()

    def test_execute_specific_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:
        task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

        self.set_up_operator(task_arn=task_arn)
        result = self.datasync.execute(None)

        assert result["TaskArn"] == task_arn
        assert self.datasync.task_arn == task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()

    @pytest.mark.db_test
    def test_return_value(self, mock_get_conn):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.datasync)
        ti.dag_run = dag_run
        result = self.datasync.execute(ti.get_template_context())
        assert result["TaskArn"] == self.task_arn
        # ### Check mocks:
        mock_get_conn.assert_called()
