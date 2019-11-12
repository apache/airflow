# -*- coding: utf-8 -*-
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

import unittest
from unittest import mock

import boto3

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.amazon.aws.hooks.datasync import AWSDataSyncHook
from airflow.providers.amazon.aws.operators.datasync import (
    AWSDataSyncCreateTaskOperator, AWSDataSyncDeleteTaskOperator, AWSDataSyncGetTasksOperator,
    AWSDataSyncTaskOperator, AWSDataSyncUpdateTaskOperator,
)
from airflow.utils import timezone
from airflow.utils.timezone import datetime


def no_datasync(x):
    return x


try:
    from moto import mock_datasync
except ImportError:
    mock_datasync = no_datasync

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2018, 1, 1)

SOURCE_HOST_NAME = 'airflow.host'
SOURCE_SUBDIR = 'airflow_subdir'
DESTINATION_BUCKET_NAME = 'airflow_bucket'

SOURCE_LOCATION_URI = 'smb://{0}/{1}'.format(SOURCE_HOST_NAME, SOURCE_SUBDIR)
DESTINATION_LOCATION_URI = 's3://{0}'.format(DESTINATION_BUCKET_NAME)
DESTINATION_LOCATION_ARN = 'arn:aws:s3:::{0}'.format(DESTINATION_BUCKET_NAME)
CREATE_TASK_KWARGS = {'Options': {'VerifyMode': 'NONE', 'Atime': 'NONE'}}
UPDATE_TASK_KWARGS = {'Options': {
    'VerifyMode': 'BEST_EFFORT', 'Atime': 'NONE'}}

MOCK_DATA = {
    'task_id': 'test_aws_datasync_task_operator',
    'create_task_id': 'test_aws_datasync_create_task_operator',
    'get_task_id': 'test_aws_datasync_get_tasks_operator',
    'update_task_id': 'test_aws_datasync_update_task_operator',
    'delete_task_id': 'test_aws_datasync_delete_task_operator',
    'source_location_uri': SOURCE_LOCATION_URI,
    'destination_location_uri': DESTINATION_LOCATION_URI,
    'case_sensitive_location_search': True,
    'create_task_kwargs': CREATE_TASK_KWARGS,
    'update_task_kwargs': UPDATE_TASK_KWARGS,
    'create_source_location_kwargs': {
        'Subdirectory': SOURCE_SUBDIR,
        'ServerHostname': SOURCE_HOST_NAME,
        'User': 'airflow',
        'Password': 'airflow_password',
        'AgentArns': ['some_agent']
    },
    'create_destination_location_kwargs': {
        'S3BucketArn': DESTINATION_LOCATION_ARN,
        'S3Config': {'BucketAccessRoleArn': 'myrole'}
    },
}


@mock_datasync
@mock.patch.object(AWSDataSyncHook, 'get_conn')
@unittest.skipIf(mock_datasync == no_datasync, 'moto datasync package missing')  # pylint: disable=W0143
class AWSDataSyncTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datasync = None

    # Runs once for each test
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }

        self.dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once',
                       default_args=args,
                       schedule_interval='@once')

        self.client = boto3.client("datasync", region_name="us-east-1")

        self.source_location_arn = self.client.create_location_smb(
            **MOCK_DATA['create_source_location_kwargs']
        )['LocationArn']
        self.destination_location_arn = self.client.create_location_s3(
            **MOCK_DATA['create_destination_location_kwargs']
        )['LocationArn']
        self.task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn
        )['TaskArn']

    def tearDown(self):
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks['Tasks']:
            self.client.delete_task(TaskArn=task['TaskArn'])
        # Delete all locations:
        locations = self.client.list_locations()
        for location in locations['Locations']:
            self.client.delete_location(LocationArn=location['LocationArn'])
        self.client = None


@mock_datasync
@mock.patch.object(AWSDataSyncHook, 'get_conn')
@unittest.skipIf(mock_datasync == no_datasync, 'moto datasync package missing')  # pylint: disable=W0143
class TestAWSDataSyncCreateTaskOperator(AWSDataSyncTestCase):

    def set_up_operator(
        self,
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI
    ):
        # Create operator
        self.datasync = AWSDataSyncCreateTaskOperator(
            task_id='test_aws_datasync_create_task_operator',
            dag=self.dag,
            source_location_uri=source_location_uri,
            destination_location_uri=destination_location_uri,
            case_sensitive_location_search=True,
            create_task_kwargs={'Options': {
                'VerifyMode': 'NONE', 'Atime': 'NONE'}},
            create_source_location_kwargs={
                'Subdirectory': SOURCE_SUBDIR,
                'ServerHostname': SOURCE_HOST_NAME,
                'User': 'airflow',
                'Password': 'airflow_password',
                'AgentArns': ['some_agent']
            },
            create_destination_location_kwargs={
                'S3BucketArn': DESTINATION_LOCATION_ARN,
                'S3Config': {'BucketAccessRoleArn': 'myrole'},
            }
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        self.assertEqual(self.datasync.task_id, MOCK_DATA['create_task_id'])
        # Defaults
        self.assertEqual(self.datasync.aws_conn_id, 'aws_default')
        # Assignments
        self.assertEqual(self.datasync.source_location_uri,
                         MOCK_DATA['source_location_uri'])
        self.assertEqual(self.datasync.destination_location_uri,
                         MOCK_DATA['destination_location_uri'])
        self.assertEqual(self.datasync.case_sensitive_location_search,
                         MOCK_DATA['case_sensitive_location_search'])
        self.assertEqual(self.datasync.create_task_kwargs,
                         MOCK_DATA['create_task_kwargs'])
        self.assertEqual(self.datasync.create_source_location_kwargs,
                         MOCK_DATA['create_source_location_kwargs'])
        self.assertEqual(self.datasync.create_destination_location_kwargs,
                         MOCK_DATA['create_destination_location_kwargs'])

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with self.assertRaises(AirflowException):
            self.set_up_operator(source_location_uri=None)
        with self.assertRaises(AirflowException):
            self.set_up_operator(destination_location_uri=None)
        with self.assertRaises(AirflowException):
            self.set_up_operator(
                source_location_uri=None, destination_location_uri=None)

    def test_create_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks['Tasks']:
            self.client.delete_task(TaskArn=task['TaskArn'])

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 0)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)
        task_arn = result

        # Assert 1 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 1)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Check task metadata
        task = self.client.describe_task(TaskArn=task_arn)
        self.assertEqual(task['Options'], CREATE_TASK_KWARGS['Options'])

    def test_create_task_even_if_one_exists(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 1)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)
        task_arn = result

        # Assert 1 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 2)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Check task metadata
        task = self.client.describe_task(TaskArn=task_arn)
        self.assertEqual(task['Options'], CREATE_TASK_KWARGS['Options'])

    def test_create_task_and_location(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks['Tasks']:
            self.client.delete_task(TaskArn=task['TaskArn'])
        # Delete all locations:
        locations = self.client.list_locations()
        for location in locations['Locations']:
            self.client.delete_location(LocationArn=location['LocationArn'])

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 0)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 0)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)

        # Assert 1 additional task and 2 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 1)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

    def test_xcom_push(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        ti = TaskInstance(task=self.datasync, execution_date=timezone.utcnow())
        ti.run()
        xcom_result = ti.xcom_pull(
            task_ids=self.datasync.task_id, key='return_value')
        self.assertIsNotNone(xcom_result)


@mock_datasync
@mock.patch.object(AWSDataSyncHook, 'get_conn')
@unittest.skipIf(mock_datasync == no_datasync, 'moto datasync package missing')  # pylint: disable=W0143
class TestAWSDataSyncGetTasksOperator(AWSDataSyncTestCase):

    def set_up_operator(
        self,
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI
    ):
        # Create operator
        self.datasync = AWSDataSyncGetTasksOperator(
            task_id='test_aws_datasync_get_tasks_operator',
            dag=self.dag,
            source_location_uri=source_location_uri,
            destination_location_uri=destination_location_uri,
            case_sensitive_location_search=True
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        self.assertEqual(self.datasync.task_id, MOCK_DATA['get_task_id'])
        # Defaults
        self.assertEqual(self.datasync.aws_conn_id, 'aws_default')
        # Assignments
        self.assertEqual(self.datasync.source_location_uri,
                         MOCK_DATA['source_location_uri'])
        self.assertEqual(self.datasync.destination_location_uri,
                         MOCK_DATA['destination_location_uri'])
        self.assertEqual(self.datasync.case_sensitive_location_search,
                         MOCK_DATA['case_sensitive_location_search'])

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with self.assertRaises(AirflowException):
            self.set_up_operator(source_location_uri=None)
        with self.assertRaises(AirflowException):
            self.set_up_operator(destination_location_uri=None)
        with self.assertRaises(AirflowException):
            self.set_up_operator(
                source_location_uri=None, destination_location_uri=None)

    def test_get_no_location(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        locations = self.client.list_locations()
        for location in locations['Locations']:
            self.client.delete_location(LocationArn=location['LocationArn'])

        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 0)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)
        self.assertFalse(result)

    def test_get_no_tasks2(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        tasks = self.client.list_tasks()
        for task in tasks['Tasks']:
            self.client.delete_task(TaskArn=task['TaskArn'])

        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 0)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)
        self.assertFalse(result)

    def test_get_one_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Make sure we dont cheat
        self.set_up_operator()
        self.assertEqual(self.datasync.task_arns, None)

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 1)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)

        task_arns = result
        self.assertIsNotNone(task_arns)
        self.assertTrue(task_arns)
        self.assertEqual(len(task_arns), 1)
        self.assertEqual(task_arns[0], self.task_arn)

        # Assert 0 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 1)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

    def test_get_many_tasks(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()

        self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn
        )

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 2)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)

        task_arns = result
        self.assertIsNotNone(task_arns)
        self.assertTrue(task_arns)
        self.assertEqual(len(task_arns), 2)

        # Assert 0 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 2)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

    def test_xcom_push(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        ti = TaskInstance(task=self.datasync, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(
            ti.xcom_pull(task_ids=self.datasync.task_id, key='return_value'),
            [self.task_arn])


@mock_datasync
@mock.patch.object(AWSDataSyncHook, 'get_conn')
@unittest.skipIf(mock_datasync == no_datasync, 'moto datasync package missing')  # pylint: disable=W0143
class TestAWSDataSyncUpdateTaskOperator(AWSDataSyncTestCase):

    def __init(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datasync = None

    def set_up_operator(
        self,
        task_arn='self',
        update_task_kwargs='default'
    ):
        if task_arn == 'self':
            task_arn = self.task_arn
        if update_task_kwargs == 'default':
            update_task_kwargs = {'Options': {
                'VerifyMode': 'BEST_EFFORT', 'Atime': 'NONE'}}
        # Create operator
        self.datasync = AWSDataSyncUpdateTaskOperator(
            task_id='test_aws_datasync_update_task_operator',
            dag=self.dag,
            task_arn=task_arn,
            update_task_kwargs=update_task_kwargs,
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        self.assertEqual(self.datasync.task_id, MOCK_DATA['update_task_id'])
        # Defaults
        self.assertEqual(self.datasync.aws_conn_id, 'aws_default')
        # Assignments
        self.assertEqual(self.datasync.task_arn, self.task_arn)
        self.assertEqual(self.datasync.update_task_kwargs,
                         MOCK_DATA['update_task_kwargs'])

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with self.assertRaises(AirflowException):
            self.set_up_operator(task_arn=None)
        with self.assertRaises(AirflowException):
            self.set_up_operator(update_task_kwargs=None)

    def test_update_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()

        # Check task before update
        task = self.client.describe_task(TaskArn=self.task_arn)
        self.assertNotIn('Options', task)

        # Execute the task
        result = self.datasync.execute(None)

        self.assertIsNotNone(result)
        self.assertEqual(result, self.task_arn)

        self.assertIsNotNone(self.datasync.task_arn)
        # Check it was updated
        task = self.client.describe_task(TaskArn=self.task_arn)
        self.assertEqual(task['Options'], UPDATE_TASK_KWARGS['Options'])

    def test_xcom_push(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        ti = TaskInstance(task=self.datasync, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(
            ti.xcom_pull(task_ids=self.datasync.task_id, key='return_value'),
            self.task_arn)


@mock_datasync
@mock.patch.object(AWSDataSyncHook, 'get_conn')
@unittest.skipIf(mock_datasync == no_datasync, 'moto datasync package missing')  # pylint: disable=W0143
class TestAWSDataSyncTaskOperator(AWSDataSyncTestCase):

    def set_up_operator(
        self,
        task_arn='self'
    ):
        if task_arn == 'self':
            task_arn = self.task_arn
        # Create operator
        self.datasync = AWSDataSyncTaskOperator(
            task_id='test_aws_datasync_task_operator',
            dag=self.dag,
            wait_interval_seconds=0,
            task_arn=task_arn
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        self.assertEqual(self.datasync.task_id, MOCK_DATA['task_id'])
        # Defaults
        self.assertEqual(self.datasync.aws_conn_id, 'aws_default')
        self.assertEqual(self.datasync.wait_interval_seconds, 0)
        # Assignments
        self.assertEqual(self.datasync.task_arn, self.task_arn)

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with self.assertRaises(AirflowException):
            self.set_up_operator(task_arn=None)

    def test_execute_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Configure the Operator with the specific task_arn
        self.set_up_operator()
        self.assertEqual(self.datasync.task_arn, self.task_arn)

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        len_tasks_before = len(tasks['Tasks'])
        locations = self.client.list_locations()
        len_locations_before = len(locations['Locations'])

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)
        task_execution_arn = result
        self.assertIsNotNone(task_execution_arn)

        # Assert 0 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), len_tasks_before)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), len_locations_before)

        # Check with the DataSync client what happened
        task_execution = self.client.describe_task_execution(
            TaskExecutionArn=task_execution_arn)
        self.assertEqual(task_execution['Status'], 'SUCCESS')

        # Insist that this specific task was executed, not anything else
        task_execution_arn = task_execution['TaskExecutionArn']
        # format of task_execution_arn:
        # arn:aws:datasync:us-east-1:111222333444:task/task-00000000000000003/execution/exec-00000000000000004
        # format of task_arn:
        # arn:aws:datasync:us-east-1:111222333444:task/task-00000000000000003
        self.assertEqual(
            '/'.join(task_execution_arn.split('/')[:2]), self.task_arn)

    @mock.patch.object(AWSDataSyncHook, 'wait_for_task_execution')
    def test_failed_task(self, mock_wait, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        mock_wait.return_value = False
        # ### Begin tests:

        self.set_up_operator()

        # Execute the task
        with self.assertRaises(AirflowException):
            self.datasync.execute(None)

    @mock.patch.object(AWSDataSyncHook, 'wait_for_task_execution')
    def test_killed_task(self, mock_wait, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Kill the task when doing wait_for_task_execution
        def kill_task(*args):
            self.datasync.on_kill()
            return True
        mock_wait.side_effect = kill_task

        self.set_up_operator()

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)

        task_execution_arn = result
        self.assertIsNotNone(task_execution_arn)

        # Verify the task was killed
        task = self.client.describe_task(TaskArn=self.task_arn)
        self.assertEqual(task['Status'], 'AVAILABLE')
        task_execution = self.client.describe_task_execution(
            TaskExecutionArn=task_execution_arn)
        self.assertEqual(task_execution['Status'], 'ERROR')

    def test_xcom_push(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        ti = TaskInstance(task=self.datasync, execution_date=timezone.utcnow())
        ti.run()
        xcom_result = ti.xcom_pull(
            task_ids=self.datasync.task_id, key='return_value')
        self.assertIsNotNone(xcom_result)


@mock_datasync
@mock.patch.object(AWSDataSyncHook, 'get_conn')
@unittest.skipIf(mock_datasync == no_datasync, 'moto datasync package missing')  # pylint: disable=W0143
class TestAWSDataSyncDeleteTaskOperator(AWSDataSyncTestCase):

    def set_up_operator(
        self,
        task_arn='self'
    ):
        if task_arn == 'self':
            task_arn = self.task_arn
        # Create operator
        self.datasync = AWSDataSyncDeleteTaskOperator(
            task_id='test_aws_datasync_delete_task_operator',
            dag=self.dag,
            task_arn=task_arn
        )

    def test_init(self, mock_get_conn):
        self.set_up_operator()
        # Airflow built-ins
        self.assertEqual(self.datasync.task_id, MOCK_DATA['delete_task_id'])
        # Defaults
        self.assertEqual(self.datasync.aws_conn_id, 'aws_default')
        # Assignments
        self.assertEqual(self.datasync.task_arn, self.task_arn)

    def test_init_fails(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        with self.assertRaises(AirflowException):
            self.set_up_operator(task_arn=None)

    def test_delete_task(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()

        # Check how many tasks and locations we have
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 1)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

        # Execute the task
        result = self.datasync.execute(None)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.task_arn)

        # Assert -1 additional task and 0 additional locations
        tasks = self.client.list_tasks()
        self.assertEqual(len(tasks['Tasks']), 0)
        locations = self.client.list_locations()
        self.assertEqual(len(locations['Locations']), 2)

    def test_xcom_push(self, mock_get_conn):
        # ### Set up mocks:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        self.set_up_operator()
        ti = TaskInstance(task=self.datasync, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(
            ti.xcom_pull(task_ids=self.datasync.task_id, key='return_value'),
            self.task_arn)
