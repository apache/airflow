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
#

import sys
import unittest

from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.exceptions import AirflowException
from tests.compat import mock

RESPONSE_WITHOUT_FAILURES = {
    "jobName": "51455483-c62c-48ac-9b88-53a6a725baa3",
    "jobId": "8ba9d676-4108-4474-9dca-8bbac1da9b19"
}


class TestAWSBatchOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.awsbatch_operator.AwsHook')
    def setUp(self, aws_hook_mock):
        self.aws_hook_mock = aws_hook_mock
        self.batch = AWSBatchOperator(
            task_id='task',
            job_name='51455483-c62c-48ac-9b88-53a6a725baa3',
            job_queue='queue',
            job_definition='hello-world',
            max_retries=5,
            overrides={},
            array_properties=None,
            aws_conn_id=None,
            region_name='eu-west-1')

    def test_init(self):
        self.assertEqual(self.batch.job_name, '51455483-c62c-48ac-9b88-53a6a725baa3')
        self.assertEqual(self.batch.job_queue, 'queue')
        self.assertEqual(self.batch.job_definition, 'hello-world')
        self.assertEqual(self.batch.max_retries, 5)
        self.assertEqual(self.batch.overrides, {})
        self.assertEqual(self.batch.array_properties, None)
        self.assertEqual(self.batch.region_name, 'eu-west-1')
        self.assertEqual(self.batch.aws_conn_id, None)
        self.assertEqual(self.batch.hook, self.aws_hook_mock.return_value)

        self.aws_hook_mock.assert_called_once_with(aws_conn_id=None)

    def test_template_fields_overrides(self):
        self.assertEqual(self.batch.template_fields, ('job_name', 'overrides',))

    @mock.patch.object(AWSBatchOperator, '_wait_for_task_ended')
    @mock.patch.object(AWSBatchOperator, '_check_success_task')
    def test_execute_without_failures(self, check_mock, wait_mock):
        client_mock = self.aws_hook_mock.return_value.get_client_type.return_value
        client_mock.submit_job.return_value = RESPONSE_WITHOUT_FAILURES

        self.batch.execute(None)

        self.aws_hook_mock.return_value.get_client_type.assert_called_once_with('batch',
                                                                                region_name='eu-west-1')
        client_mock.submit_job.assert_called_once_with(
            jobQueue='queue',
            jobName='51455483-c62c-48ac-9b88-53a6a725baa3',
            containerOverrides={},
            jobDefinition='hello-world',
            arrayProperties=None
        )

        wait_mock.assert_called_once_with()
        check_mock.assert_called_once_with()
        self.assertEqual(self.batch.jobId, '8ba9d676-4108-4474-9dca-8bbac1da9b19')

    def test_execute_with_failures(self):
        client_mock = self.aws_hook_mock.return_value.get_client_type.return_value
        client_mock.submit_job.return_value = ""

        with self.assertRaises(AirflowException):
            self.batch.execute(None)

        self.aws_hook_mock.return_value.get_client_type.assert_called_once_with('batch',
                                                                                region_name='eu-west-1')
        client_mock.submit_job.assert_called_once_with(
            jobQueue='queue',
            jobName='51455483-c62c-48ac-9b88-53a6a725baa3',
            containerOverrides={},
            jobDefinition='hello-world',
            arrayProperties=None
        )

    def test_wait_end_tasks(self):
        client_mock = mock.Mock()
        self.batch.jobId = '8ba9d676-4108-4474-9dca-8bbac1da9b19'
        self.batch.client = client_mock

        self.batch._wait_for_task_ended()

        client_mock.get_waiter.assert_called_once_with('job_execution_complete')
        client_mock.get_waiter.return_value.wait.assert_called_once_with(
            jobs=['8ba9d676-4108-4474-9dca-8bbac1da9b19']
        )
        self.assertEqual(sys.maxsize, client_mock.get_waiter.return_value.config.max_attempts)

    def test_check_success_tasks_raises(self):
        client_mock = mock.Mock()
        self.batch.jobId = '8ba9d676-4108-4474-9dca-8bbac1da9b19'
        self.batch.client = client_mock

        client_mock.describe_jobs.return_value = {
            'jobs': []
        }

        with self.assertRaises(Exception) as e:
            self.batch._check_success_task()

        # Ordering of str(dict) is not guaranteed.
        self.assertIn('No job found for ', str(e.exception))

    def test_check_success_tasks_raises_failed(self):
        client_mock = mock.Mock()
        self.batch.jobId = '8ba9d676-4108-4474-9dca-8bbac1da9b19'
        self.batch.client = client_mock

        client_mock.describe_jobs.return_value = {
            'jobs': [{
                'status': 'FAILED',
                'statusReason': 'This is an error reason',
                'attempts': [{
                    'exitCode': 1
                }]
            }]
        }

        with self.assertRaises(Exception) as e:
            self.batch._check_success_task()

        # Ordering of str(dict) is not guaranteed.
        self.assertIn('Job failed with status ', str(e.exception))

    def test_check_success_tasks_raises_pending(self):
        client_mock = mock.Mock()
        self.batch.jobId = '8ba9d676-4108-4474-9dca-8bbac1da9b19'
        self.batch.client = client_mock

        client_mock.describe_jobs.return_value = {
            'jobs': [{
                'status': 'RUNNABLE'
            }]
        }

        with self.assertRaises(Exception) as e:
            self.batch._check_success_task()

        # Ordering of str(dict) is not guaranteed.
        self.assertIn('This task is still pending ', str(e.exception))

    def test_check_success_tasks_raises_multiple(self):
        client_mock = mock.Mock()
        self.batch.jobId = '8ba9d676-4108-4474-9dca-8bbac1da9b19'
        self.batch.client = client_mock

        client_mock.describe_jobs.return_value = {
            'jobs': [{
                'status': 'FAILED',
                'statusReason': 'This is an error reason',
                'attempts': [{
                    'exitCode': 1
                }, {
                    'exitCode': 10
                }]
            }]
        }

        with self.assertRaises(Exception) as e:
            self.batch._check_success_task()

        # Ordering of str(dict) is not guaranteed.
        self.assertIn('Job failed with status ', str(e.exception))

    def test_check_success_task_not_raises(self):
        client_mock = mock.Mock()
        self.batch.jobId = '8ba9d676-4108-4474-9dca-8bbac1da9b19'
        self.batch.client = client_mock

        client_mock.describe_jobs.return_value = {
            'jobs': [{
                'status': 'SUCCEEDED'
            }]
        }

        self.batch._check_success_task()

        # Ordering of str(dict) is not guaranteed.
        client_mock.describe_jobs.assert_called_once_with(jobs=['8ba9d676-4108-4474-9dca-8bbac1da9b19'])


if __name__ == '__main__':
    unittest.main()
