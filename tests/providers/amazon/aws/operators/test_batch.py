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

# pylint: disable=missing-docstring

import unittest

import mock

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator

# Use dummy AWS credentials
AWS_REGION = "eu-west-1"
AWS_ACCESS_KEY_ID = "airflow_dummy_key"
AWS_SECRET_ACCESS_KEY = "airflow_dummy_secret"

JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"

RESPONSE_WITHOUT_FAILURES = {
    "jobName": JOB_NAME,
    "jobId": JOB_ID,
}


class TestAwsBatchOperator(unittest.TestCase):

    MAX_RETRIES = 2
    STATUS_RETRIES = 3

    @mock.patch.dict("os.environ", AWS_DEFAULT_REGION=AWS_REGION)
    @mock.patch.dict("os.environ", AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID)
    @mock.patch.dict("os.environ", AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsHook")
    def setUp(self, aws_hook_mock):
        self.aws_hook_mock = aws_hook_mock
        self.batch = AwsBatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id=None,
            region_name="eu-west-1",
        )
        self.client_mock = self.aws_hook_mock.return_value.get_client_type.return_value
        self.assertEqual(self.batch.client, self.client_mock)  # setup client property

        # don't pause in unit tests
        self.mock_delay = mock.Mock(return_value=None)
        self.batch.delay = self.mock_delay
        self.mock_exponential_delay = mock.Mock(return_value=0)
        self.batch.exponential_delay = self.mock_exponential_delay

        # Assign a job ID for most tests, so they don't depend on a job submission.
        self.assertIsNone(self.batch.job_id)
        self.batch.job_id = JOB_ID

    def test_init(self):
        self.assertEqual(self.batch.job_id, JOB_ID)
        self.assertEqual(self.batch.job_name, JOB_NAME)
        self.assertEqual(self.batch.job_queue, "queue")
        self.assertEqual(self.batch.job_definition, "hello-world")
        self.assertEqual(self.batch.waiters, None)
        self.assertEqual(self.batch.max_retries, self.MAX_RETRIES)
        self.assertEqual(self.batch.status_retries, self.STATUS_RETRIES)
        self.assertEqual(self.batch.parameters, None)
        self.assertEqual(self.batch.overrides, {})
        self.assertEqual(self.batch.array_properties, {})
        self.assertEqual(self.batch.region_name, "eu-west-1")
        self.assertEqual(self.batch.aws_conn_id, None)
        self.assertEqual(self.batch.hook, self.aws_hook_mock.return_value)
        self.assertEqual(self.batch.client, self.client_mock)

        self.aws_hook_mock.assert_called_once_with(aws_conn_id=None)
        self.aws_hook_mock.return_value.get_client_type.assert_called_once_with(
            "batch", region_name="eu-west-1"
        )

    def test_template_fields_overrides(self):
        self.assertEqual(self.batch.template_fields, ("job_name", "overrides", "parameters",))

    @mock.patch.object(AwsBatchOperator, "wait_for_job")
    @mock.patch.object(AwsBatchOperator, "check_job_success")
    def test_execute_without_failures(self, check_mock, wait_mock):
        # JOB_ID is in RESPONSE_WITHOUT_FAILURES
        self.client_mock.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        self.batch.job_id = None
        self.batch.waiters = None  # use default wait

        self.batch.execute(None)

        self.client_mock.submit_job.assert_called_once_with(
            jobQueue="queue",
            jobName=JOB_NAME,
            containerOverrides={},
            jobDefinition="hello-world",
            arrayProperties={},
            parameters=None,
        )

        self.assertEqual(self.batch.job_id, JOB_ID)
        wait_mock.assert_called_once_with(JOB_ID)
        check_mock.assert_called_once_with(JOB_ID)

    def test_execute_with_failures(self):
        self.client_mock.submit_job.return_value = ""

        with self.assertRaises(AirflowException):
            self.batch.execute(None)

        self.client_mock.submit_job.assert_called_once_with(
            jobQueue="queue",
            jobName=JOB_NAME,
            containerOverrides={},
            jobDefinition="hello-world",
            arrayProperties={},
            parameters=None,
        )

    @mock.patch.object(AwsBatchOperator, "check_job_success")
    def test_wait_job_complete_using_waiters(self, check_mock):
        mock_waiters = mock.Mock()
        self.batch.waiters = mock_waiters

        self.client_mock.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        self.client_mock.describe_jobs.return_value = {
            "jobs": [{"jobId": JOB_ID, "status": "SUCCEEDED"}]
        }
        self.batch.execute(None)

        mock_waiters.wait_for_job.assert_called_once_with(JOB_ID)
        check_mock.assert_called_once_with(JOB_ID)

    def test_kill_job(self):
        self.client_mock.terminate_job.return_value = {}
        self.batch.on_kill()
        self.client_mock.terminate_job.assert_called_once_with(
            jobId=JOB_ID, reason="Task killed by the user"
        )


if __name__ == "__main__":
    unittest.main()
