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
from __future__ import annotations

import unittest
from unittest import mock

import botocore.exceptions
import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook

# Use dummy AWS credentials
AWS_REGION = "eu-west-1"
AWS_ACCESS_KEY_ID = "airflow_dummy_key"
AWS_SECRET_ACCESS_KEY = "airflow_dummy_secret"

JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
LOG_STREAM_NAME = "test/stream/d56a66bb98a14c4593defa1548686edf"


class TestBatchClient(unittest.TestCase):

    MAX_RETRIES = 2
    STATUS_RETRIES = 3

    @mock.patch.dict("os.environ", AWS_DEFAULT_REGION=AWS_REGION)
    @mock.patch.dict("os.environ", AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID)
    @mock.patch.dict("os.environ", AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def setUp(self, get_client_type_mock):
        self.get_client_type_mock = get_client_type_mock
        self.batch_client = BatchClientHook(
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            aws_conn_id='airflow_test',
            region_name=AWS_REGION,
        )
        self.client_mock = get_client_type_mock.return_value
        assert self.batch_client.client == self.client_mock  # setup client property

        # don't pause in these unit tests
        self.mock_delay = mock.Mock(return_value=None)
        self.batch_client.delay = self.mock_delay
        self.mock_exponential_delay = mock.Mock(return_value=0)
        self.batch_client.exponential_delay = self.mock_exponential_delay

    def test_init(self):
        assert self.batch_client.max_retries == self.MAX_RETRIES
        assert self.batch_client.status_retries == self.STATUS_RETRIES
        assert self.batch_client.region_name == AWS_REGION
        assert self.batch_client.aws_conn_id == 'airflow_test'
        assert self.batch_client.client == self.client_mock

        self.get_client_type_mock.assert_called_once_with(region_name=AWS_REGION)

    def test_wait_for_job_with_success(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "SUCCEEDED"}]}

        with mock.patch.object(
            self.batch_client,
            "poll_for_job_running",
            wraps=self.batch_client.poll_for_job_running,
        ) as job_running:
            self.batch_client.wait_for_job(JOB_ID)
            job_running.assert_called_once_with(JOB_ID, None)

        with mock.patch.object(
            self.batch_client,
            "poll_for_job_complete",
            wraps=self.batch_client.poll_for_job_complete,
        ) as job_complete:
            self.batch_client.wait_for_job(JOB_ID)
            job_complete.assert_called_once_with(JOB_ID, None)

        assert self.client_mock.describe_jobs.call_count == 4

    def test_wait_for_job_with_failure(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "FAILED"}]}

        with mock.patch.object(
            self.batch_client,
            "poll_for_job_running",
            wraps=self.batch_client.poll_for_job_running,
        ) as job_running:
            self.batch_client.wait_for_job(JOB_ID)
            job_running.assert_called_once_with(JOB_ID, None)

        with mock.patch.object(
            self.batch_client,
            "poll_for_job_complete",
            wraps=self.batch_client.poll_for_job_complete,
        ) as job_complete:
            self.batch_client.wait_for_job(JOB_ID)
            job_complete.assert_called_once_with(JOB_ID, None)

        assert self.client_mock.describe_jobs.call_count == 4

    def test_poll_job_running_for_status_running(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "RUNNING"}]}
        self.batch_client.poll_for_job_running(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])

    def test_poll_job_complete_for_status_success(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "SUCCEEDED"}]}
        self.batch_client.poll_for_job_complete(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])

    def test_poll_job_complete_raises_for_max_retries(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "RUNNING"}]}
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.poll_for_job_complete(JOB_ID)
        msg = f"AWS Batch job ({JOB_ID}) status checks exceed max_retries"
        assert msg in str(ctx.value)
        self.client_mock.describe_jobs.assert_called_with(jobs=[JOB_ID])
        assert self.client_mock.describe_jobs.call_count == self.MAX_RETRIES + 1

    def test_poll_job_status_hit_api_throttle(self):
        self.client_mock.describe_jobs.side_effect = botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "TooManyRequestsException"}},
            operation_name="get job description",
        )
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.poll_for_job_complete(JOB_ID)
        msg = f"AWS Batch job ({JOB_ID}) description error"
        assert msg in str(ctx.value)
        # It should retry when this client error occurs
        self.client_mock.describe_jobs.assert_called_with(jobs=[JOB_ID])
        assert self.client_mock.describe_jobs.call_count == self.STATUS_RETRIES

    def test_poll_job_status_with_client_error(self):
        self.client_mock.describe_jobs.side_effect = botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "InvalidClientTokenId"}},
            operation_name="get job description",
        )
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.poll_for_job_complete(JOB_ID)
        msg = f"AWS Batch job ({JOB_ID}) description error"
        assert msg in str(ctx.value)
        # It will not retry when this client error occurs
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])

    def test_check_job_success(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "SUCCEEDED"}]}
        status = self.batch_client.check_job_success(JOB_ID)
        assert status
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])

    def test_check_job_success_raises_failed(self):
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "status": "FAILED",
                    "statusReason": "This is an error reason",
                    "attempts": [{"exitCode": 1}],
                }
            ]
        }
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.check_job_success(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])
        msg = f"AWS Batch job ({JOB_ID}) failed"
        assert msg in str(ctx.value)

    def test_check_job_success_raises_failed_for_multiple_attempts(self):
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "status": "FAILED",
                    "statusReason": "This is an error reason",
                    "attempts": [{"exitCode": 1}, {"exitCode": 10}],
                }
            ]
        }
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.check_job_success(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])
        msg = f"AWS Batch job ({JOB_ID}) failed"
        assert msg in str(ctx.value)

    def test_check_job_success_raises_incomplete(self):
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": "RUNNABLE"}]}
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.check_job_success(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])
        msg = f"AWS Batch job ({JOB_ID}) is not complete"
        assert msg in str(ctx.value)

    def test_check_job_success_raises_unknown_status(self):
        status = "STRANGE"
        self.client_mock.describe_jobs.return_value = {"jobs": [{"jobId": JOB_ID, "status": status}]}
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.check_job_success(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])
        msg = f"AWS Batch job ({JOB_ID}) has unknown status"
        assert msg in str(ctx.value)
        assert status in str(ctx.value)

    def test_check_job_success_raises_without_jobs(self):
        self.client_mock.describe_jobs.return_value = {"jobs": []}
        with pytest.raises(AirflowException) as ctx:
            self.batch_client.check_job_success(JOB_ID)
        self.client_mock.describe_jobs.assert_called_once_with(jobs=[JOB_ID])
        msg = f"AWS Batch job ({JOB_ID}) description error"
        assert msg in str(ctx.value)

    def test_terminate_job(self):
        self.client_mock.terminate_job.return_value = {}
        reason = "Task killed by the user"
        response = self.batch_client.terminate_job(JOB_ID, reason)
        self.client_mock.terminate_job.assert_called_once_with(jobId=JOB_ID, reason=reason)
        assert response == {}

    def test_job_awslogs_default(self):
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "container": {"logStreamName": LOG_STREAM_NAME},
                }
            ]
        }
        self.client_mock.meta.client.meta.region_name = AWS_REGION

        awslogs = self.batch_client.get_job_awslogs_info(JOB_ID)
        assert awslogs["awslogs_stream_name"] == LOG_STREAM_NAME
        assert awslogs["awslogs_group"] == "/aws/batch/job"
        assert awslogs["awslogs_region"] == AWS_REGION

    def test_job_awslogs_user_defined(self):
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "container": {
                        "logStreamName": LOG_STREAM_NAME,
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": "/test/batch/job",
                                "awslogs-region": "ap-southeast-2",
                            },
                        },
                    },
                }
            ]
        }
        awslogs = self.batch_client.get_job_awslogs_info(JOB_ID)
        assert awslogs["awslogs_stream_name"] == LOG_STREAM_NAME
        assert awslogs["awslogs_group"] == "/test/batch/job"
        assert awslogs["awslogs_region"] == "ap-southeast-2"

    def test_job_no_awslogs_stream(self):
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "container": {},
                }
            ]
        }
        with self.assertLogs(level='WARNING') as capture_logs:
            assert self.batch_client.get_job_awslogs_info(JOB_ID) is None
            assert len(capture_logs.records) == 1

    def test_job_splunk_logs(self):
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "logStreamName": LOG_STREAM_NAME,
                    "container": {
                        "logConfiguration": {
                            "logDriver": "splunk",
                        }
                    },
                }
            ]
        }
        with self.assertLogs(level='WARNING') as capture_logs:
            assert self.batch_client.get_job_awslogs_info(JOB_ID) is None
            assert len(capture_logs.records) == 1


class TestBatchClientDelays(unittest.TestCase):
    @mock.patch.dict("os.environ", AWS_DEFAULT_REGION=AWS_REGION)
    @mock.patch.dict("os.environ", AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID)
    @mock.patch.dict("os.environ", AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    def setUp(self):
        self.batch_client = BatchClientHook(aws_conn_id='airflow_test', region_name=AWS_REGION)

    def test_init(self):
        assert self.batch_client.max_retries == self.batch_client.MAX_RETRIES
        assert self.batch_client.status_retries == self.batch_client.STATUS_RETRIES
        assert self.batch_client.region_name == AWS_REGION
        assert self.batch_client.aws_conn_id == 'airflow_test'

    def test_add_jitter(self):
        minima = 0
        width = 5
        result = self.batch_client.add_jitter(0, width=width, minima=minima)
        assert result >= minima
        assert result <= width

    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.uniform")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.sleep")
    def test_delay_defaults(self, mock_sleep, mock_uniform):
        assert BatchClientHook.DEFAULT_DELAY_MIN == 1
        assert BatchClientHook.DEFAULT_DELAY_MAX == 10
        mock_uniform.return_value = 0
        self.batch_client.delay()
        mock_uniform.assert_called_once_with(
            BatchClientHook.DEFAULT_DELAY_MIN, BatchClientHook.DEFAULT_DELAY_MAX
        )
        mock_sleep.assert_called_once_with(0)

    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.uniform")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.sleep")
    def test_delay_with_zero(self, mock_sleep, mock_uniform):
        self.batch_client.delay(0)
        mock_uniform.assert_called_once_with(0, 1)  # in add_jitter
        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.uniform")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.sleep")
    def test_delay_with_int(self, mock_sleep, mock_uniform):
        self.batch_client.delay(5)
        mock_uniform.assert_called_once_with(4, 6)  # in add_jitter
        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.uniform")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.sleep")
    def test_delay_with_float(self, mock_sleep, mock_uniform):
        self.batch_client.delay(5.0)
        mock_uniform.assert_called_once_with(4.0, 6.0)  # in add_jitter
        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @parameterized.expand(
        [
            (0, 0, 1),
            (1, 0, 2),
            (2, 0, 3),
            (3, 1, 5),
            (4, 2, 7),
            (5, 3, 11),
            (6, 4, 14),
            (7, 6, 19),
            (8, 8, 25),
            (9, 10, 31),
            (45, 200, 600),  # > 40 tries invokes maximum delay allowed
        ]
    )
    def test_exponential_delay(self, tries, lower, upper):
        result = self.batch_client.exponential_delay(tries)
        assert result >= lower
        assert result <= upper
