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

import unittest
from unittest import mock

from testfixtures import LogCapture

from airflow.providers.amazon.aws.hooks.glacier import GlacierHook

CREDENTIALS = "aws_conn"
VAULT_NAME = "airflow"
JOB_ID = "1234abcd"
REQUEST_RESULT = {"jobId": "1234abcd"}
RESPONSE_BODY = {"body": "data"}
JOB_STATUS = {"Action": "", "StatusCode": "Succeeded"}


class TestAmazonGlacierHook(unittest.TestCase):
    def setUp(self):
        with mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.__init__", return_value=None):
            self.hook = GlacierHook(aws_conn_id="aws_default")

    @mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.get_conn")
    def test_retrieve_inventory_should_return_job_id(self, mock_conn):
        # Given
        job_id = {"jobId": "1234abcd"}
        # when
        mock_conn.return_value.initiate_job.return_value = job_id
        result = self.hook.retrieve_inventory(VAULT_NAME)
        # then
        mock_conn.assert_called_once_with()
        self.assertEqual(job_id, result)

    @mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.get_conn")
    def test_retrieve_inventory_should_log_mgs(self, mock_conn):
        # given
        job_id = {"jobId": "1234abcd"}
        # when
        with LogCapture() as log:
            mock_conn.return_value.initiate_job.return_value = job_id
            self.hook.retrieve_inventory(VAULT_NAME)
            # then
            log.check(
                (
                    'airflow.providers.amazon.aws.hooks.glacier.GlacierHook',
                    'INFO',
                    f"Retrieving inventory for vault: {VAULT_NAME}",
                ),
                (
                    'airflow.providers.amazon.aws.hooks.glacier.GlacierHook',
                    'INFO',
                    f"Initiated inventory-retrieval job for: {VAULT_NAME}",
                ),
                (
                    'airflow.providers.amazon.aws.hooks.glacier.GlacierHook',
                    'INFO',
                    f"Retrieval Job ID: {job_id.get('jobId')}",
                ),
            )

    @mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.get_conn")
    def test_retrieve_inventory_results_should_return_response(self, mock_conn):
        # when
        mock_conn.return_value.get_job_output.return_value = RESPONSE_BODY
        response = self.hook.retrieve_inventory_results(VAULT_NAME, JOB_ID)
        # then
        mock_conn.assert_called_once_with()
        self.assertEqual(response, RESPONSE_BODY)

    @mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.get_conn")
    def test_retrieve_inventory_results_should_log_mgs(self, mock_conn):
        # when
        with LogCapture() as log:
            mock_conn.return_value.get_job_output.return_value = REQUEST_RESULT
            self.hook.retrieve_inventory_results(VAULT_NAME, JOB_ID)
            # then
            log.check(
                (
                    'airflow.providers.amazon.aws.hooks.glacier.GlacierHook',
                    'INFO',
                    f"Retrieving the job results for vault: {VAULT_NAME}...",
                ),
            )

    @mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.get_conn")
    def test_describe_job_should_return_status_succeeded(self, mock_conn):
        # when
        mock_conn.return_value.describe_job.return_value = JOB_STATUS
        response = self.hook.describe_job(VAULT_NAME, JOB_ID)
        # then
        mock_conn.assert_called_once_with()
        self.assertEqual(response, JOB_STATUS)

    @mock.patch("airflow.providers.amazon.aws.hooks.glacier.GlacierHook.get_conn")
    def test_describe_job_should_log_mgs(self, mock_conn):
        # when
        with LogCapture() as log:
            mock_conn.return_value.describe_job.return_value = JOB_STATUS
            self.hook.describe_job(VAULT_NAME, JOB_ID)
            # then
            log.check(
                (
                    'airflow.providers.amazon.aws.hooks.glacier.GlacierHook',
                    'INFO',
                    f"Retrieving status for vault: {VAULT_NAME} and job {JOB_ID}",
                ),
                (
                    'airflow.providers.amazon.aws.hooks.glacier.GlacierHook',
                    'INFO',
                    f"Job status: {JOB_STATUS.get('Action')}, code status: {JOB_STATUS.get('StatusCode')}",
                ),
            )
