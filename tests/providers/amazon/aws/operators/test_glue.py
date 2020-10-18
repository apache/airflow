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

import mock

from airflow import configuration
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator


class TestAwsGlueJobOperator(unittest.TestCase):
    @mock.patch('airflow.providers.amazon.aws.hooks.glue.AwsGlueJobHook')
    def setUp(self, glue_hook_mock):
        configuration.load_test_config()

        self.glue_hook_mock = glue_hook_mock
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        self.glue = AwsGlueJobOperator(
            task_id='test_glue_operator',
            job_name='my_test_job',
            script_location=some_script,
            aws_conn_id='aws_default',
            region_name='us-west-2',
            s3_bucket='some_bucket',
            iam_role_name='my_test_role',
        )

    @mock.patch.object(AwsGlueJobHook, 'get_job_state')
    @mock.patch.object(AwsGlueJobHook, 'initialize_job')
    @mock.patch.object(AwsGlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_without_failure(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state
    ):
        mock_initialize_job.return_value = {'JobRunState': 'RUNNING', 'JobRunId': '11111'}
        mock_get_job_state.return_value = 'SUCCEEDED'
        self.glue.execute(None)

        mock_initialize_job.assert_called_once_with({})
        self.assertEqual(self.glue.job_name, 'my_test_job')
