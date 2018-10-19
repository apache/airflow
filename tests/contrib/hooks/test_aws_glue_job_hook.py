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

import unittest


try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

try:
    from moto import mock_iam
except ImportError:
    mock_iam = None


from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook


class TestGlueJobHook(unittest.TestCase):

    def setUp(self):
        self.some_aws_region = "us-west-2"

    @mock.patch.object(AwsGlueJobHook, 'get_conn')
    def test_get_conn_returns_a_boto3_connection(self, mock_get_conn):
        hook = AwsGlueJobHook(job_name='aws_test_glue_job', s3_bucket='some_bucket')
        self.assertIsNotNone(hook.get_conn())

    @mock.patch.object(AwsGlueJobHook, "get_conn")
    def test_get_glue_job(self, mock_get_conn):
        mock_job_name = 'aws_test_glue_job'
        mock_glue_job = mock_get_conn.return_value.get_job(JobName=mock_job_name)
        glue_job = AwsGlueJobHook(
            job_name=mock_job_name,
            desc='This is test case job from Airflow',
            region_name=self.some_aws_region
        ).get_glue_job()
        self.assertEqual(glue_job, mock_glue_job['Job'])

    @mock.patch.object(AwsGlueJobHook, "job_completion")
    @mock.patch.object(AwsGlueJobHook, "get_glue_job")
    @mock.patch.object(AwsGlueJobHook, "get_conn")
    def test_initialize_job(self, mock_get_conn,
                            mock_get_glue_job,
                            mock_completion):
        some_data_path = "s3://glue-datasets/examples/medicare/SampleData.csv"
        some_script_arguments = {"--s3_input_data_path": some_data_path}

        mock_get_glue_job.Name = mock.Mock(Name='aws_test_glue_job')
        mock_get_conn.return_value.start_job_run()

        mock_job_run_state = mock_completion.return_value
        glue_job_run_state = AwsGlueJobHook(
            job_name='aws_test_glue_job',
            desc='This is test case job from Airflow',
            region_name=self.some_aws_region
        ).initialize_job(some_script_arguments)
        self.assertEqual(glue_job_run_state, mock_job_run_state, msg='Mocks but be equal')


if __name__ == '__main__':
    unittest.main()
