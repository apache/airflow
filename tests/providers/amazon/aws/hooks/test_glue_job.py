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
import json
import unittest

import mock

from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook

try:
    from moto import mock_iam
except ImportError:
    mock_iam = None


class TestGlueJobHook(unittest.TestCase):
    def setUp(self):
        self.some_aws_region = "us-west-2"

    @unittest.skipIf(mock_iam is None, 'mock_iam package not present')
    @mock_iam
    def test_get_iam_execution_role(self):
        hook = AwsGlueJobHook(job_name='aws_test_glue_job',
                              s3_bucket='some_bucket',
                              iam_role_name='my_test_role')
        iam_role = hook.get_client_type('iam').create_role(
            Path="/",
            RoleName='my_test_role',
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            })
        )
        iam_role = hook.get_iam_execution_role()
        self.assertIsNotNone(iam_role)

    @mock.patch.object(AwsGlueJobHook, "get_iam_execution_role")
    @mock.patch.object(AwsGlueJobHook, "get_conn")
    def test_get_or_create_glue_job(self, mock_get_conn,
                                    mock_get_iam_execution_role
                                    ):
        mock_get_iam_execution_role.return_value = \
            mock.MagicMock(Role={'RoleName': 'my_test_role'})
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"

        mock_glue_job = mock_get_conn.return_value.get_job()['Job']['Name']
        glue_job = AwsGlueJobHook(job_name='aws_test_glue_job',
                                  desc='This is test case job from Airflow',
                                  script_location=some_script,
                                  iam_role_name='my_test_role',
                                  s3_bucket=some_s3_bucket,
                                  region_name=self.some_aws_region)\
            .get_or_create_glue_job()
        self.assertEqual(glue_job, mock_glue_job)

    @mock.patch.object(AwsGlueJobHook, "job_completion")
    @mock.patch.object(AwsGlueJobHook, "get_or_create_glue_job")
    @mock.patch.object(AwsGlueJobHook, "get_conn")
    def test_initialize_job(self, mock_get_conn,
                            mock_get_or_create_glue_job,
                            mock_completion):
        some_data_path = "s3://glue-datasets/examples/medicare/SampleData.csv"
        some_script_arguments = {"--s3_input_data_path": some_data_path}
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"

        mock_get_or_create_glue_job.Name = mock.Mock(Name='aws_test_glue_job')
        mock_get_conn.return_value.start_job_run()

        mock_job_run_state = mock_completion.return_value
        glue_job_run_state = AwsGlueJobHook(job_name='aws_test_glue_job',
                                            desc='This is test case job from Airflow',
                                            iam_role_name='my_test_role',
                                            script_location=some_script,
                                            s3_bucket=some_s3_bucket,
                                            region_name=self.some_aws_region)\
            .initialize_job(some_script_arguments)
        self.assertEqual(glue_job_run_state, mock_job_run_state, msg='Mocks but be equal')


if __name__ == '__main__':
    unittest.main()
