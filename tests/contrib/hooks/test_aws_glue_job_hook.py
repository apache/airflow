# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import botocore.session
from botocore.stub import Stubber

from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook


class TestGlueJobHook(unittest.TestCase):

    def setUp(self):
        self.glue_client = botocore.session.get_session().create_client('glue')
        self.some_aws_region = "us-west-2"
        self.stubber = Stubber(self.glue_client)

    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsGlueJobHook(job_name='aws_test_glue_job')
        self.assertIsNotNone(hook.get_conn())

    def test_initiate_job_returns_job_status(self):
        some_script_path = "../utils/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"
        some_data_path = "s3://my-includes/glue-datasets/examples/medicare/SampleData.csv"
        some_script_arguments = {"--s3_input_data_path": some_data_path}
        hook = AwsGlueJobHook(job_name='aws_test_glue_job',
                              desc='This is test case job from Airflow',
                              concurrent_run_limit=2,
                              script_location=some_script_path,
                              s3_bucket=some_s3_bucket,
                              region_name=self.some_aws_region)

        job_status = hook.initialize_job(script_arguments=some_script_arguments)
        hook.job_tear_down()
        print(job_status, " checking status")
        self.assertEqual(job_status, "FAILED", msg="AWS Glue job status must be 'FAILED'")


if __name__ == '__main__':
    unittest.main()
