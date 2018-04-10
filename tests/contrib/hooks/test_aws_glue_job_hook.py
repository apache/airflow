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

from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook

try:
    from moto import mock_glue
except ImportError:
    mock_glue = None


class TestGlueJobHook(unittest.TestCase):

    @unittest.skipIf(mock_glue is None, 'mock_glue package not present')
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsGlueJobHook(job_name='aws_test_glue_job')
        self.assertIsNotNone(hook.get_conn())

    @unittest.skipIf(mock_glue is None, 'mock_glue package not present')
    def test_initialize_glue_job(self):
        hook = AwsGlueJobHook(job_name='aws_test_glue_job',
                              concurrent_run_limit=2,
                              script_location="")
        self.assertIsNotNone(hook.initialize_job())


if __name__ == '__main__':
    unittest.main()
