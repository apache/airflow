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

from airflow.providers.amazon.aws.hooks.cloud_formation import AWSCloudFormationHook

try:
    from moto import mock_cloudformation
except ImportError:
    mock_cloudformation = None


@unittest.skipIf(mock_cloudformation is None, 'moto package not present')
class TestAWSCloudFormationHook(unittest.TestCase):
    @mock_cloudformation
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AWSCloudFormationHook(aws_conn_id='aws_default')
        self.assertIsNotNone(hook.get_conn().describe_stacks())


if __name__ == '__main__':
    unittest.main()
