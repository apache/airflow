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

import unittest

import mock

from airflow.contrib.operators.s3_list_operator import S3ListOperator

TASK_ID = 'test-s3-list-operator'
BUCKET = 'test-bucket'
DELIMITER = '.csv'
PREFIX = 'TEST'
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]


class TestS3ListOperator(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.s3_list_operator.S3Hook')
    def test_execute(self, mock_hook):

        mock_hook.return_value.list_keys.return_value = MOCK_FILES

        operator = S3ListOperator(
            task_id=TASK_ID, bucket=BUCKET, prefix=PREFIX, delimiter=DELIMITER)

        files = operator.execute(None)

        mock_hook.return_value.list_keys.assert_called_once_with(
            bucket_name=BUCKET, prefix=PREFIX, delimiter=DELIMITER)
        self.assertEqual(sorted(files), sorted(MOCK_FILES))


if __name__ == '__main__':
    unittest.main()
