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

from airflow.contrib.operators.adls_list_operator import AzureDataLakeStorageListOperator
from tests.compat import mock

TASK_ID = 'test-adls-list-operator'
TEST_PATH = 'test/*'
MOCK_FILES = ["test/TEST1.csv", "test/TEST2.csv", "test/path/TEST3.csv",
              "test/path/PARQUET.parquet", "test/path/PIC.png"]


class AzureDataLakeStorageListOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.adls_list_operator.AzureDataLakeHook')
    def test_execute(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES

        operator = AzureDataLakeStorageListOperator(task_id=TASK_ID,
                                                    path=TEST_PATH)

        files = operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            path=TEST_PATH
        )
        self.assertEqual(sorted(files), sorted(MOCK_FILES))
