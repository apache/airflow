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

from unittest import mock
import unittest

from airflow.contrib.operators.vertica_operator import VerticaOperator


class TestVerticaOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.vertica_operator.VerticaHook')
    def test_execute(self, mock_hook):
        sql = "select a, b, c"
        op = VerticaOperator(task_id='test_task_id',
                             sql=sql)
        op.execute(None)
        mock_hook.return_value.run.assert_called_once_with(
            sql=sql
        )


if __name__ == '__main__':
    unittest.main()
