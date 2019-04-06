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

from mock import patch
import unittest

from airflow.contrib.operators.presto_operator import PrestoOperator


class TestPrestoOperator(unittest.TestCase):

    @patch('airflow.contrib.operators.presto_operator.PrestoHook')
    def test_presto_operator(self, mock_presto_hook):
        hql = 'select 1'
        presto_operator = PrestoOperator(hql=hql, task_id='test_presto_operator')

        presto_operator.execute(context=None)

        mock_presto_hook.return_value.run.assert_called_once_with(hql)


if __name__ == '__main__':
    unittest.main()
