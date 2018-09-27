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

from airflow.operators.postgres_operator import PostgresOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestPostgresOperator(unittest.TestCase):

    @mock.patch('airflow.operators.postgres_operator.PostgresHook')
    def test_execute(self, mock_hook):
        operator = PostgresOperator(
            task_id='postgres',
            sql='select 1',
        )

        operator.execute(None)
        mock_hook.assert_called_once_with(
            postgres_conn_id='postgres_default',
            schema=None,
        )
        mock_hook.return_value.run.assert_called_once_with(
            'select 1',
            False,
            parameters=None,
        )
