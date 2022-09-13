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
from __future__ import annotations

import unittest
from unittest import mock
from unittest.mock import MagicMock

from parameterized import parameterized

from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator


class TestRedshiftSQLOperator(unittest.TestCase):
    @parameterized.expand([(True, ('a', 'b')), (False, ('c', 'd'))])
    @mock.patch("airflow.providers.amazon.aws.operators.redshift_sql.RedshiftSQLOperator.get_hook")
    def test_redshift_operator(self, test_autocommit, test_parameters, mock_get_hook):
        hook = MagicMock()
        mock_run = hook.run
        mock_get_hook.return_value = hook
        sql = MagicMock()
        operator = RedshiftSQLOperator(
            task_id='test', sql=sql, autocommit=test_autocommit, parameters=test_parameters
        )
        operator.execute(None)
        mock_run.assert_called_once_with(
            sql,
            autocommit=test_autocommit,
            parameters=test_parameters,
        )
