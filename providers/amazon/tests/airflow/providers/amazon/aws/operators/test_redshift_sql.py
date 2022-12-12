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

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler


class TestRedshiftSQLOperator:
    @pytest.mark.parametrize("test_autocommit, test_parameters", [(True, ("a", "b")), (False, ("c", "d"))])
    @mock.patch("airflow.providers.amazon.aws.operators.redshift_sql.RedshiftSQLOperator.get_db_hook")
    def test_redshift_operator(self, mock_get_hook, test_autocommit, test_parameters):
        hook = MagicMock()
        mock_run = hook.run
        mock_get_hook.return_value = hook
        sql = MagicMock()
        operator = RedshiftSQLOperator(
            task_id="test", sql=sql, autocommit=test_autocommit, parameters=test_parameters
        )
        operator.execute(None)
        mock_run.assert_called_once_with(
            sql=sql,
            autocommit=test_autocommit,
            parameters=test_parameters,
            handler=fetch_all_handler,
            return_last=True,
            split_statements=False,
        )
