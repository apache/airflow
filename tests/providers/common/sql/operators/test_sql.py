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

import pandas as pd
import pytest

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator


class MockHook:
    def get_first(self):
        return

    def get_pandas_df(self):
        return


def _get_mock_db_hook():
    return MockHook()


class TestColumnCheckOperator:

    valid_column_mapping = {
        "X": {
            "null_check": {"equal_to": 0},
            "distinct_check": {"equal_to": 10, "tolerance": 0.1},
            "unique_check": {"geq_to": 10},
            "min": {"leq_to": 1},
            "max": {"less_than": 20, "greater_than": 10},
        }
    }

    invalid_column_mapping = {"Y": {"invalid_check_name": {"expectation": 5}}}

    def _construct_operator(self, monkeypatch, column_mapping, return_vals):
        def get_first_return(*arg):
            return return_vals

        operator = SQLColumnCheckOperator(
            task_id="test_task", table="test_table", column_mapping=column_mapping
        )
        monkeypatch.setattr(operator, "get_db_hook", _get_mock_db_hook)
        monkeypatch.setattr(MockHook, "get_first", get_first_return)
        return operator

    def test_check_not_in_column_checks(self, monkeypatch):
        with pytest.raises(AirflowException, match="Invalid column check: invalid_check_name."):
            self._construct_operator(monkeypatch, self.invalid_column_mapping, ())

    def test_pass_all_checks_exact_check(self, monkeypatch):
        operator = self._construct_operator(monkeypatch, self.valid_column_mapping, (0, 10, 10, 1, 19))
        operator.execute()

    def test_max_less_than_fails_check(self, monkeypatch):
        with pytest.raises(AirflowException):
            operator = self._construct_operator(monkeypatch, self.valid_column_mapping, (0, 10, 10, 1, 21))
            operator.execute()
            assert operator.column_mapping["X"]["max"]["success"] is False

    def test_max_greater_than_fails_check(self, monkeypatch):
        with pytest.raises(AirflowException):
            operator = self._construct_operator(monkeypatch, self.valid_column_mapping, (0, 10, 10, 1, 9))
            operator.execute()
            assert operator.column_mapping["X"]["max"]["success"] is False

    def test_pass_all_checks_inexact_check(self, monkeypatch):
        operator = self._construct_operator(monkeypatch, self.valid_column_mapping, (0, 9, 12, 0, 15))
        operator.execute()

    def test_fail_all_checks_check(self, monkeypatch):
        operator = operator = self._construct_operator(
            monkeypatch, self.valid_column_mapping, (1, 12, 11, -1, 20)
        )
        with pytest.raises(AirflowException):
            operator.execute()


class TestTableCheckOperator:

    checks = {
        "row_count_check": {"check_statement": "COUNT(*) == 1000"},
        "column_sum_check": {"check_statement": "col_a + col_b < col_c"},
    }

    def _construct_operator(self, monkeypatch, checks, return_df):
        def get_pandas_df_return(*arg):
            return return_df

        operator = SQLTableCheckOperator(task_id="test_task", table="test_table", checks=checks)
        monkeypatch.setattr(operator, "get_db_hook", _get_mock_db_hook)
        monkeypatch.setattr(MockHook, "get_pandas_df", get_pandas_df_return)
        return operator

    def test_pass_all_checks_check(self, monkeypatch):
        df = pd.DataFrame(
            data={
                "check_name": ["row_count_check", "column_sum_check"],
                "check_result": [
                    "1",
                    "y",
                ],
            }
        )
        operator = self._construct_operator(monkeypatch, self.checks, df)
        operator.execute()

    def test_fail_all_checks_check(self, monkeypatch):
        df = pd.DataFrame(
            data={"check_name": ["row_count_check", "column_sum_check"], "check_result": ["0", "n"]}
        )
        operator = self._construct_operator(monkeypatch, self.checks, df)
        with pytest.raises(AirflowException):
            operator.execute()
