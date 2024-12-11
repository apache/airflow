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

import pytest
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError

from airflow.api_fastapi.common.exceptions import _DatabaseDialect, _UniqueConstraintErrorHandler
from airflow.configuration import conf
from airflow.models import DagRun, Pool, Variable
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import clear_db_connections, clear_db_dags, clear_db_pools, clear_db_runs

pytestmark = pytest.mark.db_test

CURRENT_DATABASE_DIALECT = conf.get_mandatory_value("database", "sql_alchemy_conn").lower()
TEST_POOL = "test_pool"
TEST_VARIABLE_KEY = "test_key"
EXPECTED_EXCEPTION_POOL = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail="Unique constraint violation: slot_pool with pool=test_pool already exists",
)

EXPECTED_EXCEPTION_VARIABLE = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail="Unique constraint violation: variable with key=test_key already exists",
)
EXPECTED_EXCEPTION_DAG_RUN = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail="Unique constraint violation: dag_run with dag_id=test_dag_id, run_id=test_run_id already exists",
)
PYTEST_MARKS_DB_DIALECT = [
    {
        "condition": CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.MYSQL.value)
        or CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.POSTGRES.value),
        "reason": f"Test for {_DatabaseDialect.SQLITE.value} only",
    },
    {
        "condition": CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.SQLITE.value)
        or CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.POSTGRES.value),
        "reason": f"Test for {_DatabaseDialect.MYSQL.value} only",
    },
    {
        "condition": CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.SQLITE.value)
        or CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.MYSQL.value),
        "reason": f"Test for {_DatabaseDialect.POSTGRES.value} only",
    },
]


def generate_test_cases_parametrize(test_cases: list[str], expected_exceptions: list[HTTPException]):
    """Generate cross product of test cases for parametrize with different database dialects."""
    generated_test_cases = []
    for test_case, expected_exception in zip(test_cases, expected_exceptions):
        for mark in PYTEST_MARKS_DB_DIALECT:
            generated_test_cases.append(
                pytest.param(
                    test_case,
                    expected_exception,
                    id=f"{mark['reason']} - {test_case}",
                    marks=pytest.mark.skipif(**mark),  # type: ignore
                )
            )
    return generated_test_cases


def get_unique_constraint_error_prefix():
    """Get unique constraint error prefix based on current database dialect."""
    if CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.SQLITE.value):
        return _UniqueConstraintErrorHandler.unique_constraint_error_prefix_dict[_DatabaseDialect.SQLITE]
    if CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.MYSQL.value):
        return _UniqueConstraintErrorHandler.unique_constraint_error_prefix_dict[_DatabaseDialect.MYSQL]
    if CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.POSTGRES.value):
        return _UniqueConstraintErrorHandler.unique_constraint_error_prefix_dict[_DatabaseDialect.POSTGRES]
    return ""


class TestUniqueConstraintErrorHandler:
    unique_constraint_error_handler = _UniqueConstraintErrorHandler()

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_connections(add_default_connections_back=False)
        clear_db_pools()
        clear_db_runs()
        clear_db_dags()

    def teardown_method(self) -> None:
        clear_db_connections()
        clear_db_pools()
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        "table, expected_exception",
        generate_test_cases_parametrize(
            ["Pool", "Variable"],
            [
                EXPECTED_EXCEPTION_POOL,
                EXPECTED_EXCEPTION_VARIABLE,
            ],
        ),
    )
    @provide_session
    def test_handle_single_column_unique_constraint_error(self, session, table, expected_exception) -> None:
        # Take Pool and Variable tables as test cases
        if table == "Pool":
            session.add(Pool(pool=TEST_POOL, slots=1, description="test pool", include_deferred=False))
            session.add(Pool(pool=TEST_POOL, slots=1, description="test pool", include_deferred=False))
        elif table == "Variable":
            session.add(Variable(key=TEST_VARIABLE_KEY, val="test_val"))
            session.add(Variable(key=TEST_VARIABLE_KEY, val="test_val"))

        with pytest.raises(IntegrityError) as exeinfo_integrity_error:
            session.commit()

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.unique_constraint_error_handler.exception_handler(None, exeinfo_integrity_error.value)  # type: ignore

        assert str(exeinfo_response_error.value) == str(expected_exception)

    @pytest.mark.parametrize(
        "table, expected_exception",
        generate_test_cases_parametrize(
            ["DagRun"],
            [EXPECTED_EXCEPTION_DAG_RUN],
        ),
    )
    @provide_session
    def test_handle_multiple_columns_unique_constraint_error(
        self, session, table, expected_exception
    ) -> None:
        if table == "DagRun":
            session.add(
                DagRun(
                    dag_id="test_dag_id", run_id="test_run_id", run_type="manual", state=DagRunState.RUNNING
                )
            )
            session.add(
                DagRun(
                    dag_id="test_dag_id", run_id="test_run_id", run_type="manual", state=DagRunState.RUNNING
                )
            )

        with pytest.raises(IntegrityError) as exeinfo_integrity_error:
            session.commit()

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.unique_constraint_error_handler.exception_handler(None, exeinfo_integrity_error.value)  # type: ignore

        assert str(exeinfo_response_error.value) == str(expected_exception)

    @pytest.mark.parametrize(
        "mock_error_orig, expected_exception",
        generate_test_cases_parametrize(
            [
                f"{get_unique_constraint_error_prefix()} This is a string that will cause parse error in exception handler",
                f"{get_unique_constraint_error_prefix()} This_is_another_string_that_will_cause_parse_error_in_exception_handler",
            ],
            [
                HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Unique constraint violation: {get_unique_constraint_error_prefix()} This is a string that will cause parse error in exception handler",
                ),
                HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Unique constraint violation: {get_unique_constraint_error_prefix()} This_is_another_string_that_will_cause_parse_error_in_exception_handler",
                ),
            ],
        ),
    )
    def test_handle_error_parsing_message_unique_constraint_error(
        self, mock_error_orig, expected_exception
    ) -> None:
        mock_integrity_error = IntegrityError(
            statement="mock statement",
            params={},
            orig=Exception(mock_error_orig),
        )
        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.unique_constraint_error_handler.exception_handler(None, mock_integrity_error)  # type: ignore

        assert str(exeinfo_response_error.value) == str(expected_exception)
