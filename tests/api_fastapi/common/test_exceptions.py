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
from airflow.models import Pool, Variable
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_connections, clear_db_pools

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

# Take Pool and Variable tables as test cases
TEST_CASES_TABLES = [
    "Pool",
    "Variable",
]
TEST_CASES_EXCEPTIONS = [
    EXPECTED_EXCEPTION_POOL,
    EXPECTED_EXCEPTION_VARIABLE,
]
TEST_CASE_DB_DIALECTS_SKIP = [
    {
        "condition": CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.MYSQL.value)
        or CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.POSTGRESQL.value),
        "reason": f"Test for {_DatabaseDialect.SQLITE.value} only",
    },
    {
        "condition": CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.SQLITE.value)
        or CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.POSTGRESQL.value),
        "reason": f"Test for {_DatabaseDialect.MYSQL.value} only",
    },
    {
        "condition": CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.SQLITE.value)
        or CURRENT_DATABASE_DIALECT.startswith(_DatabaseDialect.MYSQL.value),
        "reason": f"Test for {_DatabaseDialect.POSTGRESQL.value} only",
    },
]


def generate_test_cases_parametrize():
    """Generate cross product of test cases for parametrize."""
    test_cases = []
    for table, expected_exception in zip(TEST_CASES_TABLES, TEST_CASES_EXCEPTIONS):
        for test_case in TEST_CASE_DB_DIALECTS_SKIP:
            test_cases.append(
                pytest.param(
                    table,
                    expected_exception,
                    id=f"{test_case['reason']} - Inserting duplicate entry in {table}",
                    marks=pytest.mark.skipif(**test_case),
                )
            )
    return test_cases


class TestUniqueConstraintErrorHandler:
    unique_constraint_error_handler = _UniqueConstraintErrorHandler()

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_connections(add_default_connections_back=False)
        clear_db_pools()

    def teardown_method(self) -> None:
        clear_db_connections()
        clear_db_pools()

    @pytest.mark.parametrize(
        "table, expected_exception",
        generate_test_cases_parametrize(),
    )
    @provide_session
    def test_handle_unique_constraint_error(self, session, table, expected_exception) -> None:
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
