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

import re
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.exceptions import (
    DagErrorHandler,
    _DatabaseDialect,
    _UniqueConstraintErrorHandler,
)
from airflow.configuration import conf
from airflow.exceptions import DeserializationError
from airflow.models import DagRun, Pool, Variable
from airflow.models.dagbag import DBDagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.compat import EmptyOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, clear_db_dags, clear_db_pools, clear_db_runs
from tests_common.test_utils.version_compat import SQLALCHEMY_V_1_4

if TYPE_CHECKING:
    from tests_common.pytest_plugin import DagMaker
pytestmark = pytest.mark.db_test

CURRENT_DATABASE_DIALECT = conf.get_mandatory_value("database", "sql_alchemy_conn").lower()
TEST_POOL = "test_pool"
TEST_VARIABLE_KEY = "test_key"
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
MOCKED_ID = "TgVcT3QW"
MESSAGE = (
    "Serious error when handling your request. Check logs for more details - "
    f"you will find it in api server when you look for ID {MOCKED_ID}"
)


def generate_test_cases_parametrize(
    test_cases: list[str], expected_exceptions_with_dialects: list[list[HTTPException]]
):
    """Generate cross product of test cases for parametrize with different database dialects."""
    generated_test_cases = []
    for test_case, expected_exception_with_dialects in zip(test_cases, expected_exceptions_with_dialects):
        for mark, expected_exception in zip(PYTEST_MARKS_DB_DIALECT, expected_exception_with_dialects):
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
        ("table", "expected_exception"),
        generate_test_cases_parametrize(
            ["Pool", "Variable"],
            [
                [  # Pool
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred, team_name) VALUES (?, ?, ?, ?, ?)",
                            "orig_error": "UNIQUE constraint failed: slot_pool.pool",
                            "message": MESSAGE,
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred, team_name) VALUES (%s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_pool' for key 'slot_pool.slot_pool_pool_uq'\")",
                            "message": MESSAGE,
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred, team_name) VALUES (%(pool)s, %(slots)s, %(description)s, %(include_deferred)s, %(team_name)s) RETURNING slot_pool.id",
                            "orig_error": 'duplicate key value violates unique constraint "slot_pool_pool_uq"\nDETAIL:  Key (pool)=(test_pool) already exists.\n',
                            "message": MESSAGE,
                        },
                    ),
                ],
                [  # Variable
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": 'INSERT INTO variable ("key", val, description, is_encrypted, team_name) VALUES (?, ?, ?, ?, ?)',
                            "orig_error": "UNIQUE constraint failed: variable.key",
                            "message": MESSAGE,
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO variable (`key`, val, description, is_encrypted, team_name) VALUES (%s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_key' for key 'variable.variable_key_uq'\")",
                            "message": MESSAGE,
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO variable (key, val, description, is_encrypted, team_name) VALUES (%(key)s, %(val)s, %(description)s, %(is_encrypted)s, %(team_name)s) RETURNING variable.id",
                            "orig_error": 'duplicate key value violates unique constraint "variable_key_uq"\nDETAIL:  Key (key)=(test_key) already exists.\n',
                            "message": MESSAGE,
                        },
                    ),
                ],
            ],
        ),
    )
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "False"})
    @provide_session
    def test_handle_single_column_unique_constraint_error(
        self,
        mock_get_random_string,
        session,
        table,
        expected_exception,
    ) -> None:
        # Take Pool and Variable tables as test cases
        # Note: SQLA2 uses a more optimized bulk insert strategy when multiple objects are added to the
        #   session. Instead of individual INSERT statements, a single INSERT with the SELECT FROM VALUES
        #   pattern is used.
        if table == "Pool":
            session.add(Pool(pool=TEST_POOL, slots=1, description="test pool", include_deferred=False))
            session.flush()  # Avoid SQLA2.0 bulk insert optimization
            session.add(Pool(pool=TEST_POOL, slots=1, description="test pool", include_deferred=False))
        elif table == "Variable":
            session.add(Variable(key=TEST_VARIABLE_KEY, val="test_val"))
            session.flush()
            session.add(Variable(key=TEST_VARIABLE_KEY, val="test_val"))

        with pytest.raises(IntegrityError) as exeinfo_integrity_error:
            session.commit()

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.unique_constraint_error_handler.exception_handler(None, exeinfo_integrity_error.value)  # type: ignore

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

    @pytest.mark.parametrize(
        ("table", "expected_exception"),
        generate_test_cases_parametrize(
            ["DagRun"],
            [
                [  # DagRun
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, triggering_user_name, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id, partition_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT max(log_template.id) AS max_1 \nFROM log_template), ?, ?, ?, ?, ?, ?, ?, ?)",
                            "orig_error": "UNIQUE constraint failed: dag_run.dag_id, dag_run.run_id",
                            "message": MESSAGE,
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, triggering_user_name, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id, partition_key) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT max(log_template.id) AS max_1 \nFROM log_template), %s, %s, %s, %s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_dag_id-test_run_id' for key 'dag_run.dag_run_dag_id_run_id_key'\")",
                            "message": MESSAGE,
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, triggering_user_name, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id, partition_key) VALUES (%(dag_id)s, %(queued_at)s, %(logical_date)s, %(start_date)s, %(end_date)s, %(state)s, %(run_id)s, %(creating_job_id)s, %(run_type)s, %(triggered_by)s, %(triggering_user_name)s, %(conf)s, %(data_interval_start)s, %(data_interval_end)s, %(run_after)s, %(last_scheduling_decision)s, (SELECT max(log_template.id) AS max_1 \nFROM log_template), %(updated_at)s, %(clear_number)s, %(backfill_id)s, %(bundle_version)s, %(scheduled_by_job_id)s, %(context_carrier)s, %(created_dag_version_id)s, %(partition_key)s) RETURNING dag_run.id",
                            "orig_error": 'duplicate key value violates unique constraint "dag_run_dag_id_run_id_key"\nDETAIL:  Key (dag_id, run_id)=(test_dag_id, test_run_id) already exists.\n',
                            "message": MESSAGE,
                        },
                    ),
                ],
            ],
        ),
    )
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "False"})
    @provide_session
    def test_handle_multiple_columns_unique_constraint_error(
        self,
        mock_get_random_string,
        session,
        table,
        expected_exception,
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

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        if SQLALCHEMY_V_1_4:
            assert exeinfo_response_error.value.detail == expected_exception.detail
        else:
            # The SQL statement is an implementation detail, so we match on the statement pattern (contains
            # the table name and is an INSERT) instead of insisting on an exact match.
            response_detail = exeinfo_response_error.value.detail
            expected_detail = expected_exception.detail
            actual_statement = response_detail.pop("statement", None)  # type: ignore[attr-defined]
            expected_detail.pop("statement", None)

            assert response_detail == expected_detail
            assert "INSERT INTO dag_run" in actual_statement
        assert exeinfo_response_error.value.detail == expected_exception.detail


class TestDagErrorHandler:
    @pytest.mark.parametrize(
        "cause",
        [
            RuntimeError("Error during Dag serialization process"),
            KeyError("required_field"),
            ValueError("Missing Dag ID in serialized Dag"),
        ],
        ids=[
            "RuntimeError",
            "KeyError",
            "ValueError",
        ],
    )
    def test_handle_deserialization_error(self, cause: Exception) -> None:
        deserialization_error = DeserializationError("test_dag_id")
        deserialization_error.__cause__ = cause

        expected_exception = HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while trying to deserialize Dag: {deserialization_error}",
        )

        with pytest.raises(HTTPException, match=re.escape(expected_exception.detail)):
            DagErrorHandler().exception_handler(Mock(), deserialization_error)

    @pytest.mark.usefixtures("testing_dag_bundle")
    @pytest.mark.need_serialized_dag
    def test_handle_real_dag_deserialization_error(self, session: Session, dag_maker: DagMaker) -> None:
        """Test handling a real Dag deserialization error with actual serialized Dag."""
        dag_id = "test_dag"
        with dag_maker(dag_id, serialized=True):
            EmptyOperator(task_id="task_1")

        s_dag_model = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))
        assert s_dag_model is not None
        assert s_dag_model.data is not None

        data = s_dag_model.data
        del data["dag"]["dag_id"]
        session.execute(
            update(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id).values(_data=data)
        )
        session.commit()

        dag_bag = DBDagBag()
        with pytest.raises(DeserializationError) as exc_info:
            dag_bag.get_latest_version_of_dag(dag_id, session=session)

        with pytest.raises(
            HTTPException,
            match=re.escape(f"An error occurred while trying to deserialize Dag: {exc_info.value}"),
        ):
            DagErrorHandler().exception_handler(Mock(), exc_info.value)
