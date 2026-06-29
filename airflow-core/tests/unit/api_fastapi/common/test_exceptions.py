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
from fastapi import FastAPI, HTTPException, status
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.exceptions import (
    ERROR_HANDLERS,
    DagErrorHandler,
    DataErrorHandler,
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
        [
            [
                "Pool",
                HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "reason": "Unique constraint violation",
                        "statement": "hidden",
                        "orig_error": "hidden",
                        "message": MESSAGE,
                    },
                ),
            ],
            [
                "Variable",
                HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "reason": "Unique constraint violation",
                        "statement": "hidden",
                        "orig_error": "hidden",
                        "message": MESSAGE,
                    },
                ),
            ],
        ],
    )
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "False"})
    @provide_session
    def test_handle_single_column_unique_constraint_error_without_stacktrace(
        self,
        mock_get_random_string,
        table,
        expected_exception,
        *,
        session: Session,
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
            ["Pool", "Variable"],
            [
                [  # Pool
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred, team_name) VALUES (?, ?, ?, ?, ?)",
                            "orig_error": "UNIQUE constraint failed: slot_pool.pool",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred, team_name) VALUES (%s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_pool' for key 'slot_pool.slot_pool_pool_uq'\")",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred, team_name) VALUES (%(pool)s, %(slots)s, %(description)s, %(include_deferred)s, %(team_name)s) RETURNING slot_pool.id",
                            "orig_error": 'duplicate key value violates unique constraint "slot_pool_pool_uq"\nDETAIL:  Key (pool)=(test_pool) already exists.\n',
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
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO variable (`key`, val, description, is_encrypted, team_name) VALUES (%s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_key' for key 'variable.variable_key_uq'\")",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO variable (key, val, description, is_encrypted, team_name) VALUES (%(key)s, %(val)s, %(description)s, %(is_encrypted)s, %(team_name)s) RETURNING variable.id",
                            "orig_error": 'duplicate key value violates unique constraint "variable_key_uq"\nDETAIL:  Key (key)=(test_key) already exists.\n',
                        },
                    ),
                ],
            ],
        ),
    )
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "True"})
    @provide_session
    def test_handle_single_column_unique_constraint_error_with_stacktrace(
        self,
        mock_get_random_string,
        table,
        expected_exception,
        *,
        session: Session,
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

        exeinfo_response_error.value.detail.pop("message", None)  # type: ignore[attr-defined]
        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "False"})
    @provide_session
    def test_handle_multiple_columns_unique_constraint_error_without_stacktrace(
        self,
        mock_get_random_string,
        *,
        session: Session,
    ) -> None:
        expected_exception = HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "Unique constraint violation",
                "statement": "hidden",
                "orig_error": "hidden",
                "message": MESSAGE,
            },
        )
        session.add(
            DagRun(dag_id="test_dag_id", run_id="test_run_id", run_type="manual", state=DagRunState.RUNNING)
        )
        session.add(
            DagRun(dag_id="test_dag_id", run_id="test_run_id", run_type="manual", state=DagRunState.RUNNING)
        )
        with pytest.raises(IntegrityError) as exeinfo_integrity_error:
            session.commit()

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.unique_constraint_error_handler.exception_handler(None, exeinfo_integrity_error.value)  # type: ignore

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        # The SQL statement is an implementation detail, so we match on the statement pattern (contains
        # the table name and is an INSERT) instead of insisting on an exact match.
        response_detail = exeinfo_response_error.value.detail
        expected_detail = expected_exception.detail

        assert response_detail == expected_detail
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
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, triggering_user_name, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id, partition_key) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT max(log_template.id) AS max_1 \nFROM log_template), %s, %s, %s, %s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_dag_id-test_run_id' for key 'dag_run.dag_run_dag_id_run_id_key'\")",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, triggering_user_name, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id, partition_key) VALUES (%(dag_id)s, %(queued_at)s, %(logical_date)s, %(start_date)s, %(end_date)s, %(state)s, %(run_id)s, %(creating_job_id)s, %(run_type)s, %(triggered_by)s, %(triggering_user_name)s, %(conf)s, %(data_interval_start)s, %(data_interval_end)s, %(run_after)s, %(last_scheduling_decision)s, (SELECT max(log_template.id) AS max_1 \nFROM log_template), %(updated_at)s, %(clear_number)s, %(backfill_id)s, %(bundle_version)s, %(scheduled_by_job_id)s, %(context_carrier)s, %(created_dag_version_id)s, %(partition_key)s) RETURNING dag_run.id",
                            "orig_error": 'duplicate key value violates unique constraint "dag_run_dag_id_run_id_key"\nDETAIL:  Key (dag_id, run_id)=(test_dag_id, test_run_id) already exists.\n',
                        },
                    ),
                ],
            ],
        ),
    )
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "True"})
    @provide_session
    def test_handle_multiple_columns_unique_constraint_error_with_stacktrace(
        self,
        mock_get_random_string,
        table,
        expected_exception,
        *,
        session: Session,
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
        # The SQL statement is an implementation detail, so we match on the statement pattern (contains
        # the table name and is an INSERT) instead of insisting on an exact match.
        response_detail = exeinfo_response_error.value.detail
        expected_detail = expected_exception.detail
        actual_statement = response_detail.pop("statement", None)  # type: ignore[attr-defined]
        expected_detail.pop("statement", None)

        # Removes the stacktrace from response to remove during comparison.
        response_detail.pop("message", None)  # type: ignore[attr-defined]
        assert response_detail == expected_detail
        assert "INSERT INTO dag_run" in actual_statement
        assert exeinfo_response_error.value.detail == expected_exception.detail


class TestDataErrorHandler:
    handler = DataErrorHandler()

    _STATEMENT = "INSERT INTO dag_run (conf) VALUES (?)"

    # One representative ``orig`` message per dialect / rejection class. The handler is
    # dialect-agnostic (it translates every DataError the same way), so the value only
    # needs to round-trip through ``orig_error`` when stacktrace exposure is on.
    _ORIG_MSGS = [
        pytest.param(
            "(1406, \"Data too long for column 'conf' at row 1\")",
            id="mysql-1406-data-too-long",
        ),
        pytest.param(
            "value too long for type character varying(250)",
            id="postgres-value-too-long",
        ),
        pytest.param(
            "string or blob too big",
            id="sqlite-blob-too-big",
        ),
        pytest.param(
            "(1264, \"Out of range value for column 'slots' at row 1\")",
            id="mysql-1264-out-of-range",
        ),
        pytest.param(
            "numeric field overflow",
            id="postgres-numeric-field-overflow",
        ),
        pytest.param(
            "(1366, \"Incorrect integer value: 'abc' for column 'slots' at row 1\")",
            id="mysql-1366-incorrect-value",
        ),
        pytest.param(
            "invalid input syntax for type integer",
            id="postgres-invalid-input-syntax",
        ),
    ]

    @staticmethod
    def _make_data_error(orig_msg: str) -> DataError:
        return DataError(
            statement=TestDataErrorHandler._STATEMENT,
            params={},
            orig=Exception(orig_msg),
        )

    @pytest.mark.parametrize("orig_msg", _ORIG_MSGS)
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "False"})
    def test_data_error_hides_db_internals_without_stacktrace(
        self,
        mock_get_random_string,
        orig_msg: str,
    ) -> None:
        exc = self._make_data_error(orig_msg)
        with pytest.raises(HTTPException) as exc_info:
            self.handler.exception_handler(Mock(), exc)
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert exc_info.value.detail == {
            "reason": "Value rejected by database",
            "statement": "hidden",
            "orig_error": "hidden",
            "message": MESSAGE,
        }

    @pytest.mark.parametrize("orig_msg", _ORIG_MSGS)
    @patch("airflow.api_fastapi.common.exceptions.get_random_string", return_value=MOCKED_ID)
    @conf_vars({("api", "expose_stacktrace"): "True"})
    def test_data_error_exposes_db_internals_with_stacktrace(
        self,
        mock_get_random_string,
        orig_msg: str,
    ) -> None:
        exc = self._make_data_error(orig_msg)
        with pytest.raises(HTTPException) as exc_info:
            self.handler.exception_handler(Mock(), exc)
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["reason"] == "Value rejected by database"
        assert detail["statement"] == self._STATEMENT
        assert detail["orig_error"] == orig_msg

    @conf_vars({("api", "expose_stacktrace"): "False"})
    def test_data_error_dispatched_through_fastapi_app(self) -> None:
        """End-to-end: a route raising DataError returns 422 via the registered handler."""
        app = FastAPI()
        for h in ERROR_HANDLERS:
            app.add_exception_handler(h.exception_cls, h.exception_handler)

        @app.post("/test")
        def trigger_data_error():
            raise self._make_data_error("(1406, \"Data too long for column 'conf' at row 1\")")

        response = TestClient(app, raise_server_exceptions=False).post("/test")
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        detail = response.json()["detail"]
        assert detail["reason"] == "Value rejected by database"
        assert detail["statement"] == "hidden"
        assert detail["orig_error"] == "hidden"


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
    @conf_vars({("api", "expose_stacktrace"): "True"})
    def test_handle_deserialization_error(self, cause: Exception) -> None:
        deserialization_error = DeserializationError("test_dag_id")
        deserialization_error.__cause__ = cause

        expected_exception = HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while trying to deserialize Dag: {deserialization_error}",
        )

        with pytest.raises(HTTPException, match=re.escape(expected_exception.detail)):
            DagErrorHandler().exception_handler(Mock(), deserialization_error)

    @conf_vars({("api", "expose_stacktrace"): "False"})
    def test_handle_deserialization_error_without_stacktrace(self) -> None:
        deserialization_error = DeserializationError("secret_dag_id")
        deserialization_error.__cause__ = RuntimeError("internal credential leak xyz")

        with patch("airflow.api_fastapi.common.exceptions.log") as mock_log:
            with pytest.raises(HTTPException) as exc_info:
                DagErrorHandler().exception_handler(Mock(), deserialization_error)

        detail = exc_info.value.detail
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        # Raw exception text is withheld when expose_stacktrace is off ...
        assert str(deserialization_error) not in detail
        assert "internal credential leak xyz" not in detail
        # ... and the caller gets an id to correlate with the server-side log.
        assert "look for ID" in detail
        # The error is still logged server-side, so detail is not silently lost.
        mock_log.error.assert_called_once()

    @conf_vars({("api", "expose_stacktrace"): "True"})
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
