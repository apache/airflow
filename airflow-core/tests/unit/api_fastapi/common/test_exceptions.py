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

from datetime import datetime

import pytest
from fastapi import HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from airflow.api_fastapi.common.exceptions import (
    DAGErrorHandler,
    _DatabaseDialect,
    _UniqueConstraintErrorHandler,
)
from airflow.configuration import conf
from airflow.exceptions import DeserializationError
from airflow.models import DagRun, Pool, Variable
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import clear_db_connections, clear_db_dags, clear_db_pools, clear_db_runs

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
        "table, expected_exception",
        generate_test_cases_parametrize(
            ["Pool", "Variable"],
            [
                [  # Pool
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred) VALUES (?, ?, ?, ?)",
                            "orig_error": "UNIQUE constraint failed: slot_pool.pool",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred) VALUES (%s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_pool' for key 'slot_pool.slot_pool_pool_uq'\")",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO slot_pool (pool, slots, description, include_deferred) VALUES (%(pool)s, %(slots)s, %(description)s, %(include_deferred)s) RETURNING slot_pool.id",
                            "orig_error": 'duplicate key value violates unique constraint "slot_pool_pool_uq"\nDETAIL:  Key (pool)=(test_pool) already exists.\n',
                        },
                    ),
                ],
                [  # Variable
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": 'INSERT INTO variable ("key", val, description, is_encrypted) VALUES (?, ?, ?, ?)',
                            "orig_error": "UNIQUE constraint failed: variable.key",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO variable (`key`, val, description, is_encrypted) VALUES (%s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_key' for key 'variable.variable_key_uq'\")",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO variable (key, val, description, is_encrypted) VALUES (%(key)s, %(val)s, %(description)s, %(is_encrypted)s) RETURNING variable.id",
                            "orig_error": 'duplicate key value violates unique constraint "variable_key_uq"\nDETAIL:  Key (key)=(test_key) already exists.\n',
                        },
                    ),
                ],
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

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

    @pytest.mark.parametrize(
        "table, expected_exception",
        generate_test_cases_parametrize(
            ["DagRun"],
            [
                [  # DagRun
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT max(log_template.id) AS max_1 \nFROM log_template), ?, ?, ?, ?, ?, ?, ?)",
                            "orig_error": "UNIQUE constraint failed: dag_run.dag_id, dag_run.run_id",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT max(log_template.id) AS max_1 \nFROM log_template), %s, %s, %s, %s, %s, %s, %s)",
                            "orig_error": "(1062, \"Duplicate entry 'test_dag_id-test_run_id' for key 'dag_run.dag_run_dag_id_run_id_key'\")",
                        },
                    ),
                    HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "reason": "Unique constraint violation",
                            "statement": "INSERT INTO dag_run (dag_id, queued_at, logical_date, start_date, end_date, state, run_id, creating_job_id, run_type, triggered_by, conf, data_interval_start, data_interval_end, run_after, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, bundle_version, scheduled_by_job_id, context_carrier, created_dag_version_id) VALUES (%(dag_id)s, %(queued_at)s, %(logical_date)s, %(start_date)s, %(end_date)s, %(state)s, %(run_id)s, %(creating_job_id)s, %(run_type)s, %(triggered_by)s, %(conf)s, %(data_interval_start)s, %(data_interval_end)s, %(run_after)s, %(last_scheduling_decision)s, (SELECT max(log_template.id) AS max_1 \nFROM log_template), %(updated_at)s, %(clear_number)s, %(backfill_id)s, %(bundle_version)s, %(scheduled_by_job_id)s, %(context_carrier)s, %(created_dag_version_id)s) RETURNING dag_run.id",
                            "orig_error": 'duplicate key value violates unique constraint "dag_run_dag_id_run_id_key"\nDETAIL:  Key (dag_id, run_id)=(test_dag_id, test_run_id) already exists.\n',
                        },
                    ),
                ],
            ],
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

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail


class TestDAGErrorHandler:
    dag_error_handler = DAGErrorHandler()

    def test_handle_deserialization_error_with_value_error(self):
        error_message = "Missing DAG ID in serialized DAG"
        deserialization_error = DeserializationError("test_dag_id")

        value_error = ValueError(error_message)
        deserialization_error.__cause__ = value_error

        expected_exception = HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An error occurred while trying to deserialize DAG: {deserialization_error}",
        )

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.dag_error_handler.exception_handler(None, deserialization_error)

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

    def test_handle_deserialization_error_with_runtime_error(self):
        error_message = "Error during DAG serialization process"
        deserialization_error = DeserializationError("test_dag_id")

        runtime_error = RuntimeError(error_message)
        deserialization_error.__cause__ = runtime_error

        expected_exception = HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An error occurred while trying to deserialize DAG: {deserialization_error}",
        )

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.dag_error_handler.exception_handler(None, deserialization_error)

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

    def test_handle_deserialization_error_with_key_error(self):
        key = "required_field"
        deserialization_error = DeserializationError("test_dag_id")

        key_error = KeyError(key)
        deserialization_error.__cause__ = key_error

        expected_exception = HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An error occurred while trying to deserialize DAG: {deserialization_error}",
        )

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.dag_error_handler.exception_handler(None, deserialization_error)

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

    def test_handle_real_dag_deserialization_error(self):
        """Test handling a real DAG deserialization error with actual serialized DAG."""

        # Create a test DAG
        dag_id = "test_dag_for_error"
        test_dag = DAG(dag_id=dag_id, start_date=datetime(2025, 4, 15), schedule="@once")
        EmptyOperator(task_id="test_task", dag=test_dag)
        test_dag.sync_to_db()
        SerializedDagModel.write_dag(test_dag, bundle_name=dag_id)

        with create_session() as session:
            dag_model = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))
            if not dag_model:
                pytest.fail("Failed to find serialized DAG in database")
            data = dag_model.data
            del data["dag"]["dag_id"]
            session.execute(
                update(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id).values(_data=data)
            )
            session.commit()

        dag_bag = DagBag(read_dags_from_db=True)
        with pytest.raises(DeserializationError) as exc_info:
            dag_bag.get_dag(dag_id)

        expected_exception = HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An error occurred while trying to deserialize DAG: {exc_info.value}",
        )

        with pytest.raises(HTTPException) as exeinfo_response_error:
            self.dag_error_handler.exception_handler(None, exc_info.value)

        assert exeinfo_response_error.value.status_code == expected_exception.status_code
        assert exeinfo_response_error.value.detail == expected_exception.detail

        with create_session() as session:
            session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
            session.commit()
