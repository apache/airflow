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

import importlib
import sys
from unittest.mock import Mock, patch

import pytest

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.pgvector.operators.pgvector import PgVectorIngestOperator


@pytest.fixture
def pg_vector_ingest_operator():
    return PgVectorIngestOperator(
        task_id="test_task",
        sql="INSERT INTO test_table VALUES (1, 'abc', '');",
        conn_id="your_postgres_conn_id",
    )


@patch("airflow.providers.postgres.hooks.postgres.USE_PSYCOPG3", False)
@patch("airflow.providers.pgvector.operators.pgvector.PgVectorIngestOperator.get_db_hook")
def test_register_vector_psycopg2(mock_get_db_hook, monkeypatch, pg_vector_ingest_operator):
    # psycopg2 is optional (see providers/postgres's [psycopg2] extra), so patch the module
    # via sys.modules instead of `@patch("pgvector.psycopg2...")`, which would import it for real.
    mock_psycopg2 = Mock()
    monkeypatch.setitem(sys.modules, "pgvector.psycopg2", mock_psycopg2)
    mock_db_hook = Mock()
    mock_get_db_hook.return_value = mock_db_hook

    pg_vector_ingest_operator._register_vector()
    mock_psycopg2.register_vector.assert_called_with(mock_db_hook.get_conn())


@patch("airflow.providers.postgres.hooks.postgres.USE_PSYCOPG3", True)
@patch("pgvector.psycopg.register_vector")
@patch("airflow.providers.pgvector.operators.pgvector.PgVectorIngestOperator.get_db_hook")
def test_register_vector_psycopg3(mock_get_db_hook, mock_register_vector, pg_vector_ingest_operator):
    mock_db_hook = Mock()
    mock_get_db_hook.return_value = mock_db_hook

    pg_vector_ingest_operator._register_vector()
    mock_register_vector.assert_called_with(mock_db_hook.get_conn())


@patch("airflow.providers.postgres.hooks.postgres.USE_PSYCOPG3", False)
@patch("airflow.providers.pgvector.operators.pgvector.SQLExecuteQueryOperator.execute")
@patch("airflow.providers.pgvector.operators.pgvector.PgVectorIngestOperator.get_db_hook")
def test_execute(
    mock_get_db_hook, mock_execute_query_operator_execute, monkeypatch, pg_vector_ingest_operator
):
    # psycopg2 is optional (see providers/postgres's [psycopg2] extra), so patch the module
    # via sys.modules instead of `@patch("pgvector.psycopg2...")`, which would import it for real.
    monkeypatch.setitem(sys.modules, "pgvector.psycopg2", Mock())
    mock_db_hook = Mock()
    mock_get_db_hook.return_value = mock_db_hook

    pg_vector_ingest_operator.execute(None)
    mock_execute_query_operator_execute.assert_called_once()


@patch("airflow.providers.postgres.hooks.postgres.USE_PSYCOPG3", False)
def test_register_vector_raises_clear_error_without_psycopg2(monkeypatch, pg_vector_ingest_operator):
    monkeypatch.setitem(sys.modules, "pgvector.psycopg2", None)
    with pytest.raises(AirflowOptionalProviderFeatureException, match="psycopg2 is not installed"):
        pg_vector_ingest_operator._register_vector()


@patch("airflow.providers.postgres.hooks.postgres.USE_PSYCOPG3", True)
def test_register_vector_raises_clear_error_without_psycopg3(monkeypatch, pg_vector_ingest_operator):
    monkeypatch.setitem(sys.modules, "pgvector.psycopg", None)
    with pytest.raises(AirflowOptionalProviderFeatureException, match=r"psycopg \(v3\) integration"):
        pg_vector_ingest_operator._register_vector()


def test_pgvector_module_imports_without_psycopg2(monkeypatch):
    """The module must import cleanly even when psycopg2/pgvector.psycopg2 isn't installed."""
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "pgvector.psycopg2", None)
    module_name = "airflow.providers.pgvector.operators.pgvector"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    try:
        module = importlib.import_module(module_name)
        assert module.PgVectorIngestOperator is not None
    finally:
        monkeypatch.delitem(sys.modules, module_name, raising=False)
