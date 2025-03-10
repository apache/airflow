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

from unittest.mock import Mock, patch

import pytest

from airflow.providers.pgvector.operators.pgvector import PgVectorIngestOperator


@pytest.fixture
def pg_vector_ingest_operator():
    return PgVectorIngestOperator(
        task_id="test_task",
        sql="INSERT INTO test_table VALUES (1, 'abc', '');",
        conn_id="your_postgres_conn_id",
    )


@patch("airflow.providers.pgvector.operators.pgvector.register_vector")
@patch("airflow.providers.pgvector.operators.pgvector.PgVectorIngestOperator.get_db_hook")
def test_register_vector(mock_get_db_hook, mock_register_vector, pg_vector_ingest_operator):
    # Create a mock database connection
    mock_db_hook = Mock()
    mock_get_db_hook.return_value = mock_db_hook

    pg_vector_ingest_operator._register_vector()
    mock_register_vector.assert_called_with(mock_db_hook.get_conn())


@patch("airflow.providers.pgvector.operators.pgvector.register_vector")
@patch("airflow.providers.pgvector.operators.pgvector.SQLExecuteQueryOperator.execute")
@patch("airflow.providers.pgvector.operators.pgvector.PgVectorIngestOperator.get_db_hook")
def test_execute(
    mock_get_db_hook, mock_execute_query_operator_execute, mock_register_vector, pg_vector_ingest_operator
):
    mock_db_hook = Mock()
    mock_get_db_hook.return_value = mock_db_hook

    pg_vector_ingest_operator.execute(None)
    mock_execute_query_operator_execute.assert_called_once()
