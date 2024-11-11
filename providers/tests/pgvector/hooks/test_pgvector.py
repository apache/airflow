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

from unittest.mock import Mock

import pytest

from airflow.providers.pgvector.hooks.pgvector import (
    PgVectorHook,
)


@pytest.fixture
def pg_vector_hook():
    return PgVectorHook(postgres_conn_id="your_postgres_conn_id")


def test_create_table(pg_vector_hook):
    pg_vector_hook.run = Mock()
    table_name = "my_table"
    columns = ["id SERIAL PRIMARY KEY", "name VARCHAR(255)", "value INTEGER"]
    pg_vector_hook.create_table(table_name, columns, if_not_exists=True)
    pg_vector_hook.run.assert_called_with(
        "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY, name VARCHAR(255), value INTEGER)"
    )


def test_create_extension(pg_vector_hook):
    pg_vector_hook.run = Mock()
    extension_name = "timescaledb"
    pg_vector_hook.create_extension(extension_name, if_not_exists=True)
    pg_vector_hook.run.assert_called_with("CREATE EXTENSION IF NOT EXISTS timescaledb")


def test_drop_table(pg_vector_hook):
    pg_vector_hook.run = Mock()
    table_name = "my_table"
    pg_vector_hook.drop_table(table_name, if_exists=True)
    pg_vector_hook.run.assert_called_with("DROP TABLE IF EXISTS my_table")


def test_truncate_table(pg_vector_hook):
    pg_vector_hook.run = Mock()
    table_name = "my_table"
    pg_vector_hook.truncate_table(table_name, restart_identity=True)
    pg_vector_hook.run.assert_called_with("TRUNCATE TABLE my_table RESTART IDENTITY")
