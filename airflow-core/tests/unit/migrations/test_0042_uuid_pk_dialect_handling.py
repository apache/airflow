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

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

MIGRATION_PATH = (
    Path(__file__).parents[3]
    / "src"
    / "airflow"
    / "migrations"
    / "versions"
    / "0042_3_0_0_add_uuid_primary_key_to_task_instance_.py"
)


@pytest.fixture(scope="module")
def migration_module():
    spec = importlib.util.spec_from_file_location("migration_0042", MIGRATION_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("dialect_name", ["postgresql", "cockroachdb"])
def test_id_column_is_native_uuid_for_postgres_family(migration_module, dialect_name):
    column_type = migration_module._get_type_id_column(dialect_name)
    assert isinstance(column_type, postgresql.UUID)
    assert not column_type.as_uuid


@pytest.mark.parametrize("dialect_name", ["mysql", "sqlite"])
def test_id_column_is_string_for_other_dialects(migration_module, dialect_name):
    column_type = migration_module._get_type_id_column(dialect_name)
    assert isinstance(column_type, sa.String)
