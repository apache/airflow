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

import os

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import MetaData

import airflow
from airflow.settings import engine
from airflow.utils.db import (
    compare_server_default,
    compare_type,
)

pytestmark = [pytest.mark.db_test]
try:
    from airflow.providers.fab.auth_manager.models.db import FABDBManager

    class TestFABDBManager:
        def setup_method(self, session):
            self.airflow_dir = os.path.dirname(airflow.__file__)
            self.db_manager = FABDBManager(session=session)

        def test_version_table_name_set(self):
            assert self.db_manager.version_table_name == "fab_alembic_version"

        def test_migration_dir_set(self):
            assert self.db_manager.migration_dir == f"{self.airflow_dir}/providers/fab/migrations"

        def test_alembic_file_set(self):
            assert self.db_manager.alembic_file == f"{self.airflow_dir}/providers/fab/alembic.ini"

        def test_supports_table_dropping_set(self):
            assert self.db_manager.supports_table_dropping is True

        def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
            def include_object(_, name, type_, *args):
                if type_ == "table" and name not in self.db_manager.metadata.tables:
                    return False
                return True

            all_meta_data = MetaData()
            for table_name, table in self.db_manager.metadata.tables.items():
                all_meta_data._add_table(table_name, table.schema, table)
            # create diff between database schema and SQLAlchemy model
            mctx = MigrationContext.configure(
                engine.connect(),
                opts={
                    "compare_type": compare_type,
                    "compare_server_default": compare_server_default,
                    "include_object": include_object,
                },
            )
            diff = compare_metadata(mctx, all_meta_data)

            assert not diff, "Database schema and SQLAlchemy model are not in sync: " + str(diff)
except ModuleNotFoundError:
    pass
