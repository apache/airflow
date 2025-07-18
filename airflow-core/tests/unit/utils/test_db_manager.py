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

from unittest import mock

import pytest

from airflow.models import Base
from airflow.utils.db_manager import BaseDBManager

pytestmark = [pytest.mark.db_test]


class MockDBManager(BaseDBManager):
    metadata = Base.metadata
    version_table_name = "mock_alembic_version"
    migration_dir = "mock_migration_dir"
    alembic_file = "mock_alembic.ini"
    supports_table_dropping = True


class TestBaseDBManager:
    @mock.patch.object(BaseDBManager, "get_alembic_config")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    @mock.patch.object(BaseDBManager, "create_db_from_orm")
    def test_create_db_from_orm_called_from_init(
        self, mock_create_db_from_orm, mock_current_revision, mock_config, session
    ):
        mock_current_revision.return_value = None

        manager = MockDBManager(session)
        manager.initdb()
        mock_create_db_from_orm.assert_called_once()

    @mock.patch.object(BaseDBManager, "get_alembic_config")
    @mock.patch("alembic.command.upgrade")
    def test_upgrade(self, mock_alembic_cmd, mock_alembic_config, session, caplog):
        manager = MockDBManager(session)
        manager.upgradedb()
        mock_alembic_cmd.assert_called_once()
        assert "Upgrading the MockDBManager database" in caplog.text

    @mock.patch.object(BaseDBManager, "get_script_object")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    def test_check_migration(self, mock_script_obj, mock_current_revision, session):
        manager = MockDBManager(session)
        manager.check_migration()  # just ensure this can be called
