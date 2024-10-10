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

from sqlalchemy import Column, MetaData, String, Table

from dev.tests_common.test_utils.compat import ignore_provider_compatibility_error

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.providers.fab.auth_manager.models import (
        add_index_on_ab_register_user_username_postgres,
        add_index_on_ab_user_username_postgres,
    )

_mock_conn = mock.MagicMock()
_mock_conn.dialect = mock.MagicMock()
_mock_conn.dialect.name = "postgresql"


def test_add_index_on_ab_user_username_postgres():
    table = Table("test_table", MetaData(), Column("username", String))

    assert len(table.indexes) == 0

    add_index_on_ab_user_username_postgres(table, _mock_conn)

    # Assert that the index was added to the table
    assert len(table.indexes) == 1

    add_index_on_ab_user_username_postgres(table, _mock_conn)

    # Assert that index is not re-added when the schema is recreated
    assert len(table.indexes) == 1


def test_add_index_on_ab_register_user_username_postgres():
    table = Table("test_table", MetaData(), Column("username", String))

    assert len(table.indexes) == 0

    add_index_on_ab_register_user_username_postgres(table, _mock_conn)

    # Assert that the index was added to the table
    assert len(table.indexes) == 1

    add_index_on_ab_register_user_username_postgres(table, _mock_conn)

    # Assert that index is not re-added when the schema is recreated
    assert len(table.indexes) == 1
