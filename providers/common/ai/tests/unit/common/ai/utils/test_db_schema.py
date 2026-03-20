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

from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.utils.db_schema import build_schema_context


class TestBuildSchemaContext:
    def test_raises_when_table_names_given_without_db_hook(self):
        with pytest.raises(ValueError, match="table_names requires db_conn_id"):
            build_schema_context(
                db_hook=None,
                table_names=["customers"],
                schema_context=None,
                datasource_config=MagicMock(),
            )

    def test_uses_bulk_schema_fetch_when_available(self):
        mock_db_hook = MagicMock()
        mock_db_hook.get_table_schemas.return_value = {
            "customers": [{"name": "id", "type": "INT"}],
            "orders": [{"name": "order_id", "type": "INT"}],
        }

        result = build_schema_context(
            db_hook=mock_db_hook,
            table_names=["customers", "orders"],
            schema_context=None,
            datasource_config=None,
        )

        mock_db_hook.get_table_schemas.assert_called_once_with(["customers", "orders"])
        mock_db_hook.get_table_schema.assert_not_called()
        assert "Table: customers" in result
        assert "Table: orders" in result

    def test_falls_back_to_single_table_schema_when_bulk_result_missing_table(self):
        mock_db_hook = MagicMock()
        mock_db_hook.get_table_schemas.return_value = {
            "customers": [{"name": "id", "type": "INT"}],
        }
        mock_db_hook.get_table_schema.return_value = [{"name": "order_id", "type": "INT"}]

        result = build_schema_context(
            db_hook=mock_db_hook,
            table_names=["customers", "orders"],
            schema_context=None,
            datasource_config=None,
        )

        mock_db_hook.get_table_schemas.assert_called_once_with(["customers", "orders"])
        mock_db_hook.get_table_schema.assert_called_once_with("orders")
        assert "Table: customers" in result
        assert "Table: orders" in result
