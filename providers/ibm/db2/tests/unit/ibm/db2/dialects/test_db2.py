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

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.ibm.db2.dialects.db2 import Db2Dialect


class TestDb2Dialect:
    """Test Db2Dialect functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create a mock that passes isinstance check for DbApiHook
        self.test_db_hook = MagicMock(spec=DbApiHook)
        # Configure the mock to pass isinstance check
        self.test_db_hook.__class__ = DbApiHook
        self.test_db_hook.placeholder = "?"
        self.test_db_hook.insert_statement_format = "INSERT INTO {} {} VALUES ({})"
        self.test_db_hook.escape_word_format = '"{}"'
        self.test_db_hook.escape_column_names = False

        # Mock get_records for primary key detection (default returns ["ID"])
        # This will be called by get_primary_keys() in the Dialect
        self.test_db_hook.get_records.return_value = [("ID",)]

    def test_placeholder(self):
        """Test placeholder property."""
        assert Db2Dialect(self.test_db_hook).placeholder == "?"

    def test_generate_replace_sql_with_single_primary_key(self):
        """Test MERGE SQL generation with single primary key."""
        # Default inspector already returns ["ID"]

        sql = Db2Dialect(self.test_db_hook).generate_replace_sql(
            table="TEST_TABLE",
            values=(1, "Alice", 100),
            target_fields=["ID", "NAME", "VALUE"],
        )

        # Verify MERGE statement structure
        assert "MERGE INTO TEST_TABLE AS t" in sql
        assert "USING (VALUES (?,?,?)) AS s(ID, NAME, VALUE)" in sql
        assert "ON t.ID = s.ID" in sql
        assert "WHEN MATCHED THEN UPDATE SET t.NAME = s.NAME, t.VALUE = s.VALUE" in sql
        assert "WHEN NOT MATCHED THEN INSERT (ID, NAME, VALUE) VALUES (s.ID, s.NAME, s.VALUE)" in sql

    def test_generate_replace_sql_with_composite_key(self):
        """Test MERGE SQL generation with composite primary key."""
        # Override get_records to return composite primary key
        self.test_db_hook.get_records.return_value = [("DEPT_ID",), ("EMP_ID",)]

        sql = Db2Dialect(self.test_db_hook).generate_replace_sql(
            table="TEST_TABLE",
            values=(1, 101, "Alice", 50000),
            target_fields=["DEPT_ID", "EMP_ID", "NAME", "SALARY"],
        )

        # Verify composite key in ON clause
        assert "ON t.DEPT_ID = s.DEPT_ID AND t.EMP_ID = s.EMP_ID" in sql

        # Extract the UPDATE SET clause to verify primary keys are excluded
        update_clause = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]

        # Verify UPDATE excludes primary key columns
        assert "t.NAME = s.NAME" in update_clause
        assert "t.SALARY = s.SALARY" in update_clause
        assert "t.DEPT_ID = s.DEPT_ID" not in update_clause  # PK should not be in UPDATE SET
        assert "t.EMP_ID = s.EMP_ID" not in update_clause  # PK should not be in UPDATE SET

    def test_generate_replace_sql_with_replace_index(self):
        """Test MERGE SQL generation with explicit replace_index."""
        sql = Db2Dialect(self.test_db_hook).generate_replace_sql(
            table="TEST_TABLE",
            values=(1, "Alice", 100),
            target_fields=["ID", "NAME", "VALUE"],
            replace_index=["NAME"],  # Use NAME as matching column instead of PK
        )

        # Verify replace_index is used in ON clause
        assert "ON t.NAME = s.NAME" in sql
        # Verify UPDATE includes ID and VALUE but not NAME
        assert "t.ID = s.ID" in sql
        assert "t.VALUE = s.VALUE" in sql

    def test_generate_replace_sql_no_pk_no_replace_index(self):
        """Test that MERGE SQL generation fails without primary key or replace_index."""
        # Override get_records to return no primary key
        self.test_db_hook.get_records.return_value = []

        with pytest.raises(ValueError, match="no primary key found and no replace_index provided"):
            Db2Dialect(self.test_db_hook).generate_replace_sql(
                table="TEST_TABLE",
                values=(1, "Alice", 100),
                target_fields=["ID", "NAME", "VALUE"],
            )

    def test_generate_replace_sql_no_target_fields(self):
        """Test that MERGE SQL generation fails without target_fields."""
        with pytest.raises(ValueError, match="requires column names"):
            Db2Dialect(self.test_db_hook).generate_replace_sql(
                table="TEST_TABLE",
                values=(1, "Alice", 100),
                target_fields=[],
            )

    def test_generate_replace_sql_all_columns_are_pk(self):
        """Test MERGE SQL when all columns are part of primary key (no UPDATE needed)."""
        # Override get_records to return both columns as PK
        self.test_db_hook.get_records.return_value = [("ID",), ("NAME",)]

        sql = Db2Dialect(self.test_db_hook).generate_replace_sql(
            table="TEST_TABLE",
            values=(1, "Alice"),
            target_fields=["ID", "NAME"],
        )

        # When all columns are PK, there should be no UPDATE clause
        assert "WHEN MATCHED THEN UPDATE" not in sql
        # But INSERT should still be present
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    def test_generate_replace_sql_with_escaped_column_names(self):
        """Test MERGE SQL generation with escaped column names."""
        self.test_db_hook.escape_column_names = True
        # Default inspector already returns ["ID"]

        sql = Db2Dialect(self.test_db_hook).generate_replace_sql(
            table="TEST_TABLE",
            values=(1, "Alice", 100),
            target_fields=["ID", "NAME", "VALUE"],
        )

        # Verify column names are escaped
        assert '"ID"' in sql
        assert '"NAME"' in sql
        assert '"VALUE"' in sql

    def test_get_column_names_excludes_identity_columns(self):
        """Test that get_column_names() excludes identity columns by default."""
        # Create a fresh hook for this test to avoid cache issues
        test_hook = MagicMock(spec=DbApiHook)
        test_hook.__class__ = DbApiHook
        test_hook.placeholder = "?"
        test_hook.escape_word_format = '"{}"'
        test_hook.escape_column_names = False

        # Mock get_records to return DB2 system catalog rows
        test_hook.get_records.return_value = [
            ("ID", "INTEGER", "N", None, "Y"),  # Identity column - should be excluded
            ("NAME", "VARCHAR", "N", None, "N"),
            ("VALUE", "INTEGER", "Y", None, "N"),
        ]

        dialect = Db2Dialect(test_hook)
        columns = dialect.get_column_names("TEST_TABLE", schema="TESTSCHEMA")

        # Verify identity column is excluded
        assert columns == ["NAME", "VALUE"]
        assert "ID" not in columns

    def test_get_column_names_includes_all_when_no_identity(self):
        """Test that get_column_names() includes all columns when none are identity columns."""
        # Create a fresh hook for this test
        test_hook = MagicMock(spec=DbApiHook)
        test_hook.__class__ = DbApiHook
        test_hook.placeholder = "?"
        test_hook.escape_word_format = '"{}"'
        test_hook.escape_column_names = False

        # Mock get_records to return columns without identity
        test_hook.get_records.return_value = [
            ("ID", "INTEGER", "N", None, "N"),
            ("NAME", "VARCHAR", "N", None, "N"),
            ("VALUE", "INTEGER", "Y", None, "N"),
        ]

        dialect = Db2Dialect(test_hook)
        columns = dialect.get_column_names("TEST_TABLE", schema="TESTSCHEMA")

        # Verify all columns are included
        assert columns == ["ID", "NAME", "VALUE"]

    def test_get_column_names_with_custom_predicate(self):
        """Test get_column_names() with custom predicate."""
        # Create a fresh hook for this test
        test_hook = MagicMock(spec=DbApiHook)
        test_hook.__class__ = DbApiHook
        test_hook.placeholder = "?"
        test_hook.escape_word_format = '"{}"'
        test_hook.escape_column_names = False

        # Mock get_records to return columns
        test_hook.get_records.return_value = [
            ("ID", "INTEGER", "N", None, "N"),
            ("NAME", "VARCHAR", "N", None, "N"),
            ("VALUE", "INTEGER", "Y", None, "N"),  # Nullable
        ]

        dialect = Db2Dialect(test_hook)
        # Custom predicate: only non-nullable columns
        columns = dialect.get_column_names(
            "TEST_TABLE", schema="TESTSCHEMA", predicate=lambda col: not col["nullable"]
        )

        # Verify only non-nullable columns are included
        assert columns == ["ID", "NAME"]
        assert "VALUE" not in columns

    def test_get_primary_keys_single(self):
        """Test get_primary_keys() with single primary key."""
        # Create a fresh hook for this test
        test_hook = MagicMock(spec=DbApiHook)
        test_hook.__class__ = DbApiHook
        test_hook.placeholder = "?"
        test_hook.escape_word_format = '"{}"'
        test_hook.escape_column_names = False

        # Mock get_records to return single primary key column
        test_hook.get_records.return_value = [("ID",)]

        dialect = Db2Dialect(test_hook)
        pk_columns = dialect.get_primary_keys("TEST_TABLE", schema="TESTSCHEMA")

        assert pk_columns == ["ID"]

    def test_get_primary_keys_composite(self):
        """Test get_primary_keys() with composite primary key."""
        # Create a fresh hook for this test
        test_hook = MagicMock(spec=DbApiHook)
        test_hook.__class__ = DbApiHook
        test_hook.placeholder = "?"
        test_hook.escape_word_format = '"{}"'
        test_hook.escape_column_names = False

        # Mock get_records to return composite primary key
        test_hook.get_records.return_value = [("DEPT_ID",), ("EMP_ID",)]

        dialect = Db2Dialect(test_hook)
        pk_columns = dialect.get_primary_keys("TEST_TABLE", schema="TESTSCHEMA")

        assert pk_columns == ["DEPT_ID", "EMP_ID"]
        assert len(pk_columns) == 2

    def test_get_primary_keys_no_pk(self):
        """Test get_primary_keys() when table has no primary key."""
        # Create a fresh hook for this test
        test_hook = MagicMock(spec=DbApiHook)
        test_hook.__class__ = DbApiHook
        test_hook.placeholder = "?"
        test_hook.escape_word_format = '"{}"'
        test_hook.escape_column_names = False

        # Mock get_records to return empty list
        test_hook.get_records.return_value = []

        dialect = Db2Dialect(test_hook)
        pk_columns = dialect.get_primary_keys("TEST_TABLE", schema="TESTSCHEMA")

        assert pk_columns is None

    def test_dialect_name_property(self):
        """Test that dialect has correct name property."""
        dialect = Db2Dialect(self.test_db_hook)
        assert dialect.name == "db2"
