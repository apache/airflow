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

import pytest
from sqlglot import exp

from airflow.providers.common.ai.utils.sql_validation import SQLSafetyError, validate_sql


class TestValidateSQLAllowed:
    """Statements that should pass validation with default settings."""

    def test_simple_select(self):
        result = validate_sql("SELECT 1")
        assert len(result) == 1
        assert isinstance(result[0], exp.Select)

    def test_select_from_table(self):
        result = validate_sql("SELECT id, name FROM users WHERE active = true")
        assert len(result) == 1
        assert isinstance(result[0], exp.Select)

    def test_select_with_join(self):
        result = validate_sql("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
        assert len(result) == 1

    def test_select_with_cte(self):
        result = validate_sql("WITH top_users AS (SELECT id FROM users LIMIT 10) SELECT * FROM top_users")
        assert len(result) == 1
        assert isinstance(result[0], exp.Select)

    def test_select_with_subquery(self):
        result = validate_sql("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
        assert len(result) == 1

    def test_union(self):
        result = validate_sql("SELECT 1 UNION SELECT 2")
        assert len(result) == 1
        assert isinstance(result[0], exp.Union)

    def test_union_all(self):
        result = validate_sql("SELECT 1 UNION ALL SELECT 2")
        assert len(result) == 1
        assert isinstance(result[0], exp.Union)

    def test_intersect(self):
        result = validate_sql("SELECT 1 INTERSECT SELECT 1")
        assert len(result) == 1
        assert isinstance(result[0], exp.Intersect)

    def test_except(self):
        result = validate_sql("SELECT 1 EXCEPT SELECT 2")
        assert len(result) == 1
        assert isinstance(result[0], exp.Except)


class TestValidateSQLBlocked:
    """Statements that should be blocked with default settings."""

    def test_insert_blocked(self):
        with pytest.raises(SQLSafetyError, match="Insert.*not allowed"):
            validate_sql("INSERT INTO users (name) VALUES ('test')")

    def test_update_blocked(self):
        with pytest.raises(SQLSafetyError, match="Update.*not allowed"):
            validate_sql("UPDATE users SET name = 'test' WHERE id = 1")

    def test_delete_blocked(self):
        with pytest.raises(SQLSafetyError, match="Delete.*not allowed"):
            validate_sql("DELETE FROM users WHERE id = 1")

    def test_drop_blocked(self):
        with pytest.raises(SQLSafetyError, match="Drop.*not allowed"):
            validate_sql("DROP TABLE users")

    def test_create_blocked(self):
        with pytest.raises(SQLSafetyError, match="Create.*not allowed"):
            validate_sql("CREATE TABLE test (id INT)")

    def test_alter_blocked(self):
        with pytest.raises(SQLSafetyError, match="Alter.*not allowed"):
            validate_sql("ALTER TABLE users ADD COLUMN email TEXT")

    def test_truncate_blocked(self):
        with pytest.raises(SQLSafetyError, match="not allowed"):
            validate_sql("TRUNCATE TABLE users")


class TestValidateSQLMultiStatement:
    """Multi-statement SQL should be blocked by default."""

    def test_multiple_statements_blocked_by_default(self):
        with pytest.raises(SQLSafetyError, match="Multiple statements detected"):
            validate_sql("SELECT 1; SELECT 2")

    def test_multiple_statements_allowed_when_opted_in(self):
        result = validate_sql("SELECT 1; SELECT 2", allow_multiple_statements=True)
        assert len(result) == 2

    def test_dangerous_hidden_after_select(self):
        """Multi-statement blocks even if first statement is safe."""
        with pytest.raises(SQLSafetyError, match="Multiple statements"):
            validate_sql("SELECT 1; DROP TABLE users")

    def test_multi_statement_still_validates_types(self):
        """Even when multi-statement is allowed, types are still checked."""
        with pytest.raises(SQLSafetyError, match="Drop.*not allowed"):
            validate_sql("SELECT 1; DROP TABLE users", allow_multiple_statements=True)


class TestValidateSQLEdgeCases:
    """Edge cases and error handling."""

    def test_empty_string_raises(self):
        with pytest.raises(SQLSafetyError, match="Empty SQL"):
            validate_sql("")

    def test_whitespace_only_raises(self):
        with pytest.raises(SQLSafetyError, match="Empty SQL"):
            validate_sql("   \n\t  ")

    def test_malformed_sql_raises(self):
        with pytest.raises(SQLSafetyError, match="SQL parse error"):
            validate_sql("NOT VALID SQL AT ALL }{][")

    def test_dialect_parameter(self):
        result = validate_sql("SELECT 1", dialect="postgres")
        assert len(result) == 1

    def test_custom_allowed_types(self):
        """Allow INSERT when explicitly opted in."""
        result = validate_sql(
            "INSERT INTO users (name) VALUES ('test')",
            allowed_types=(exp.Insert,),
        )
        assert len(result) == 1

    def test_custom_allowed_types_still_blocks_others(self):
        """Custom types don't allow everything."""
        with pytest.raises(SQLSafetyError, match="Select.*not allowed"):
            validate_sql("SELECT 1", allowed_types=(exp.Insert,))

    def test_select_with_trailing_semicolon(self):
        """Trailing semicolon should not cause multi-statement error."""
        result = validate_sql("SELECT 1;")
        assert len(result) == 1
