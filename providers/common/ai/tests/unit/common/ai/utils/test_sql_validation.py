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

from airflow.providers.common.ai.utils.sql_validation import (
    SQLSafetyError,
    collect_table_references,
    parse_sql,
    resolve_sqlglot_dialect,
    validate_sql,
)


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


class TestDataModifyingNodeDetection:
    """Data-modifying operations hidden inside allowed statement types should be blocked."""

    def test_cte_with_delete_blocked(self):
        """DELETE inside a CTE bypasses top-level type check."""
        with pytest.raises(SQLSafetyError, match="Data-modifying operation 'Delete'"):
            validate_sql(
                "WITH del AS (DELETE FROM users RETURNING *) SELECT * FROM del",
                dialect="postgres",
            )

    def test_cte_with_insert_blocked(self):
        """INSERT inside a CTE bypasses top-level type check."""
        with pytest.raises(SQLSafetyError, match="Data-modifying operation 'Insert'"):
            validate_sql(
                "WITH ins AS (INSERT INTO users(name) VALUES ('x') RETURNING *) SELECT * FROM ins",
                dialect="postgres",
            )

    def test_cte_with_update_blocked(self):
        """UPDATE inside a CTE bypasses top-level type check."""
        with pytest.raises(SQLSafetyError, match="Data-modifying operation 'Update'"):
            validate_sql(
                "WITH upd AS (UPDATE users SET name = 'x' RETURNING *) SELECT * FROM upd",
                dialect="postgres",
            )

    def test_select_into_blocked(self):
        """SELECT INTO creates a new table — should be blocked."""
        with pytest.raises(SQLSafetyError, match="Data-modifying operation 'Into'"):
            validate_sql("SELECT * INTO new_table FROM users")

    def test_plain_cte_select_still_allowed(self):
        """Normal read-only CTEs should not be affected."""
        result = validate_sql("WITH t AS (SELECT id FROM users) SELECT * FROM t")
        assert len(result) == 1

    def test_nested_subquery_select_still_allowed(self):
        """Subqueries that are pure reads should not be affected."""
        result = validate_sql("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
        assert len(result) == 1

    def test_deep_scan_runs_with_explicit_default_types(self):
        """Deep scan should also block when DEFAULT_ALLOWED_TYPES is passed explicitly."""
        from airflow.providers.common.ai.utils.sql_validation import DEFAULT_ALLOWED_TYPES

        with pytest.raises(SQLSafetyError, match="Data-modifying operation 'Delete'"):
            validate_sql(
                "WITH del AS (DELETE FROM users RETURNING *) SELECT * FROM del",
                allowed_types=DEFAULT_ALLOWED_TYPES,
                dialect="postgres",
            )


class TestReadOnlyMetadata:
    """Read-only metadata statements (DESCRIBE/SHOW) with ``allow_read_only_metadata``."""

    @pytest.mark.parametrize(
        ("sql", "kwargs"),
        [
            ("DESCRIBE TABLE users", {}),
            ("SHOW TABLES", {"dialect": "snowflake"}),
        ],
        ids=["describe", "show"],
    )
    def test_metadata_blocked_without_flag(self, sql, kwargs):
        with pytest.raises(SQLSafetyError, match="not allowed"):
            validate_sql(sql, **kwargs)

    @pytest.mark.parametrize(
        ("sql", "dialect", "expected_type"),
        [
            # DESCRIBE/DESC parse to exp.Describe in every dialect (dialect-agnostic).
            ("DESCRIBE TABLE users", None, exp.Describe),
            ("DESC users", None, exp.Describe),
            # SHOW only parses to exp.Show when a supporting dialect is passed.
            ("SHOW TABLES", "snowflake", exp.Show),
            ("SHOW COLUMNS IN users", "snowflake", exp.Show),
        ],
    )
    def test_metadata_allowed_with_flag(self, sql, dialect, expected_type):
        result = validate_sql(sql, dialect=dialect, allow_read_only_metadata=True)
        assert len(result) == 1
        assert isinstance(result[0], expected_type)

    def test_show_blocked_without_supporting_dialect(self):
        """Without a dialect that supports SHOW, sqlglot falls back to exp.Command, still blocked."""
        with pytest.raises(SQLSafetyError, match="Command.*not allowed"):
            validate_sql("SHOW TABLES", allow_read_only_metadata=True)

    def test_explain_wrapped_write_still_blocked(self):
        """EXPLAIN <write> parses to exp.Describe but the deep scan rejects the inner write."""
        with pytest.raises(SQLSafetyError, match="Data-modifying operation 'Delete'"):
            validate_sql("EXPLAIN DELETE FROM users", dialect="mysql", allow_read_only_metadata=True)

    @pytest.mark.parametrize(
        ("sql", "node"),
        [
            ("DESCRIBE CREATE TABLE t (a int)", "Create"),
            ("DESCRIBE DROP TABLE users", "Drop"),
            ("DESCRIBE TRUNCATE TABLE users", "TruncateTable"),
            ("DESCRIBE DELETE FROM users", "Delete"),
        ],
    )
    def test_describe_wrapped_ddl_or_dml_blocked(self, sql, node):
        """DESCRIBE <DDL/DML> parses to exp.Describe; the deep scan rejects the inner write."""
        with pytest.raises(SQLSafetyError, match=f"Data-modifying operation '{node}'"):
            validate_sql(sql, allow_read_only_metadata=True)

    def test_metadata_flag_ignored_when_custom_types_supplied(self):
        """When the caller supplies allowed_types it controls the allow-list; the flag is ignored."""
        with pytest.raises(SQLSafetyError, match="Describe.*not allowed"):
            validate_sql(
                "DESCRIBE TABLE users",
                allowed_types=(exp.Select,),
                allow_read_only_metadata=True,
            )

    def test_select_still_allowed_with_flag(self):
        result = validate_sql("SELECT 1", allow_read_only_metadata=True)
        assert isinstance(result[0], exp.Select)

    def test_writes_still_blocked_with_flag(self):
        with pytest.raises(SQLSafetyError, match="Delete.*not allowed"):
            validate_sql("DELETE FROM users WHERE id = 1", allow_read_only_metadata=True)


class TestResolveSqlglotDialect:
    """``resolve_sqlglot_dialect`` normalizes/validates SQLAlchemy dialect names."""

    @pytest.mark.parametrize(
        ("dialect_name", "expected"),
        [
            ("postgresql", "postgres"),
            ("mssql", "tsql"),
            ("mysql", "mysql"),
            ("snowflake", "snowflake"),
            ("sqlite", "sqlite"),
            (None, None),
            ("", None),
            ("default", None),
            ("not_a_real_dialect", None),
            (123, None),
        ],
    )
    def test_resolution(self, dialect_name, expected):
        assert resolve_sqlglot_dialect(dialect_name) == expected


class TestParseSQL:
    """``parse_sql`` enforces only the empty- and multi-statement guards."""

    def test_returns_statements(self):
        parsed = parse_sql("SELECT 1")
        assert len(parsed) == 1
        assert isinstance(parsed[0], exp.Select)

    def test_does_not_apply_type_checks(self):
        """Unlike validate_sql, parse_sql accepts writes -- callers add their own policy."""
        parsed = parse_sql("DELETE FROM users WHERE id = 1")
        assert isinstance(parsed[0], exp.Delete)

    @pytest.mark.parametrize("sql", ["", "   ", "\n\t"])
    def test_rejects_empty(self, sql):
        with pytest.raises(SQLSafetyError, match="Empty SQL"):
            parse_sql(sql)

    def test_rejects_multiple_statements_by_default(self):
        with pytest.raises(SQLSafetyError, match="Multiple statements"):
            parse_sql("SELECT 1; SELECT 2")

    def test_allows_multiple_statements_when_opted_in(self):
        assert len(parse_sql("SELECT 1; SELECT 2", allow_multiple_statements=True)) == 2

    def test_rejects_unparsable(self):
        with pytest.raises(SQLSafetyError, match="parse error"):
            parse_sql("SELECT FROM WHERE )(")


class TestCollectTableReferences:
    """``collect_table_references`` reports the real tables a query reaches."""

    @pytest.mark.parametrize(
        ("sql", "dialect", "expected"),
        [
            ("SELECT * FROM secret", None, [("", "", "secret")]),
            ("SELECT * FROM model_crm.orders", None, [("", "model_crm", "orders")]),
            ("SELECT * FROM a JOIN b ON a.id = b.id", None, [("", "", "a"), ("", "", "b")]),
            ("SELECT * FROM (SELECT * FROM inner_t) x", None, [("", "", "inner_t")]),
            ("SELECT * FROM a UNION SELECT * FROM b", None, [("", "", "a"), ("", "", "b")]),
            (
                "SELECT table_name FROM information_schema.tables",
                "postgres",
                [("", "information_schema", "tables")],
            ),
            ("DESCRIBE secret", "mysql", [("", "", "secret")]),
            # Cross-database reference carries its catalog so the caller can reject it.
            ("SELECT * FROM otherdb.public.orders", "snowflake", [("otherdb", "public", "orders")]),
        ],
        ids=["table", "qualified", "join", "subquery", "union", "catalog", "describe", "cross_db"],
    )
    def test_collects_real_tables(self, sql, dialect, expected):
        scan = collect_table_references(parse_sql(sql, dialect=dialect))
        assert sorted(scan.tables) == sorted(expected)
        assert scan.unverifiable_sources == []

    def test_excludes_cte_reference_but_keeps_its_body(self):
        scan = collect_table_references(parse_sql("WITH s AS (SELECT * FROM base) SELECT * FROM s"))
        assert scan.tables == [("", "", "base")]  # 's' is the CTE, not a table

    def test_cte_that_shadows_a_table_name_yields_no_table(self):
        scan = collect_table_references(parse_sql("WITH secret AS (SELECT 1 AS x) SELECT * FROM secret"))
        assert scan.tables == []

    def test_schema_qualified_name_is_never_treated_as_a_cte(self):
        scan = collect_table_references(parse_sql("WITH s AS (SELECT 1 AS x) SELECT * FROM myschema.s"))
        assert scan.tables == [("", "myschema", "s")]

    def test_inner_cte_does_not_shadow_outer_real_table(self):
        """A same-named CTE in an inner subquery must not hide the real top-level table."""
        sql = "SELECT * FROM secret WHERE id IN (WITH secret AS (SELECT 1 id) SELECT id FROM secret)"
        scan = collect_table_references(parse_sql(sql))
        assert ("", "", "secret") in scan.tables  # the top-level real table is reported

    def test_cte_self_body_references_real_table(self):
        """A non-recursive CTE is not in scope within its own body, so the real table shows."""
        scan = collect_table_references(
            parse_sql("WITH secret AS (SELECT * FROM secret) SELECT * FROM secret")
        )
        assert ("", "", "secret") in scan.tables

    def test_cte_forward_reference_is_real_table(self):
        """A CTE may only reference earlier siblings; a later-defined name is the real table."""
        sql = "WITH a AS (SELECT * FROM secret), secret AS (SELECT 1 id) SELECT * FROM a"
        scan = collect_table_references(parse_sql(sql))
        assert ("", "", "secret") in scan.tables

    def test_legit_cte_reference_is_excluded(self):
        """A genuine CTE reference is not reported as a base table (no false reject)."""
        scan = collect_table_references(
            parse_sql("WITH ranked AS (SELECT * FROM orders) SELECT * FROM ranked")
        )
        assert scan.tables == [("", "", "orders")]

    @pytest.mark.parametrize(
        ("sql", "dialect"),
        [
            ("SELECT * FROM orders/*!UNION SELECT * FROM secret*/", "mysql"),
            ("SELECT * FROM orders /*!50000 UNION SELECT * FROM secret */", "mysql"),
            ("SELECT * FROM orders WHERE 0--+1 OR id IN (SELECT id FROM secret)", "mysql"),
        ],
        ids=["exec_comment", "versioned_comment", "dashdash"],
    )
    def test_flags_inline_comment_as_unverifiable(self, sql, dialect):
        """Comments hide parser-vs-engine differentials (MySQL executable comments), so reject them."""
        scan = collect_table_references(parse_sql(sql, dialect=dialect))
        assert scan.unverifiable_sources

    @pytest.mark.parametrize(
        "sql",
        ["SELECT * FROM TABLE('secret')", "SELECT * FROM TABLE($$secret$$)"],
        ids=["string", "dollar"],
    )
    def test_flags_table_row_source_as_unverifiable(self, sql):
        """Snowflake TABLE('name') names a table through a string the parser can't resolve."""
        scan = collect_table_references(parse_sql(sql, dialect="snowflake"))
        assert scan.unverifiable_sources

    def test_flags_table_shorthand_as_unverifiable(self):
        """The TABLE <name> shorthand is mis-parsed by sqlglot, so reject it."""
        scan = collect_table_references(
            parse_sql("TABLE secret UNION SELECT * FROM orders", dialect="postgres")
        )
        assert scan.unverifiable_sources

    @pytest.mark.parametrize(
        "sql",
        ['SELECT * FROM public."Orders"', 'SELECT * FROM "Orders"', 'SELECT * FROM "PUBLIC".orders'],
        ids=["quoted_table", "quoted_bare", "quoted_schema"],
    )
    def test_flags_quoted_identifier_as_unverifiable(self, sql):
        """A quoted identifier is case-sensitive; case-insensitive matching can't verify it."""
        scan = collect_table_references(parse_sql(sql, dialect="postgres"))
        assert scan.unverifiable_sources

    def test_dml_target_is_real_but_cte_source_is_excluded(self):
        """A CTE used as a DML source is not a base table; the target and CTE body are."""
        sql = "WITH src AS (SELECT * FROM orders) INSERT INTO orders SELECT * FROM src"
        scan = collect_table_references(parse_sql(sql, dialect="postgres"))
        # Only the real table `orders` is reported (target + CTE body); `src` is the CTE.
        assert {t for _, _, t in scan.tables} == {"orders"}
        assert scan.unverifiable_sources == []

    def test_dml_target_shadowed_by_cte_is_still_reported(self):
        """A DML target is a real table even when a same-named CTE exists (can't write a CTE)."""
        sql = "WITH secret AS (SELECT 1) DELETE FROM secret"
        scan = collect_table_references(parse_sql(sql, dialect="postgres"))
        assert ("", "", "secret") in scan.tables

    @pytest.mark.parametrize(
        ("sql", "dialect"),
        [
            ("SELECT * FROM dblink('h', 'SELECT 1') AS t(x int)", "postgres"),
            ("SELECT * FROM generate_series(1, 10)", "postgres"),
        ],
        ids=["dblink", "generate_series"],
    )
    def test_flags_table_valued_functions_as_unverifiable(self, sql, dialect):
        scan = collect_table_references(parse_sql(sql, dialect=dialect))
        assert scan.tables == []
        assert scan.unverifiable_sources

    @pytest.mark.parametrize(
        ("sql", "dialect"),
        [("SHOW TABLES", "snowflake"), ("SHOW COLUMNS FROM secret", "mysql")],
        ids=["show_tables", "show_columns"],
    )
    def test_flags_show_as_unverifiable(self, sql, dialect):
        scan = collect_table_references(parse_sql(sql, dialect=dialect))
        assert scan.unverifiable_sources

    def test_scalar_function_without_table_has_no_references(self):
        """A scalar function call references no table -- the allow-list does not cover it."""
        scan = collect_table_references(parse_sql("SELECT pg_read_file('/etc/passwd')", dialect="postgres"))
        assert scan.tables == []
        assert scan.unverifiable_sources == []

    @pytest.mark.parametrize(
        ("sql", "dialect"),
        [("EXEC sp_who", "tsql"), ("EXECUTE my_proc", "tsql")],
        ids=["exec", "execute"],
    )
    def test_flags_dynamic_sql_as_unverifiable(self, sql, dialect):
        """EXEC/EXECUTE hide their table access in text the parser can't read."""
        scan = collect_table_references(parse_sql(sql, dialect=dialect))
        assert scan.tables == []
        assert scan.unverifiable_sources
