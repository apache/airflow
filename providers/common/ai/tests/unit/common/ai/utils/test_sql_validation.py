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
    _DATA_MODIFYING_NODES,
    DEFAULT_ALLOWED_TYPES,
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

    @pytest.mark.parametrize(
        ("sql", "dialect"),
        [
            # File / large-object I/O.
            ("SELECT pg_read_file('/etc/passwd')", "postgres"),
            ("SELECT pg_read_binary_file('/etc/passwd')", "postgres"),
            ("SELECT lo_export(0, '/tmp/x')", "postgres"),
            ("SELECT lo_import('/etc/passwd')", "postgres"),
            # Query-carrying function: the string argument reaches another table.
            ("SELECT query_to_xml('SELECT * FROM secrets', true, false, '')", "postgres"),
            # table_to_xml names an off-list table directly (only a read role needed).
            ("SELECT table_to_xml('secrets', true, false, '')", "postgres"),
            # MySQL server-side file read.
            ("SELECT load_file('/etc/passwd')", "mysql"),
            # Scalar dblink: reaches a remote database through a string.
            ("SELECT dblink_exec('c', 'SELECT 1')", "postgres"),
            # pg_ls_dir sibling that lists another server directory.
            ("SELECT pg_ls_logdir()", "postgres"),
            # Nested inside a projection over an allowed table -- still caught.
            ("SELECT id, pg_read_file('/etc/passwd') FROM orders", "postgres"),
            # Case-insensitive: uppercased name is folded before the comparison.
            ("SELECT PG_READ_FILE('/etc/passwd')", "postgres"),
            # Schema-qualified call: the bare name sits on the nested Anonymous, still reached.
            ("SELECT pg_catalog.pg_read_file('/etc/passwd')", "postgres"),
        ],
        ids=[
            "pg_read_file",
            "pg_read_binary_file",
            "lo_export",
            "lo_import",
            "query_to_xml",
            "table_to_xml",
            "load_file_mysql",
            "dblink_exec",
            "pg_ls_logdir",
            "in_projection",
            "uppercase",
            "schema_qualified",
        ],
    )
    def test_flags_data_reaching_function_as_unverifiable(self, sql, dialect):
        """A function reaching a file/other table/program carries no table node and is
        not a sqlglot-typed builtin, so the fail-closed scan reports it unverifiable."""
        scan = collect_table_references(parse_sql(sql, dialect=dialect))
        assert scan.unverifiable_sources

    def test_typed_builtins_are_not_flagged(self):
        """Built-in scalar functions parse to typed nodes, not Anonymous, so they pass."""
        scan = collect_table_references(
            parse_sql("SELECT count(*), lower(name), coalesce(x, 0) FROM orders", dialect="postgres")
        )
        assert scan.tables == [("", "", "orders")]
        assert scan.unverifiable_sources == []

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT my_udf(name) FROM orders",  # a bespoke UDF sqlglot cannot type
            "SELECT json_build_object('id', id) FROM orders",  # a legit builtin sqlglot leaves Anonymous
        ],
        ids=["udf", "json_build_object"],
    )
    def test_unrecognized_function_flagged_by_default(self, sql):
        """Fail-closed: any function sqlglot cannot type is rejected unless allow-listed."""
        scan = collect_table_references(parse_sql(sql, dialect="postgres"))
        assert scan.unverifiable_sources

    def test_allowed_functions_permits_named_function(self):
        """A function named in allowed_functions passes, and its table is still checked."""
        scan = collect_table_references(
            parse_sql("SELECT json_build_object('id', id) FROM orders", dialect="postgres"),
            allowed_functions=frozenset({"json_build_object"}),
        )
        assert scan.tables == [("", "", "orders")]
        assert scan.unverifiable_sources == []

    def test_allowed_functions_matched_case_insensitively(self):
        """allowed_functions entries are compared case-folded, like the query's names."""
        scan = collect_table_references(
            parse_sql("SELECT JSON_BUILD_OBJECT('id', id) FROM orders", dialect="postgres"),
            allowed_functions=frozenset({"json_build_object"}),
        )
        assert scan.unverifiable_sources == []

    def test_allowed_functions_does_not_permit_a_different_function(self):
        """Allow-listing one function does not let a second, unlisted one through."""
        scan = collect_table_references(
            parse_sql(
                "SELECT json_build_object('x', pg_read_file('/etc/passwd')) FROM orders", dialect="postgres"
            ),
            allowed_functions=frozenset({"json_build_object"}),
        )
        assert scan.unverifiable_sources

    @pytest.mark.parametrize(
        "sql",
        [
            "COPY orders FROM PROGRAM 'id > /tmp/x'",
            "COPY orders FROM '/tmp/data.csv'",
            "COPY orders TO '/tmp/orders.csv'",
            "COPY orders TO PROGRAM 'cat > /tmp/x'",
        ],
        ids=["from_program", "from_file", "to_file", "to_program"],
    )
    def test_flags_copy_as_unverifiable(self, sql):
        """COPY moves data through a file/program channel the allow-list cannot describe."""
        scan = collect_table_references(parse_sql(sql, dialect="postgres"))
        assert scan.unverifiable_sources

    @pytest.mark.parametrize("sql", ["LIST @mystage", "LS @mystage"], ids=["list", "ls"])
    def test_flags_snowflake_stage_listing_as_unverifiable(self, sql):
        """Snowflake LIST/LS @stage mis-parse to a bare aliased expression (Column AS
        Parameter); reject it so it cannot list stage files past the allow-list."""
        scan = collect_table_references(parse_sql(sql, dialect="snowflake"))
        assert scan.unverifiable_sources

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


# sqlglot has no upper bound in pyproject.toml, so an upstream rename or split of an exp class
# would otherwise make the read-only check silently stop recognising a write.
# The tests below turn that drift into a CI failure; if one fails after a sqlglot bump, review
# _DATA_MODIFYING_NODES / DEFAULT_ALLOWED_TYPES in utils/sql_validation.py.
_DIALECTS = ["postgres", "mysql", "snowflake", "bigquery", "sqlite"]

_WRITE_STATEMENTS = {
    "insert": "INSERT INTO t (a) VALUES (1)",
    "update": "UPDATE t SET a = 1",
    "delete": "DELETE FROM t",
    "merge": "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = 1",
    "drop": "DROP TABLE t",
    "truncate": "TRUNCATE TABLE t",
    "create": "CREATE TABLE t (id INT)",
    "create_as_select": "CREATE TABLE t AS SELECT 1",
    "alter": "ALTER TABLE t ADD COLUMN c INT",
    "copy": "COPY t FROM '/tmp/x'",
    "grant": "GRANT SELECT ON t TO u",
    "cte_delete": "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
    "cte_insert": "WITH d AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM d",
    "select_into": "SELECT * INTO n FROM t",
}

# Node each write must parse to, keyed like _WRITE_STATEMENTS. copy and grant are absent
# on purpose: they are not in _DATA_MODIFYING_NODES and are only stopped by the top-level type check.
_WRITE_NODE_TYPES = {
    "insert": exp.Insert,
    "update": exp.Update,
    "delete": exp.Delete,
    "merge": exp.Merge,
    "drop": exp.Drop,
    "truncate": exp.TruncateTable,
    "create": exp.Create,
    "create_as_select": exp.Create,
    "alter": exp.Alter,
    "cte_delete": exp.Delete,
    "cte_insert": exp.Insert,
    "select_into": exp.Into,
}

_TOP_LEVEL_GUARD = "Statement type '{}' is not allowed"
_DEEP_SCAN_GUARD = "Data-modifying operation '{}'"

# Guard that must reject each write: the top-level type check, or the deep scan for writes
# nested inside an otherwise-allowed SELECT.
_WRITE_REJECTION = {
    "insert": _TOP_LEVEL_GUARD.format("Insert"),
    "update": _TOP_LEVEL_GUARD.format("Update"),
    "delete": _TOP_LEVEL_GUARD.format("Delete"),
    "merge": _TOP_LEVEL_GUARD.format("Merge"),
    "drop": _TOP_LEVEL_GUARD.format("Drop"),
    "truncate": _TOP_LEVEL_GUARD.format("TruncateTable"),
    "create": _TOP_LEVEL_GUARD.format("Create"),
    "create_as_select": _TOP_LEVEL_GUARD.format("Create"),
    "alter": _TOP_LEVEL_GUARD.format("Alter"),
    "copy": _TOP_LEVEL_GUARD.format("Copy"),
    "grant": _TOP_LEVEL_GUARD.format("Grant"),
    "cte_delete": _DEEP_SCAN_GUARD.format("Delete"),
    "cte_insert": _DEEP_SCAN_GUARD.format("Insert"),
    "select_into": _DEEP_SCAN_GUARD.format("Into"),
}

_READ_STATEMENTS = {
    "select": "SELECT a FROM t",
    "cte_select": "WITH c AS (SELECT 1 AS a) SELECT * FROM c",
    "union_all": "SELECT 1 UNION ALL SELECT 2",
}


class TestSqlglotDriftTripwires:
    """Fail loudly if a sqlglot upgrade silently changes what the read-only check recognises."""

    def test_allowed_types_never_overlap_data_modifying_nodes(self):
        """A write type slipping into the allow-list would bypass the statement-type check."""
        for allowed in DEFAULT_ALLOWED_TYPES:
            for denied in _DATA_MODIFYING_NODES:
                assert not issubclass(allowed, denied), f"{allowed.__name__} is a kind of {denied.__name__}"
                assert not issubclass(denied, allowed), f"{denied.__name__} is a kind of {allowed.__name__}"

    @pytest.mark.parametrize("dialect", _DIALECTS)
    @pytest.mark.parametrize(
        ("key", "node_type"), list(_WRITE_NODE_TYPES.items()), ids=list(_WRITE_NODE_TYPES)
    )
    def test_write_statement_parses_to_a_denied_node(self, key, node_type, dialect):
        """sqlglot must keep producing the node class the deny-list names for each write."""
        sql = _WRITE_STATEMENTS[key]
        parsed = parse_sql(sql, dialect=dialect)
        found = [n for stmt in parsed for n in stmt.walk() if isinstance(n, node_type)]
        assert found, f"{sql!r} no longer parses to {node_type.__name__}; review _DATA_MODIFYING_NODES"
        assert issubclass(node_type, _DATA_MODIFYING_NODES)

    @pytest.mark.parametrize("allow_read_only_metadata", [False, True])
    @pytest.mark.parametrize("dialect", _DIALECTS)
    @pytest.mark.parametrize("key", list(_WRITE_STATEMENTS))
    def test_write_and_ddl_statements_rejected_in_every_dialect(self, key, dialect, allow_read_only_metadata):
        with pytest.raises(SQLSafetyError, match=_WRITE_REJECTION[key]):
            validate_sql(
                _WRITE_STATEMENTS[key], dialect=dialect, allow_read_only_metadata=allow_read_only_metadata
            )

    @pytest.mark.parametrize("allow_read_only_metadata", [False, True])
    @pytest.mark.parametrize(
        ("sql", "dialect", "node_name"),
        [
            ("DESCRIBE ALTER TABLE t ADD COLUMN c INT", "postgres", "Alter"),
            ("DESCRIBE ALTER TABLE t ADD COLUMN c INT", "mysql", "Alter"),
            ("EXPLAIN DELETE FROM t", "mysql", "Delete"),
        ],
        ids=["describe_alter_postgres", "describe_alter_mysql", "explain_delete_mysql"],
    )
    def test_wrapped_write_rejected_by_the_guard_that_applies(
        self, sql, dialect, node_name, allow_read_only_metadata
    ):
        """Without the flag the wrapper itself is refused; with it, the wrapped write hits the deep scan."""
        if allow_read_only_metadata:
            match = _DEEP_SCAN_GUARD.format(node_name)
        else:
            match = _TOP_LEVEL_GUARD.format("Describe")
        with pytest.raises(SQLSafetyError, match=match):
            validate_sql(sql, dialect=dialect, allow_read_only_metadata=allow_read_only_metadata)

    @pytest.mark.parametrize("dialect", _DIALECTS)
    @pytest.mark.parametrize("sql", list(_READ_STATEMENTS.values()), ids=list(_READ_STATEMENTS))
    def test_read_statements_accepted_in_every_dialect(self, sql, dialect):
        assert validate_sql(sql, dialect=dialect)

    @pytest.mark.parametrize("name", ["nextval", "pg_terminate_backend", "dblink_exec"])
    def test_side_effect_function_is_flagged_only_when_allowed_tables_is_set(self, name):
        """validate_sql accepts these calls; only collect_table_references (used when
        allowed_tables is set) flags them, as exp.Anonymous functions with no exp.Table."""
        sql = f"SELECT {name}('x')"
        assert validate_sql(sql, dialect="postgres")
        parsed = parse_sql(sql, dialect="postgres")
        assert any(fn.name.casefold() == name for fn in parsed[0].find_all(exp.Anonymous)), (
            f"{name}() is no longer parsed as exp.Anonymous; the allowed_functions guard would miss it"
        )
        scan = collect_table_references(parsed)
        assert scan.unverifiable_sources
        assert name in scan.unverifiable_sources[0]
