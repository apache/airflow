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

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    sqlglot = None  # type: ignore[assignment]
    exp = None  # type: ignore[assignment]

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.utils.sql_validation import (
    SQLValidationError,
    parse_sql_query,
    resolve_sqlglot_dialect,
    validate_query,
)

needs_sqlglot = pytest.mark.skipif(sqlglot is None, reason="sqlglot is not installed")


@pytest.mark.parametrize(
    ("input_dialect", "expected_dialect"),
    [
        ("postgres", "postgres"),
        ("postgresql", "postgres"),
        ("PostgreSQL", "postgres"),
        ("mysql", "mysql"),
        ("MySQL", "mysql"),
        ("snowflake", "snowflake"),
        ("SnowFlake", "snowflake"),
        ("bigquery", "bigquery"),
        ("BigQuery", "bigquery"),
        ("sqlite", "sqlite"),
        ("SQLite", "sqlite"),
        ("mssql", "tsql"),
        ("tsql", "tsql"),
        ("microsoft-sql-server", "tsql"),
        ("microsoft_sql_server", "tsql"),
        ("postgresql+psycopg2", "postgres"),
        ("mysql+pymysql", "mysql"),
        ("mssql+pyodbc", "tsql"),
        ("snowflake+snowflake", "snowflake"),
        ("trino", "trino"),
        ("Trino", "trino"),
        ("redshift", "redshift"),
        ("Redshift", "redshift"),
        ("duckdb", "duckdb"),
        ("clickhouse", "clickhouse"),
        ("oracle", "oracle"),
        ("presto", "presto"),
        ("spark", "spark"),
        ("sparksql", "spark"),
        ("athena", "athena"),
        ("awsathena", "athena"),
        (None, None),
        ("", None),
        ("   ", None),
        ("unknown_custom_db", None),
        (12345, None),
    ],
)
def test_resolve_sqlglot_dialect(input_dialect, expected_dialect):
    assert resolve_sqlglot_dialect(input_dialect) == expected_dialect


def test_missing_sqlglot_raises_feature_exception(monkeypatch):
    import airflow.providers.common.sql.utils.sql_validation as mod

    monkeypatch.setattr(mod, "sqlglot", None)
    with pytest.raises(AirflowOptionalProviderFeatureException, match="sqlglot is required"):
        mod.validate_query("SELECT 1")

    with pytest.raises(AirflowOptionalProviderFeatureException, match="sqlglot is required"):
        mod.parse_sql_query("SELECT 1")

    with pytest.raises(AirflowOptionalProviderFeatureException, match="sqlglot is required"):
        mod.check_query_partition_filters([])


@pytest.mark.parametrize(
    "empty_query",
    ["", "   ", "\n\t  "],
)
def test_empty_sql_raises_validation_error(empty_query):
    with pytest.raises(SQLValidationError, match="cannot be empty"):
        parse_sql_query(empty_query)


def test_invalid_query_type_raises_validation_error():
    with pytest.raises(SQLValidationError, match="Expected SQL query as str or Sequence"):
        validate_query(12345)  # type: ignore[arg-type]


@needs_sqlglot
def test_valid_queries_return_ast():
    stmts = validate_query("SELECT id, name FROM users WHERE active = true")
    assert len(stmts) == 1
    assert isinstance(stmts[0], exp.Select)

    stmts_multi = validate_query(["SELECT 1", "SELECT 2"])
    assert len(stmts_multi) == 2
    assert all(isinstance(s, exp.Select) for s in stmts_multi)

    stmts_semicolon = validate_query("SELECT 1; SELECT 2")
    assert len(stmts_semicolon) == 2


@needs_sqlglot
def test_dialect_syntax_validation():
    tsql_sql = "SELECT TOP 10 * FROM users"
    tsql_ast = validate_query(tsql_sql, dialect="mssql")
    assert len(tsql_ast) == 1

    with pytest.raises(SQLValidationError, match="SQL syntax error for dialect 'postgres'"):
        validate_query(tsql_sql, dialect="postgres")


@needs_sqlglot
def test_partition_guard_unbounded_query():
    unbounded_sql = "SELECT * FROM large_table"
    with pytest.raises(
        SQLValidationError,
        match="Query on table\\(s\\) 'large_table' lacks a WHERE clause",
    ):
        validate_query(unbounded_sql, check_partition_filters=True)

    with pytest.raises(
        SQLValidationError,
        match="Query on table\\(s\\) 'large_table' lacks a WHERE clause",
    ):
        validate_query("SELECT * FROM large_table LIMIT 10", check_partition_filters=True)


@needs_sqlglot
def test_partition_guard_allow_unbounded_with_limit():
    bounded_sql = "SELECT * FROM large_table LIMIT 100"
    stmts = validate_query(bounded_sql, check_partition_filters=True, allow_unbounded=True)
    assert len(stmts) == 1


@needs_sqlglot
def test_partition_guard_select_without_table():
    stmts = validate_query("SELECT 1", check_partition_filters=True)
    assert len(stmts) == 1


@needs_sqlglot
def test_partition_guard_with_where_clause():
    stmts = validate_query("SELECT * FROM large_table WHERE id = 1", check_partition_filters=True)
    assert len(stmts) == 1


@needs_sqlglot
def test_partition_guard_with_configured_columns_sequence():
    valid_sql = "SELECT * FROM events WHERE ds = '2026-01-01' AND count > 5"
    stmts = validate_query(
        valid_sql,
        check_partition_filters=True,
        partition_columns=["ds", "date"],
    )
    assert len(stmts) == 1

    missing_partition_sql = "SELECT * FROM events WHERE count > 5"
    with pytest.raises(SQLValidationError, match="lacks required partition filter on column\\(s\\)"):
        validate_query(
            missing_partition_sql,
            check_partition_filters=True,
            partition_columns=["ds", "date"],
        )


@needs_sqlglot
def test_partition_guard_with_configured_columns_mapping():
    mapping = {
        "orders": ["order_date", "dt"],
        "customers": ["signup_date"],
    }
    valid_orders_sql = "SELECT * FROM orders WHERE order_date = '2026-01-01'"
    stmts = validate_query(valid_orders_sql, check_partition_filters=True, partition_columns=mapping)
    assert len(stmts) == 1

    invalid_orders_sql = "SELECT * FROM orders WHERE total > 100"
    with pytest.raises(
        SQLValidationError,
        match="Query on table 'orders' lacks required partition filter",
    ):
        validate_query(invalid_orders_sql, check_partition_filters=True, partition_columns=mapping)

    other_table_sql = "SELECT * FROM products WHERE price > 20"
    stmts2 = validate_query(other_table_sql, check_partition_filters=True, partition_columns=mapping)
    assert len(stmts2) == 1


@needs_sqlglot
def test_partition_guard_with_cte():
    valid_cte = "WITH cte AS (SELECT * FROM orders WHERE dt = '2026-01-01') SELECT * FROM cte"
    stmts = validate_query(valid_cte, check_partition_filters=True)
    assert len(stmts) == 1

    missing_filter_cte = "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte"
    with pytest.raises(SQLValidationError, match="lacks a WHERE clause or partition filter"):
        validate_query(missing_filter_cte, check_partition_filters=True)


@needs_sqlglot
def test_partition_guard_with_subquery():
    valid_subquery = "SELECT id FROM (SELECT * FROM orders WHERE dt = '2026-01-01') sub"
    stmts = validate_query(valid_subquery, check_partition_filters=True)
    assert len(stmts) == 1

    missing_filter_subquery = "SELECT id FROM (SELECT * FROM orders) sub"
    with pytest.raises(SQLValidationError, match="lacks a WHERE clause or partition filter"):
        validate_query(missing_filter_subquery, check_partition_filters=True)


@needs_sqlglot
def test_partition_guard_with_qualified_table_name():
    sql = "SELECT * FROM analytics.events WHERE dt = '2026-01-01'"
    stmts = validate_query(
        sql,
        check_partition_filters=True,
        partition_columns={"analytics.events": ["dt"]},
    )
    assert len(stmts) == 1

    invalid_sql = "SELECT * FROM analytics.events WHERE id = 1"
    with pytest.raises(SQLValidationError, match="lacks required partition filter"):
        validate_query(
            invalid_sql,
            check_partition_filters=True,
            partition_columns={"analytics.events": ["dt"]},
        )


@needs_sqlglot
def test_partition_guard_dml_statements():
    valid_delete = "DELETE FROM orders WHERE dt = '2026-01-01'"
    stmts = validate_query(valid_delete, check_partition_filters=True)
    assert len(stmts) == 1

    invalid_delete = "DELETE FROM orders"
    with pytest.raises(SQLValidationError, match="DELETE statement on 'orders' lacks a WHERE clause"):
        validate_query(invalid_delete, check_partition_filters=True)

    valid_update = "UPDATE orders SET status = 'shipped' WHERE id = 42"
    stmts2 = validate_query(valid_update, check_partition_filters=True)
    assert len(stmts2) == 1

    invalid_update = "UPDATE orders SET status = 'shipped'"
    with pytest.raises(SQLValidationError, match="UPDATE statement on 'orders' lacks a WHERE clause"):
        validate_query(invalid_update, check_partition_filters=True)


@needs_sqlglot
def test_partition_guard_dml_with_partition_columns():
    valid_delete = "DELETE FROM orders WHERE dt = '2026-01-01'"
    stmts = validate_query(valid_delete, check_partition_filters=True, partition_columns=["dt"])
    assert len(stmts) == 1

    missing_col_delete = "DELETE FROM orders WHERE status = 'old'"
    with pytest.raises(SQLValidationError, match="lacks required partition filter on column"):
        validate_query(missing_col_delete, check_partition_filters=True, partition_columns=["dt"])

    valid_update = "UPDATE orders SET status = 'shipped' WHERE dt = '2026-01-01'"
    stmts2 = validate_query(valid_update, check_partition_filters=True, partition_columns=["dt"])
    assert len(stmts2) == 1

    missing_col_update = "UPDATE orders SET status = 'shipped' WHERE id = 42"
    with pytest.raises(SQLValidationError, match="lacks required partition filter on column"):
        validate_query(missing_col_update, check_partition_filters=True, partition_columns=["dt"])


@needs_sqlglot
def test_partition_guard_single_str_column():
    valid_sql = "SELECT * FROM events WHERE dt = '2026-01-01'"
    stmts = validate_query(valid_sql, check_partition_filters=True, partition_columns="dt")
    assert len(stmts) == 1

    invalid_sql = "SELECT * FROM events WHERE other = 1"
    with pytest.raises(SQLValidationError, match="lacks required partition filter on column"):
        validate_query(invalid_sql, check_partition_filters=True, partition_columns="dt")


@needs_sqlglot
def test_partition_guard_compound_set_operations():
    valid_union = (
        "SELECT * FROM tbl1 WHERE dt = '2026-01-01' UNION ALL SELECT * FROM tbl2 WHERE dt = '2026-01-01'"
    )
    stmts = validate_query(valid_union, check_partition_filters=True, partition_columns=["dt"])
    assert len(stmts) == 1

    invalid_union = "SELECT * FROM tbl1 WHERE dt = '2026-01-01' UNION ALL SELECT * FROM tbl2"
    with pytest.raises(SQLValidationError, match="Query on table\\(s\\) 'tbl2' lacks a WHERE clause"):
        validate_query(invalid_union, check_partition_filters=True)

    bounded_union = "SELECT * FROM tbl1 UNION ALL SELECT * FROM tbl2 LIMIT 10"
    stmts2 = validate_query(bounded_union, check_partition_filters=True, allow_unbounded=True)
    assert len(stmts2) == 1

    valid_intersect = (
        "SELECT * FROM tbl1 WHERE dt = '2026-01-01' INTERSECT SELECT * FROM tbl2 WHERE dt = '2026-01-01'"
    )
    stmts3 = validate_query(valid_intersect, check_partition_filters=True, partition_columns=["dt"])
    assert len(stmts3) == 1

    valid_except = (
        "SELECT * FROM tbl1 WHERE dt = '2026-01-01' EXCEPT SELECT * FROM tbl2 WHERE dt = '2026-01-01'"
    )
    stmts4 = validate_query(valid_except, check_partition_filters=True, partition_columns=["dt"])
    assert len(stmts4) == 1


@needs_sqlglot
def test_partition_guard_multi_table_comma_joins():
    valid_sql = "SELECT * FROM table_a, table_b WHERE table_a.id = 1 AND table_b.dt = '2026-01-01'"
    stmts = validate_query(valid_sql, check_partition_filters=True, partition_columns={"table_b": ["dt"]})
    assert len(stmts) == 1

    invalid_sql = "SELECT * FROM table_a, table_b WHERE table_a.id = 1"
    with pytest.raises(
        SQLValidationError,
        match="Query on table 'table_b' lacks required partition filter",
    ):
        validate_query(
            invalid_sql,
            check_partition_filters=True,
            partition_columns={"table_b": ["dt"]},
        )


@needs_sqlglot
def test_partition_guard_join_on_clause_partition_filter():
    valid_join = (
        "SELECT * FROM dim LEFT JOIN large_fact "
        "ON dim.id = large_fact.id AND large_fact.dt = '2026-01-01' "
        "WHERE dim.active = true"
    )
    stmts = validate_query(
        valid_join,
        check_partition_filters=True,
        partition_columns={"large_fact": ["dt"]},
    )
    assert len(stmts) == 1


@needs_sqlglot
def test_partition_guard_cte_name_collision_with_schema():
    valid_cte = "WITH orders AS (SELECT * FROM raw.orders WHERE dt = '2026-01-01') SELECT * FROM orders"
    stmts = validate_query(valid_cte, check_partition_filters=True)
    assert len(stmts) == 1

    invalid_cte = "WITH orders AS (SELECT * FROM raw.orders) SELECT * FROM orders"
    with pytest.raises(SQLValidationError, match="lacks a WHERE clause or partition filter"):
        validate_query(invalid_cte, check_partition_filters=True)


@needs_sqlglot
def test_partition_guard_insert_into_select():
    valid_insert = "INSERT INTO target_tbl SELECT * FROM source_tbl WHERE dt = '2026-01-01'"
    stmts = validate_query(
        valid_insert,
        check_partition_filters=True,
        partition_columns={"target_tbl": ["dt"]},
    )
    assert len(stmts) == 1

    invalid_insert = "INSERT INTO target_tbl SELECT * FROM source_tbl WHERE other_col = 1"
    with pytest.raises(
        SQLValidationError,
        match="Query on table 'target_tbl' lacks required partition filter",
    ):
        validate_query(
            invalid_insert,
            check_partition_filters=True,
            partition_columns={"target_tbl": ["dt"]},
        )

    values_insert = "INSERT INTO target_tbl (id, dt) VALUES (1, '2026-01-01')"
    stmts2 = validate_query(
        values_insert,
        check_partition_filters=True,
        partition_columns={"target_tbl": ["dt"]},
    )
    assert len(stmts2) == 1


@needs_sqlglot
def test_partition_guard_multi_table_join_partial_partitioning():
    mapping = {"large_fact": ["ds"]}
    valid_join = (
        "SELECT * FROM large_fact lf JOIN dim_users du ON lf.user_id = du.id "
        "JOIN dim_status ds ON lf.status_id = ds.id WHERE lf.ds = '2026-01-01'"
    )
    stmts = validate_query(valid_join, check_partition_filters=True, partition_columns=mapping)
    assert len(stmts) == 1

    missing_partition_join = (
        "SELECT * FROM large_fact lf JOIN dim_users du ON lf.user_id = du.id "
        "JOIN dim_status ds ON lf.status_id = ds.id WHERE du.active = true"
    )
    with pytest.raises(
        SQLValidationError,
        match="Query on table 'large_fact' lacks required partition filter",
    ):
        validate_query(
            missing_partition_join,
            check_partition_filters=True,
            partition_columns=mapping,
        )


@needs_sqlglot
def test_parameterized_sql_with_dbapi_format():
    sql_mysql = "SELECT * FROM users WHERE id = %s AND ds = %s"
    stmts_mysql = validate_query(
        sql_mysql,
        dialect="mysql",
        check_partition_filters=True,
        partition_columns=["ds"],
    )
    assert len(stmts_mysql) == 1

    sql_named = "SELECT * FROM users WHERE id = %(id)s AND ds = %(ds)s"
    stmts_named = validate_query(
        sql_named,
        dialect="mysql",
        check_partition_filters=True,
        partition_columns=["ds"],
    )
    assert len(stmts_named) == 1


@needs_sqlglot
def test_partition_guard_table_alias_isolation():
    mapping = {"large_fact": ["ds"]}
    sql_wrong_alias = (
        "SELECT * FROM large_fact lf JOIN dim_users du ON lf.user_id = du.id WHERE du.ds = '2026-01-01'"
    )
    with pytest.raises(
        SQLValidationError,
        match="Query on table 'large_fact' lacks required partition filter",
    ):
        validate_query(
            sql_wrong_alias,
            check_partition_filters=True,
            partition_columns=mapping,
        )

    sql_right_alias = (
        "SELECT * FROM large_fact lf JOIN dim_users du ON lf.user_id = du.id "
        "WHERE lf.ds = '2026-01-01' AND du.id = 1"
    )
    stmts = validate_query(
        sql_right_alias,
        check_partition_filters=True,
        partition_columns=mapping,
    )
    assert len(stmts) == 1
