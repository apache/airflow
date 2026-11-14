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

import re
from collections.abc import Mapping, Sequence
from typing import Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.dialects import Dialects
    from sqlglot.errors import ErrorLevel, ParseError
except ImportError:
    sqlglot = None
    exp = None
    Dialects = None
    ErrorLevel = None
    ParseError = None

_KNOWN_SQLGLOT_DIALECTS: frozenset[str] = (
    frozenset(d.value for d in Dialects) if Dialects is not None else frozenset()
)

_DIALECT_MAPPING: dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "sqlite": "sqlite",
    "mssql": "tsql",
    "tsql": "tsql",
    "microsoft-sql-server": "tsql",
    "microsoft_sql_server": "tsql",
    "trino": "trino",
    "redshift": "redshift",
    "awsathena": "athena",
    "athena": "athena",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "oracle": "oracle",
    "presto": "presto",
    "spark": "spark",
    "sparksql": "spark",
    "databricks": "databricks",
}

_DBAPI_PARAM_PATTERN: re.Pattern = re.compile(r"%\([a-zA-Z0-9_]+\)s|%s")


class SQLValidationError(ValueError):
    """Raised when SQL pre-flight validation fails."""


def resolve_sqlglot_dialect(dialect_name: str | None) -> str | None:
    if not isinstance(dialect_name, str) or not dialect_name.strip():
        return None
    normalized = dialect_name.strip().lower()
    if "+" in normalized:
        normalized = normalized.split("+")[0]
    if normalized in _DIALECT_MAPPING:
        return _DIALECT_MAPPING[normalized]
    if _KNOWN_SQLGLOT_DIALECTS and normalized in _KNOWN_SQLGLOT_DIALECTS:
        return normalized
    return None


def _require_sqlglot() -> None:
    if sqlglot is None:
        raise AirflowOptionalProviderFeatureException(
            "sqlglot is required for SQL query pre-flight validation. "
            "Install it with: pip install 'apache-airflow-providers-common-sql[sqlglot]'"
        )


def parse_sql_query(
    sql: str,
    *,
    dialect: str | None = None,
) -> list[Any]:
    _require_sqlglot()
    if not sql or not sql.strip():
        raise SQLValidationError("SQL query string cannot be empty.")
    resolved_dialect = resolve_sqlglot_dialect(dialect) if dialect else None
    try:
        statements = sqlglot.parse(sql, read=resolved_dialect, error_level=ErrorLevel.RAISE)
    except ParseError as err:
        if _DBAPI_PARAM_PATTERN.search(sql):
            try:
                norm_sql = _DBAPI_PARAM_PATTERN.sub("?", sql)
                statements = sqlglot.parse(norm_sql, read=resolved_dialect, error_level=ErrorLevel.RAISE)
            except Exception:
                dialect_msg = f" for dialect '{resolved_dialect}'" if resolved_dialect else ""
                raise SQLValidationError(f"SQL syntax error{dialect_msg}: {err}") from err
        else:
            dialect_msg = f" for dialect '{resolved_dialect}'" if resolved_dialect else ""
            raise SQLValidationError(f"SQL syntax error{dialect_msg}: {err}") from err
    except Exception as err:
        raise SQLValidationError(f"Failed to parse SQL: {err}") from err

    parsed = [s for s in statements if s is not None]
    if not parsed:
        raise SQLValidationError("SQL query string cannot be empty.")
    return parsed


def _extract_table_from_node(node: Any) -> exp.Table | None:
    if isinstance(node, exp.Table):
        return node
    if hasattr(node, "this") and isinstance(node.this, exp.Table):
        return node.this
    return None


def _validate_partition_columns_in_filters(
    filter_nodes: list[Any],
    tables: list[Any],
    partition_columns: str | Sequence[str] | set[str] | Mapping[str, Sequence[str] | set[str]],
) -> None:
    table_alias_map: dict[str, str] = {}
    for t in tables:
        tbl_name = t.name.lower()
        table_alias_map[tbl_name] = tbl_name
        if getattr(t, "alias", None):
            table_alias_map[t.alias.lower()] = tbl_name
        if getattr(t, "db", None):
            table_alias_map[f"{t.db}.{t.name}".lower()] = tbl_name

    columns_in_filters: set[str] = set()
    table_qualified_columns: dict[str, set[str]] = {}
    for f_node in filter_nodes:
        if f_node is None:
            continue
        for col in f_node.find_all(exp.Column):
            if col.name:
                col_name = col.name.lower()
                columns_in_filters.add(col_name)
                col_tbl = getattr(col, "table", None)
                if col_tbl:
                    resolved_tbl = table_alias_map.get(col_tbl.lower(), col_tbl.lower())
                    table_qualified_columns.setdefault(resolved_tbl, set()).add(col_name)

    if isinstance(partition_columns, str):
        partition_columns = [partition_columns]

    if isinstance(partition_columns, Mapping):
        for table in tables:
            tbl_name = table.name.lower()
            full_name = f"{table.db}.{table.name}".lower() if getattr(table, "db", None) else tbl_name
            matching_key = None
            for key in partition_columns:
                if key.lower() in (tbl_name, full_name):
                    matching_key = key
                    break
            if matching_key is not None:
                val = partition_columns[matching_key]
                col_list = [val] if isinstance(val, str) else val
                required_cols = {c.lower() for c in col_list}
                matching_cols = table_qualified_columns.get(tbl_name, set())
                unqualified_cols = columns_in_filters - {
                    c for cols in table_qualified_columns.values() for c in cols
                }
                available = matching_cols | unqualified_cols
                if not available.intersection(required_cols):
                    raise SQLValidationError(
                        f"Query on table '{table.name}' lacks required partition filter on {sorted(required_cols)}."
                    )
    elif isinstance(partition_columns, (Sequence, set, frozenset)):
        required_cols = {c.lower() for c in partition_columns}
        if not columns_in_filters.intersection(required_cols):
            raise SQLValidationError(
                f"Query lacks required partition filter on column(s): {sorted(required_cols)}."
            )


def check_query_partition_filters(
    statements: list[Any],
    *,
    partition_columns: str | Sequence[str] | set[str] | Mapping[str, Sequence[str] | set[str]] | None = None,
    allow_unbounded: bool = False,
) -> None:
    _require_sqlglot()
    for stmt in statements:
        if stmt is None:
            continue
        cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE) if cte.alias_or_name}
        select_nodes = list(stmt.find_all(exp.Select))
        if isinstance(stmt, exp.Select) and stmt not in select_nodes:
            select_nodes.insert(0, stmt)

        root_limit = stmt.args.get("limit") if hasattr(stmt, "args") else None

        for select_node in select_nodes:
            direct_tables: list[exp.Table] = []
            from_clause = select_node.args.get("from_") or select_node.args.get("from")
            if from_clause:
                tbl = _extract_table_from_node(from_clause.this)
                if tbl is not None:
                    direct_tables.append(tbl)
                for expr in getattr(from_clause, "expressions", None) or []:
                    expr_tbl = _extract_table_from_node(expr)
                    if expr_tbl is not None:
                        direct_tables.append(expr_tbl)

            join_on_nodes: list[Any] = []
            for join in select_node.args.get("joins") or []:
                tbl = _extract_table_from_node(join.this)
                if tbl is not None:
                    direct_tables.append(tbl)
                on_clause = join.args.get("on")
                if on_clause is not None:
                    join_on_nodes.append(on_clause)

            tables = [
                t
                for t in direct_tables
                if t.name and not (not getattr(t, "db", "") and t.name.lower() in cte_names)
            ]
            if not tables:
                continue

            where_clause = select_node.args.get("where")
            limit_clause = select_node.args.get("limit")

            if where_clause is None:
                if (limit_clause is not None or root_limit is not None) and allow_unbounded:
                    continue
                table_names = ", ".join(t.name for t in tables)
                raise SQLValidationError(
                    f"Query on table(s) '{table_names}' lacks a WHERE clause or partition filter."
                )

            filter_nodes = [where_clause] + join_on_nodes

            if partition_columns is not None:
                all_check_tables = list(tables)
                if isinstance(stmt, exp.Insert):
                    target_tbl = _extract_table_from_node(stmt.args.get("this"))
                    if target_tbl is not None and target_tbl not in all_check_tables:
                        all_check_tables.append(target_tbl)
                _validate_partition_columns_in_filters(filter_nodes, all_check_tables, partition_columns)

        if isinstance(stmt, (exp.Delete, exp.Update)):
            where_clause = stmt.args.get("where")
            limit_clause = stmt.args.get("limit")
            target = stmt.args.get("this")
            tables = [t for t in target.find_all(exp.Table) if t.name] if target else []
            table_names = ", ".join(t.name for t in tables) or "target"
            if where_clause is None:
                if limit_clause is not None and allow_unbounded:
                    continue
                raise SQLValidationError(
                    f"{type(stmt).__name__.upper()} statement on '{table_names}' lacks a WHERE clause."
                )
            if partition_columns is not None and tables:
                _validate_partition_columns_in_filters([where_clause], tables, partition_columns)


def validate_query(
    sql: str | Sequence[str],
    *,
    dialect: str | None = None,
    check_partition_filters: bool = False,
    partition_columns: str | Sequence[str] | set[str] | Mapping[str, Sequence[str] | set[str]] | None = None,
    allow_unbounded: bool = False,
) -> list[Any]:
    _require_sqlglot()
    if isinstance(sql, str):
        sql_list = [sql]
    elif isinstance(sql, (Sequence, set)):
        sql_list = list(sql)
    else:
        raise SQLValidationError(f"Expected SQL query as str or Sequence[str], got {type(sql).__name__}.")

    all_statements: list[Any] = []
    for s in sql_list:
        parsed_statements = parse_sql_query(s, dialect=dialect)
        all_statements.extend(parsed_statements)

    if check_partition_filters:
        check_query_partition_filters(
            all_statements,
            partition_columns=partition_columns,
            allow_unbounded=allow_unbounded,
        )

    return all_statements
