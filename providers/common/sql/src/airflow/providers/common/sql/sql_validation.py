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
except ImportError as err:
    raise AirflowOptionalProviderFeatureException(
        "sqlglot is required for SQL query pre-flight validation. "
        "Install it with: pip install 'apache-airflow-providers-common-sql[sqlglot]'"
    ) from err

_KNOWN_SQLGLOT_DIALECTS: frozenset[str] = frozenset(d.value for d in Dialects)

_DIALECT_MAPPING: dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "google_cloud_platform": "bigquery",
    "gcpbigquery": "bigquery",
    "gcp": "bigquery",
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

_VALID_BOUNDING_TYPES = (
    exp.EQ,
    exp.In,
    exp.Between,
    exp.GTE,
    exp.LTE,
    exp.GT,
    exp.LT,
)


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


def parse_sql_query(
    sql: str,
    *,
    dialect: str | None = None,
) -> list[Any]:
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


def _is_tautology(expr: Any) -> bool:
    if expr is None:
        return False
    if isinstance(expr, exp.Paren):
        return _is_tautology(expr.this)
    if isinstance(expr, exp.Boolean):
        return expr.this is True
    if isinstance(expr, exp.Literal):
        return expr.is_number and expr.this == "1"
    if isinstance(expr, exp.EQ):
        left, right = expr.this, expr.expression
        if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
            return str(left.this) == str(right.this)
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            tbl_left = getattr(left, "table", "") or ""
            tbl_right = getattr(right, "table", "") or ""
            return left.name.lower() == right.name.lower() and tbl_left.lower() == tbl_right.lower()
    return False


def _is_pure_tautology(expr: Any) -> bool:
    if expr is None:
        return True
    if isinstance(expr, exp.Paren):
        return _is_pure_tautology(expr.this)
    if _is_tautology(expr):
        return True
    if isinstance(expr, exp.And):
        return _is_pure_tautology(expr.this) and _is_pure_tautology(expr.expression)
    if isinstance(expr, exp.Or):
        return _is_pure_tautology(expr.this) and _is_pure_tautology(expr.expression)
    return False


def _extract_columns_excluding_subqueries(node: Any) -> list[exp.Column]:
    cols: list[exp.Column] = []
    if node is None:
        return cols
    stack = [node]
    while stack:
        curr = stack.pop()
        if curr is not node and isinstance(curr, (exp.Select, exp.Subquery)):
            continue
        if isinstance(curr, exp.Column):
            cols.append(curr)
        for child in curr.args.values():
            if isinstance(child, list):
                stack.extend(c for c in child if isinstance(c, exp.Expression))
            elif isinstance(child, exp.Expression):
                stack.append(child)
    return cols


def _column_matches_table(
    col: exp.Column,
    target_table: str,
    target_cols: set[str],
    table_alias_map: dict[str, str],
    valid_tables: set[str] | None = None,
) -> bool:
    col_name = col.name.lower()
    if col_name not in target_cols:
        return False
    col_table = getattr(col, "table", None)
    if col_table:
        resolved = table_alias_map.get(col_table.lower(), col_table.lower())
        return resolved == target_table.lower()
    if valid_tables is not None:
        return target_table.lower() in valid_tables
    return True


def _is_predicate_bounding_column(
    expr: Any,
    target_table: str,
    target_cols: set[str],
    table_alias_map: dict[str, str],
    valid_tables: set[str] | None = None,
) -> bool:
    if not isinstance(expr, _VALID_BOUNDING_TYPES):
        return False

    if isinstance(expr, (exp.EQ, exp.GTE, exp.LTE, exp.GT, exp.LT)):
        left_cols = _extract_columns_excluding_subqueries(expr.this)
        right_cols = _extract_columns_excluding_subqueries(expr.expression)

        left_matches = any(
            _column_matches_table(c, target_table, target_cols, table_alias_map, valid_tables)
            for c in left_cols
        )
        right_matches = any(
            _column_matches_table(c, target_table, target_cols, table_alias_map, valid_tables)
            for c in right_cols
        )

        if left_matches and not right_cols:
            return True
        if right_matches and not left_cols:
            return True
        return False

    if isinstance(expr, exp.Between):
        if expr.args.get("is_negated"):
            return False
        this_cols = _extract_columns_excluding_subqueries(expr.this)
        bound_cols = _extract_columns_excluding_subqueries(
            expr.args.get("low")
        ) + _extract_columns_excluding_subqueries(expr.args.get("high"))
        if any(
            _column_matches_table(c, target_table, target_cols, table_alias_map, valid_tables)
            for c in this_cols
        ):
            return len(bound_cols) == 0
        return False

    if isinstance(expr, exp.In):
        if expr.args.get("is_negated"):
            return False
        this_cols = _extract_columns_excluding_subqueries(expr.this)
        val_cols: list[exp.Column] = []
        for val in expr.expressions or []:
            val_cols.extend(_extract_columns_excluding_subqueries(val))
        if any(
            _column_matches_table(c, target_table, target_cols, table_alias_map, valid_tables)
            for c in this_cols
        ):
            return len(val_cols) == 0
        return False

    return False


def _is_condition_bounding(
    expr: Any,
    target_table: str,
    target_cols: set[str],
    table_alias_map: dict[str, str],
    valid_tables: set[str] | None = None,
) -> bool:
    if expr is None:
        return False
    if isinstance(expr, exp.Paren):
        return _is_condition_bounding(expr.this, target_table, target_cols, table_alias_map, valid_tables)
    if isinstance(expr, exp.Not):
        return False
    if isinstance(expr, exp.And):
        return _is_condition_bounding(
            expr.this, target_table, target_cols, table_alias_map, valid_tables
        ) or _is_condition_bounding(expr.expression, target_table, target_cols, table_alias_map, valid_tables)
    if isinstance(expr, exp.Or):
        return _is_condition_bounding(
            expr.this, target_table, target_cols, table_alias_map, valid_tables
        ) and _is_condition_bounding(
            expr.expression, target_table, target_cols, table_alias_map, valid_tables
        )
    return _is_predicate_bounding_column(expr, target_table, target_cols, table_alias_map, valid_tables)


def _get_applicable_filters_for_table(
    target_tbl_name: str,
    select_node: exp.Select,
    base_table_names: set[str],
) -> list[Any]:
    applicable: list[Any] = []
    where_clause = select_node.args.get("where")
    if where_clause is not None and not _is_pure_tautology(where_clause.this):
        applicable.append(where_clause.this)

    preceding = set(base_table_names)
    for join in select_node.args.get("joins") or []:
        on_clause = join.args.get("on")
        if on_clause is None or _is_pure_tautology(on_clause):
            continue

        joined_tbl = _extract_table_from_node(join.this)
        joined_tbl_name = joined_tbl.name.lower() if joined_tbl and joined_tbl.name else ""

        side = (join.side or "").upper()
        if side == "LEFT":
            if target_tbl_name == joined_tbl_name:
                applicable.append(on_clause)
        elif side == "RIGHT":
            if target_tbl_name in preceding:
                applicable.append(on_clause)
        elif side == "FULL":
            pass
        else:
            if target_tbl_name == joined_tbl_name or target_tbl_name in preceding:
                applicable.append(on_clause)

        if joined_tbl_name:
            preceding.add(joined_tbl_name)

    return applicable


def check_query_partition_filters(
    statements: list[Any],
    *,
    partition_columns: str | Sequence[str] | set[str] | Mapping[str, Sequence[str] | set[str]] | None = None,
    allow_unbounded: bool = False,
) -> None:
    for stmt in statements:
        if stmt is None:
            continue
        cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE) if cte.alias_or_name}
        select_nodes = list(stmt.find_all(exp.Select))
        if isinstance(stmt, exp.Select) and stmt not in select_nodes:
            select_nodes.insert(0, stmt)

        root_limit = stmt.args.get("limit") if hasattr(stmt, "args") else None

        for select_node in select_nodes:
            base_tables: list[exp.Table] = []
            from_clause = select_node.args.get("from_") or select_node.args.get("from")
            if from_clause:
                tbl = _extract_table_from_node(from_clause.this)
                if tbl is not None:
                    base_tables.append(tbl)
                for expr in getattr(from_clause, "expressions", None) or []:
                    expr_tbl = _extract_table_from_node(expr)
                    if expr_tbl is not None:
                        base_tables.append(expr_tbl)

            joined_tables: list[exp.Table] = []
            for join in select_node.args.get("joins") or []:
                tbl = _extract_table_from_node(join.this)
                if tbl is not None:
                    joined_tables.append(tbl)

            all_direct_tables = base_tables + joined_tables
            tables = [
                t
                for t in all_direct_tables
                if t.name and not (not getattr(t, "db", "") and t.name.lower() in cte_names)
            ]
            if not tables:
                continue

            table_alias_map: dict[str, str] = {}
            for t in tables:
                tbl_name = t.name.lower()
                table_alias_map[tbl_name] = tbl_name
                if getattr(t, "alias", None):
                    table_alias_map[t.alias.lower()] = tbl_name
                if getattr(t, "db", None):
                    table_alias_map[f"{t.db}.{t.name}".lower()] = tbl_name

            base_table_names = {t.name.lower() for t in base_tables if t.name}
            valid_table_names = {t.name.lower() for t in tables}
            limit_clause = select_node.args.get("limit")
            is_unbounded_allowed = (limit_clause is not None or root_limit is not None) and allow_unbounded

            if partition_columns is not None:
                if isinstance(partition_columns, str):
                    partition_columns = [partition_columns]

                if isinstance(partition_columns, Mapping):
                    for table in tables:
                        tbl_name = table.name.lower()
                        full_name = (
                            f"{table.db}.{table.name}".lower() if getattr(table, "db", None) else tbl_name
                        )
                        matching_key = None
                        for key in partition_columns:
                            if key.lower() in (tbl_name, full_name):
                                matching_key = key
                                break
                        if matching_key is not None:
                            val = partition_columns[matching_key]
                            col_list = [val] if isinstance(val, str) else val
                            required_cols = {c.lower() for c in col_list}

                            applicable_filters = _get_applicable_filters_for_table(
                                tbl_name, select_node, base_table_names
                            )
                            table_bounded = any(
                                _is_condition_bounding(
                                    f_root, tbl_name, required_cols, table_alias_map, valid_table_names
                                )
                                for f_root in applicable_filters
                            )
                            if not table_bounded:
                                if is_unbounded_allowed:
                                    continue
                                raise SQLValidationError(
                                    f"Query on table '{table.name}' lacks required partition filter on {sorted(required_cols)}."
                                )
                elif isinstance(partition_columns, (Sequence, set, frozenset)):
                    required_cols = {c.lower() for c in partition_columns}
                    any_bounded = False
                    for table in tables:
                        tbl_name = table.name.lower()
                        applicable_filters = _get_applicable_filters_for_table(
                            tbl_name, select_node, base_table_names
                        )
                        if any(
                            _is_condition_bounding(
                                f_root, tbl_name, required_cols, table_alias_map, valid_table_names
                            )
                            for f_root in applicable_filters
                        ):
                            any_bounded = True
                            break
                    if not any_bounded:
                        if not is_unbounded_allowed:
                            raise SQLValidationError(
                                f"Query lacks required partition filter on column(s): {sorted(required_cols)}."
                            )
            else:
                where_clause = select_node.args.get("where")
                has_effective_where = where_clause is not None and not _is_pure_tautology(where_clause.this)
                if not has_effective_where:
                    if is_unbounded_allowed:
                        continue
                    uncovered_tables = [
                        t.name
                        for t in tables
                        if not _get_applicable_filters_for_table(
                            t.name.lower(), select_node, base_table_names
                        )
                    ]
                    if uncovered_tables:
                        table_names = ", ".join(uncovered_tables)
                        raise SQLValidationError(
                            f"Query on table(s) '{table_names}' lacks a WHERE clause or partition filter."
                        )

        if isinstance(stmt, exp.Insert):
            target_tbl = _extract_table_from_node(stmt.args.get("this"))
            if (
                target_tbl is not None
                and target_tbl.name
                and partition_columns is not None
                and isinstance(partition_columns, Mapping)
            ):
                tbl_name = target_tbl.name.lower()
                full_name = (
                    f"{target_tbl.db}.{target_tbl.name}".lower()
                    if getattr(target_tbl, "db", None)
                    else tbl_name
                )
                matching_key = None
                for key in partition_columns:
                    if key.lower() in (tbl_name, full_name):
                        matching_key = key
                        break
                if matching_key is not None:
                    part_clause = getattr(target_tbl, "args", {}).get("partition")
                    part_bounded = False
                    if part_clause and hasattr(part_clause, "find_all"):
                        val = partition_columns[matching_key]
                        col_list = [val] if isinstance(val, str) else val
                        required_cols = {c.lower() for c in col_list}
                        for eq in part_clause.find_all(exp.EQ):
                            cols = _extract_columns_excluding_subqueries(eq.this)
                            if any(c.name.lower() in required_cols for c in cols):
                                part_bounded = True
                                break
                    if not part_bounded:
                        val = partition_columns[matching_key]
                        col_list = [val] if isinstance(val, str) else val
                        required_cols = {c.lower() for c in col_list}
                        raise SQLValidationError(
                            f"INSERT statement on table '{target_tbl.name}' lacks required partition specification on {sorted(required_cols)}."
                        )

        if isinstance(stmt, (exp.Delete, exp.Update)):
            where_clause = stmt.args.get("where")
            limit_clause = stmt.args.get("limit")
            target = stmt.args.get("this")
            tables = [t for t in target.find_all(exp.Table) if t.name] if target else []
            table_names = ", ".join(t.name for t in tables) or "target"
            has_effective_where = where_clause is not None and not _is_pure_tautology(where_clause.this)
            if not has_effective_where:
                if limit_clause is not None and allow_unbounded:
                    continue
                raise SQLValidationError(
                    f"{type(stmt).__name__.upper()} statement on '{table_names}' lacks a WHERE clause."
                )
            if partition_columns is not None and tables and where_clause is not None:
                valid_tables = {t.name.lower() for t in tables}
                table_alias_map = {t.name.lower(): t.name.lower() for t in tables}
                where_root = where_clause.this
                if isinstance(partition_columns, Mapping):
                    for table in tables:
                        tbl_name = table.name.lower()
                        if tbl_name in partition_columns:
                            val = partition_columns[tbl_name]
                            required_cols = {c.lower() for c in ([val] if isinstance(val, str) else val)}
                            if not _is_condition_bounding(
                                where_root, tbl_name, required_cols, table_alias_map, valid_tables
                            ):
                                raise SQLValidationError(
                                    f"{type(stmt).__name__.upper()} statement on '{table.name}' lacks required partition filter on {sorted(required_cols)}."
                                )
                elif isinstance(partition_columns, (Sequence, set, frozenset)):
                    required_cols = {c.lower() for c in partition_columns}
                    any_bounded = any(
                        _is_condition_bounding(
                            where_root, t.name.lower(), required_cols, table_alias_map, valid_tables
                        )
                        for t in tables
                    )
                    if not any_bounded:
                        raise SQLValidationError(
                            f"{type(stmt).__name__.upper()} statement lacks required partition filter on column(s): {sorted(required_cols)}."
                        )


def validate_query(
    sql: str | Sequence[str],
    *,
    dialect: str | None = None,
    check_partition_filters: bool = False,
    partition_columns: str | Sequence[str] | set[str] | Mapping[str, Sequence[str] | set[str]] | None = None,
    allow_unbounded: bool = False,
) -> list[Any]:
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
