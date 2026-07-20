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
"""
SQL safety validation for LLM-generated queries.

Uses an allowlist approach: only explicitly permitted statement types pass.
This is safer than a denylist because new/unexpected statement types
(INSERT, UPDATE, MERGE, TRUNCATE, COPY, etc.) are blocked by default.
"""

from __future__ import annotations

from typing import NamedTuple

import sqlglot
from sqlglot import exp
from sqlglot.dialects import Dialects
from sqlglot.errors import ErrorLevel

# Dialect names sqlglot recognizes. Used to drop unknown dialect names so a bad
# value never breaks parsing (sqlglot raises on an unknown dialect).
_KNOWN_SQLGLOT_DIALECTS: frozenset[str] = frozenset(d.value for d in Dialects)

# SQLAlchemy ``dialect_name`` → sqlglot dialect mapping for names that differ.
_SQLALCHEMY_TO_SQLGLOT_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mssql": "tsql",
}


def resolve_sqlglot_dialect(dialect_name: str | None) -> str | None:
    """
    Normalize a SQLAlchemy dialect name to a sqlglot dialect.

    Returns ``None`` (dialect-agnostic parsing) for empty, non-string, or
    unknown inputs, so a bad dialect value never breaks SQL validation.

    :param dialect_name: A SQLAlchemy ``dialect_name`` (e.g. ``"postgresql"``).
    :return: The matching sqlglot dialect (e.g. ``"postgres"``), or ``None``.
    """
    if not isinstance(dialect_name, str) or not dialect_name:
        return None
    mapped = _SQLALCHEMY_TO_SQLGLOT_DIALECT.get(dialect_name, dialect_name)
    return mapped if mapped in _KNOWN_SQLGLOT_DIALECTS else None


# Allowlist: only these top-level statement types pass validation by default.
# - Select: plain queries and CTE-wrapped queries (WITH ... AS ... SELECT is parsed
#   as Select with a `with` clause property — still a Select node at the top level)
# - Union/Intersect/Except: set operations on SELECT results
DEFAULT_ALLOWED_TYPES: tuple[type[exp.Expr], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
)

# Read-only metadata statements that introspect the schema without touching data:
# - Describe: DESCRIBE / DESC <table> (and EXPLAIN on some dialects)
# - Show: SHOW TABLES / SHOW COLUMNS / SHOW DATABASES, etc.
# Opt-in via ``allow_read_only_metadata=True``. SHOW only parses to ``exp.Show``
# when a dialect that supports it is passed (e.g. snowflake, mysql); without a
# dialect sqlglot falls back to ``exp.Command``, which stays blocked.
READ_ONLY_METADATA_TYPES: tuple[type[exp.Expr], ...] = (
    exp.Describe,
    exp.Show,
)

# Denylist: expression types that mutate data or schema when found anywhere in the AST.
# This catches data-modifying CTEs (e.g. WITH del AS (DELETE …) SELECT …),
# SELECT INTO, DDL or DML wrapped behind DESCRIBE/EXPLAIN (e.g. DESCRIBE DROP TABLE …),
# and other constructs that bypass top-level type checks.
# Note: exp.Command is sqlglot's fallback for any syntax it doesn't recognize.
# Including it makes the denylist fail-closed (safer), but may block legitimate
# vendor-specific SQL that sqlglot can't parse. Callers who need such syntax can
# provide custom allowed_types to bypass the deep scan entirely.
_DATA_MODIFYING_NODES: tuple[type[exp.Expr], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Into,
    exp.Command,
    # DDL — newly reachable through the DESCRIBE/SHOW allowlist, so deny it here too.
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
)


class SQLSafetyError(Exception):
    """Generated SQL failed safety validation."""


def parse_sql(
    sql: str,
    *,
    dialect: str | None = None,
    allow_multiple_statements: bool = False,
) -> list[exp.Expr]:
    """
    Parse SQL into statements, enforcing the empty- and multi-statement guards only.

    Shared by :func:`validate_sql` (which then applies statement-type checks) and by
    callers that need the parsed AST for their own analysis -- e.g. table-reference
    extraction for ``allowed_tables`` enforcement -- without the read-only allow-list.

    :param sql: SQL string to parse.
    :param dialect: SQL dialect for parsing (``postgres``, ``mysql``, etc.).
    :param allow_multiple_statements: Whether to allow multiple semicolon-separated
        statements. Default ``False`` -- multi-statement input can hide a dangerous
        operation after a benign one.
    :return: List of parsed sqlglot Expression objects (never empty).
    :raises SQLSafetyError: If the SQL is empty, cannot be parsed, or contains multiple
        statements when not permitted.
    """
    if not sql or not sql.strip():
        raise SQLSafetyError("Empty SQL input.")

    try:
        statements = sqlglot.parse(sql, dialect=dialect, error_level=ErrorLevel.RAISE)
    except sqlglot.errors.ParseError as e:
        raise SQLSafetyError(f"SQL parse error: {e}") from e

    # sqlglot.parse can return [None] for empty input
    parsed = [s for s in statements if s is not None]
    if not parsed:
        raise SQLSafetyError("Empty SQL input.")

    if not allow_multiple_statements and len(parsed) > 1:
        raise SQLSafetyError(
            f"Multiple statements detected ({len(parsed)}). Only single statements are allowed by default."
        )
    return parsed


class TableScan(NamedTuple):
    """Result of :func:`collect_table_references`."""

    #: ``(catalog, schema, table)`` for every real base table referenced anywhere in
    #: the AST. ``catalog`` and ``schema`` are ``""`` when the reference omits them.
    #: In-scope CTE references are excluded. Catalog is reported so the caller can
    #: reject cross-database references (``otherdb.public.orders``) that a
    #: ``schema.table`` allow-list cannot describe.
    tables: list[tuple[str, str, str]]
    #: Human-readable descriptions of constructs that cannot be checked against an
    #: allow-list and so must be rejected while one is active: table-valued functions
    #: (``dblink``), ``TABLE('name')`` row sources, ``SHOW``, dynamic SQL
    #: (``EXEC``/``Command``), inline comments (a parser-vs-engine differential), and
    #: the ``TABLE <name>`` shorthand. Empty when every construct is verifiable.
    unverifiable_sources: list[str]


_DML_TYPES: tuple[type[exp.Expr], ...] = (exp.Insert, exp.Update, exp.Delete, exp.Merge)


def _same_identifier(a: exp.Identifier, b: exp.Identifier) -> bool:
    """
    Compare two identifiers under standard identifier-folding rules.

    Unquoted names fold (case-insensitive); quoted names are case-preserving and
    distinct from unquoted ones. Used to decide whether a table reference names a CTE:
    being *stricter* here is safe -- a near-miss falls through to the allow-list check.
    """
    aq, bq = bool(a.args.get("quoted")), bool(b.args.get("quoted"))
    if not aq and not bq:
        return str(a.this).casefold() == str(b.this).casefold()
    if aq and bq:
        return str(a.this) == str(b.this)
    return False


def _enclosing_cte(table: exp.Expr, with_: exp.With) -> exp.CTE | None:
    """Return the CTE of ``with_`` whose *definition* contains ``table`` (else ``None``)."""
    node = table.parent
    while node is not None and node is not with_.parent:
        if isinstance(node, exp.CTE) and node.parent is with_:
            return node
        node = node.parent
    return None


def _is_in_scope_cte(table: exp.Table) -> bool:
    """
    Report whether ``table`` is a bare reference resolved by a CTE visible at its scope.

    Walks the ancestor chain (lexical scope) collecting CTE names from each enclosing
    ``WITH``. A CTE defined in a *sibling* or *inner* subquery is not an ancestor, so a
    real top-level table is never excluded by an unrelated same-named CTE
    (``SELECT * FROM secret WHERE id IN (WITH secret AS (...) SELECT ...)``). A
    non-recursive CTE is not visible inside its own definition, so
    ``WITH secret AS (SELECT * FROM secret) ...`` still reports the real ``secret``.
    CTE order matters too: inside one CTE's body only *earlier* siblings are in scope
    (forward references need ``RECURSIVE``), so ``WITH a AS (SELECT * FROM secret),
    secret AS (...) SELECT * FROM a`` still reports the real ``secret`` read by ``a``.
    """
    ref = table.this
    if not isinstance(ref, exp.Identifier):
        return False
    node: exp.Expr | None = table.parent
    while node is not None:
        # A WITH attaches to its owning query (Select/Union/DML) as a sibling of the
        # body, so the query -- an ancestor of the table -- holds it. Find it by type
        # rather than a fixed arg key (sqlglot has used both ``with`` and ``with_``).
        with_ = (
            next((v for v in node.args.values() if isinstance(v, exp.With)), None)
            if isinstance(node, exp.Expression)
            else None
        )
        if isinstance(with_, exp.With):
            recursive = bool(with_.args.get("recursive"))
            ctes = list(with_.expressions)
            enclosing = _enclosing_cte(table, with_)
            # If the reference sits inside CTE E's own body, only CTEs defined *before*
            # E are visible there (plus E itself when RECURSIVE); a CTE defined after E
            # is not yet in scope. In the main query body every CTE is visible.
            enclosing_idx = next((i for i, c in enumerate(ctes) if c is enclosing), None)
            for idx, cte in enumerate(ctes):
                if enclosing_idx is not None:
                    if idx > enclosing_idx:
                        continue
                    if idx == enclosing_idx and not recursive:
                        continue
                alias = cte.args.get("alias")
                cte_ident = alias.this if isinstance(alias, exp.TableAlias) else None
                if isinstance(cte_ident, exp.Identifier) and _same_identifier(cte_ident, ref):
                    return True
        node = node.parent
    return False


def collect_table_references(statements: list[exp.Expr]) -> TableScan:
    """
    Walk parsed statements and report every real table they reach, scope-correctly.

    This is the AST half of ``allowed_tables`` enforcement: it returns the concrete
    base tables a query reaches (including those nested in subqueries, CTEs, JOINs, set
    operations, ``DESCRIBE``, and DML) as ``(catalog, schema, table)`` so the caller can
    check each against its allow-list, plus a list of constructs that cannot be checked
    and must therefore be rejected while an allow-list is active.

    Handled carefully (each was a confirmed bypass before it was closed):

    - **CTE references are excluded by lexical scope, not by name.** A table is treated
      as a CTE only when a ``WITH`` *enclosing that reference* defines the name (see
      :func:`_is_in_scope_cte`); a same-named CTE in a sibling/inner query no longer
      hides a real top-level table. A DML *target* is always a real table (you cannot
      write to a CTE, so a same-named CTE does not shadow it), but DML *sources* follow
      normal CTE scoping -- a CTE used as an INSERT/UPDATE source is not flagged.
    - **Catalog-qualified references are reported with their catalog**, so the caller
      rejects ``otherdb.public.orders`` instead of matching it to ``public.orders``.
    - **Unverifiable constructs are listed, not silently dropped:** nameless
      table-valued functions (``dblink``), ``TABLE('name')`` row sources
      (``exp.TableFromRows``), ``SHOW``, dynamic SQL (``EXEC``/``Command``), the
      ``TABLE <name>`` shorthand (which sqlglot parses incorrectly, leaking the
      ``TABLE`` keyword as a column), a **quoted identifier** (case-sensitive on the engine but
      matched case-insensitively here, so ``"Orders"`` could otherwise reach a table
      distinct from the allow-listed ``orders``), and **any inline comment** --
      comments are where parser-vs-engine differentials hide (MySQL executable
      ``/*! ... */``, ``--`` not followed by whitespace, ``#``).

    :param statements: Parsed sqlglot statements (from :func:`parse_sql`).
    :return: A :class:`TableScan` of real table references and unverifiable constructs.
    """
    tables: list[tuple[str, str, str]] = []
    unverifiable: list[str] = []
    for stmt in statements:
        # SHOW enumerates objects / leaks a table's columns outside any single table.
        if isinstance(stmt, exp.Show):
            unverifiable.append("a SHOW statement")
            continue
        # Dynamic SQL and anything sqlglot can only represent as a raw Command reach
        # data through text the parser cannot inspect.
        if isinstance(stmt, (exp.Command, exp.Execute)):
            unverifiable.append(f"a {type(stmt).__name__.lower()} statement")
            continue

        # A comment is a parser-vs-engine differential vector: sqlglot drops it, but the
        # engine may execute it (MySQL `/*! ... */`) or tokenize it differently (`--`
        # without a trailing space, `#`). sqlglot tokenizes string literals correctly,
        # so a `--` inside a quoted string is not flagged here.
        if any(node.comments for node in stmt.walk()):
            unverifiable.append("an inline comment")
            continue

        # `TABLE('name')` / `TABLE($$name$$)` name a table through a string the parser
        # cannot resolve; sqlglot models them as TableFromRows, not exp.Table.
        if any(True for _ in stmt.find_all(exp.TableFromRows)):
            unverifiable.append("a TABLE(...) row source")
            continue

        # `TABLE <name>` (Postgres/MySQL shorthand for SELECT * FROM <name>) is not
        # modelled by sqlglot; it parses incorrectly, leaking the reserved word TABLE as an
        # unquoted column identifier. No real query has an unquoted column named TABLE.
        if any(
            isinstance(col.this, exp.Identifier)
            and not col.this.args.get("quoted")
            and str(col.this.this).upper() == "TABLE"
            for col in stmt.find_all(exp.Column)
        ):
            unverifiable.append("a TABLE <name> shorthand")
            continue

        # A DML statement's *target* (the table written to) is always a real table --
        # you cannot INSERT/UPDATE/DELETE/MERGE into a CTE, so even a same-named CTE does
        # not shadow it. Its *sources* (the SELECT/USING/subqueries) follow normal CTE
        # scoping, so a CTE used as a source is not mistaken for a base table.
        target = stmt.args.get("this") if isinstance(stmt, _DML_TYPES) else None
        target_ids = {id(t) for t in target.find_all(exp.Table)} if target is not None else set()
        for table in stmt.find_all(exp.Table):
            name = table.name
            if not name:
                unverifiable.append(f"table-valued function ({table.sql()})")
                continue
            # A bare, non-target reference may be a CTE; a qualified one or a DML target
            # never is.
            if id(table) not in target_ids and not table.db and not table.catalog and _is_in_scope_cte(table):
                continue
            # A quoted identifier is case-sensitive on the engine, but the allow-list is
            # matched case-insensitively (and a plain ``schema.table`` string cannot
            # carry quoting), so a quoted reference cannot be matched soundly: on
            # Postgres/Snowflake ``"Orders"`` is a *different* table from the allow-listed
            # ``orders``. Reject rather than risk reaching a case-distinct table.
            if any(
                isinstance(part, exp.Identifier) and part.args.get("quoted")
                for part in (table.this, table.args.get("db"), table.args.get("catalog"))
            ):
                unverifiable.append("a quoted identifier")
                continue
            tables.append((table.catalog, table.db, name))
    return TableScan(tables=tables, unverifiable_sources=unverifiable)


def validate_sql(
    sql: str,
    *,
    allowed_types: tuple[type[exp.Expr], ...] | None = None,
    dialect: str | None = None,
    allow_multiple_statements: bool = False,
    allow_read_only_metadata: bool = False,
) -> list[exp.Expr]:
    """
    Parse SQL and verify all statements are in the allowed types list.

    By default, only a single SELECT-family statement is allowed. Multi-statement
    SQL (separated by semicolons) is rejected unless ``allow_multiple_statements=True``,
    because multi-statement inputs can hide dangerous operations after a benign SELECT.

    Returns parsed statements on success, raises :class:`SQLSafetyError` on violation.

    :param sql: SQL string to validate.
    :param allowed_types: Tuple of sqlglot expression types to permit.
        Defaults to ``(Select, Union, Intersect, Except)``. When supplied, the
        caller takes full control of the allow-list and ``allow_read_only_metadata``
        is ignored.
    :param dialect: SQL dialect for parsing (``postgres``, ``mysql``, etc.).
    :param allow_multiple_statements: Whether to allow multiple semicolon-separated
        statements. Default ``False``.
    :param allow_read_only_metadata: Also permit read-only metadata statements
        (``DESCRIBE``/``SHOW``) on top of the default read-only allow-list. Ignored
        when ``allowed_types`` is supplied. Note ``SHOW`` only parses to a metadata
        statement when a ``dialect`` that supports it is given. Default ``False``.
    :return: List of parsed sqlglot Expression objects.
    :raises SQLSafetyError: If the SQL is empty, contains disallowed statement types,
        or has multiple statements when not permitted.
    """
    # A caller-supplied ``allowed_types`` is an explicit opt-out of the curated
    # read-only defaults (and the data-modifying deep scan). Otherwise we use the
    # read-only defaults, optionally widened with metadata statements, and keep
    # the deep scan on.
    if allowed_types is None:
        types: tuple[type[exp.Expr], ...] = DEFAULT_ALLOWED_TYPES
        if allow_read_only_metadata:
            types = types + READ_ONLY_METADATA_TYPES
        run_data_modifying_scan = True
    else:
        types = allowed_types
        run_data_modifying_scan = types == DEFAULT_ALLOWED_TYPES

    parsed = parse_sql(sql, dialect=dialect, allow_multiple_statements=allow_multiple_statements)

    for stmt in parsed:
        if not isinstance(stmt, types):
            allowed_names = ", ".join(t.__name__ for t in types)
            raise SQLSafetyError(
                f"Statement type '{type(stmt).__name__}' is not allowed. Allowed types: {allowed_names}"
            )

    # Deep scan: reject data-modifying nodes hidden inside otherwise-allowed statements
    # (e.g. data-modifying CTEs, SELECT INTO, EXPLAIN <write>). Runs for the curated
    # read-only allow-list — callers who provide custom allowed_types have explicitly
    # opted into non-read-only operations.
    if run_data_modifying_scan:
        _check_for_data_modifying_nodes(parsed)

    return parsed


def _check_for_data_modifying_nodes(statements: list[exp.Expr]) -> None:
    """
    Walk the full AST of each statement and reject data-modifying expressions.

    This catches bypass vectors like:
    - Data-modifying CTEs: ``WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d``
    - SELECT INTO: ``SELECT * INTO new_table FROM t``
    - INSERT/UPDATE/DELETE hidden inside subqueries or CTEs

    :raises SQLSafetyError: If any data-modifying node is found in the AST.
    """
    for stmt in statements:
        for node in stmt.walk():
            if isinstance(node, _DATA_MODIFYING_NODES):
                raise SQLSafetyError(
                    f"Data-modifying operation '{type(node).__name__}' found inside statement. "
                    f"Only pure read operations are allowed in read-only mode."
                )
