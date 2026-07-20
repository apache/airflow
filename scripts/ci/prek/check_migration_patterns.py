#!/usr/bin/env python
#
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Static analysis checks for Alembic migration anti-patterns.

These checks use AST analysis to detect common migration mistakes that can cause
silent failures, particularly on SQLite where ``PRAGMA foreign_keys`` has specific
requirements about transaction state.

Rules
=====

MIG001 -- DML before ``disable_sqlite_fkeys``
----------------------------------------------

**What it does:**
Detects ``op.execute()`` calls containing DML keywords (UPDATE, INSERT, DELETE) that
appear before a ``with disable_sqlite_fkeys(op):`` block in the same function.

**Why is this bad:**
On SQLite, any DML statement triggers *autobegin*, starting a transaction.  Once a
transaction is active, ``PRAGMA foreign_keys=off`` (issued by ``disable_sqlite_fkeys``)
becomes a no-op.  This means foreign key checks remain enabled, and
``batch_alter_table`` (which recreates tables) may fail with foreign key constraint
violations.

**Example (bad):**

.. code-block:: python

    def upgrade():
        op.execute("UPDATE dag SET col = '' WHERE col IS NULL")  # triggers autobegin
        with disable_sqlite_fkeys(op):  # PRAGMA is now a no-op!
            with op.batch_alter_table("dag") as batch_op:
                batch_op.alter_column("col", nullable=False)

**Use instead:**

.. code-block:: python

    def upgrade():
        with disable_sqlite_fkeys(op):
            op.execute("UPDATE dag SET col = '' WHERE col IS NULL")
            with op.batch_alter_table("dag") as batch_op:
                batch_op.alter_column("col", nullable=False)


MIG002 -- DDL before ``disable_sqlite_fkeys``
----------------------------------------------

**What it does:**
Detects any Alembic ``op.*`` call (DDL operations like ``op.add_column``,
``op.drop_column``, ``op.create_table``, ``op.drop_table``, ``op.create_index``,
``op.batch_alter_table``, etc.) that appears before a
``with disable_sqlite_fkeys(op):`` block in the same function.  Excludes DML calls
covered by MIG001.

**Why is this bad:**
DDL operations also trigger *autobegin* on SQLite, making the subsequent
``PRAGMA foreign_keys=off`` a no-op, exactly like DML.

**Example (bad):**

.. code-block:: python

    def upgrade():
        op.add_column("dag_run", sa.Column("created_at", ...))  # triggers autobegin
        with disable_sqlite_fkeys(op):  # PRAGMA is now a no-op!
            with op.batch_alter_table("backfill") as batch_op:
                batch_op.alter_column("col", nullable=True)

**Use instead:**

.. code-block:: python

    def upgrade():
        with disable_sqlite_fkeys(op):
            op.add_column("dag_run", sa.Column("created_at", ...))
            with op.batch_alter_table("backfill") as batch_op:
                batch_op.alter_column("col", nullable=True)


MIG003 -- DML without offline-mode guard
------------------------------------------

**What it does:**
Detects ``op.execute()`` calls containing DML keywords (UPDATE, INSERT, DELETE) in
``upgrade()`` or ``downgrade()`` functions that do not contain a
``context.is_offline_mode()`` check anywhere in the same function.

**Why is this bad:**
Alembic's offline mode generates SQL scripts instead of executing against a live
database.  DML statements like ``UPDATE ... WHERE col IS NULL`` depend on actual data
and produce no useful output in offline mode.  Without an ``is_offline_mode()`` guard,
the generated script includes DML that may fail or silently do nothing when applied
later.

**Example (bad):**

.. code-block:: python

    def upgrade():
        op.execute("UPDATE dag SET col = '' WHERE col IS NULL")
        with op.batch_alter_table("dag") as batch_op:
            batch_op.alter_column("col", nullable=False)

**Use instead:**

.. code-block:: python

    def upgrade():
        if not context.is_offline_mode():
            op.execute("UPDATE dag SET col = '' WHERE col IS NULL")
        with op.batch_alter_table("dag") as batch_op:
            batch_op.alter_column("col", nullable=False)


**Exception -- DML tied to DDL on the same table:**

When an ``op.execute()`` DML call targets a table that also has DDL operations
(``batch_alter_table``, ``create_table``, ``drop_table``, ``rename_table``,
``add_column``, ``drop_column``, ``alter_column``, ``create_index``, ``drop_index``,
``drop_constraint``) in the same ``upgrade()`` or ``downgrade()`` function, MIG003
is not raised.  This covers the common pattern of filling NULL values before adding
a NOT NULL constraint, deleting rows before dropping a table, etc.  The DML is
considered a safe prerequisite for the DDL.


Known Limitation -- Future MIG004 Candidate
---------------------------------------------

Migrations that perform DML and use ``batch_alter_table`` but never call
``disable_sqlite_fkeys`` at all are NOT detected by MIG001/MIG002.  These migrations
may silently break SQLite foreign key handling.  Detecting this pattern requires
understanding whether foreign key relationships exist on the affected tables, which
is beyond AST-level analysis.  This is tracked as a future MIG004 candidate.


Suppression
===========

Add ``# noqa: MIG0XX`` to a line to suppress the corresponding check for that line.
Multiple codes can be comma-separated: ``# noqa: MIG001, MIG003``.

Include a brief reason after ``--``::

    # noqa: MIG003 -- simple UPDATE safe in offline mode
"""

from __future__ import annotations

import ast
import dataclasses
import re
import sys
from pathlib import Path

from rich.console import Console

console = Console(color_system="standard", width=200)

DML_KEYWORDS = ("UPDATE", "INSERT", "DELETE")

_DML_TABLE_RE = re.compile(
    r"^\s*(?:"
    r"(?:UPDATE)\s+[`\"]?(\w+)[`\"]?"
    r"|(?:DELETE\s+FROM)\s+[`\"]?(\w+)[`\"]?"
    r"|(?:INSERT\s+(?:(?:OR\s+)?IGNORE\s+)?INTO)\s+[`\"]?(\w+)[`\"]?"
    r")",
    re.IGNORECASE,
)


@dataclasses.dataclass(frozen=True)
class MigrationFile:
    path: Path
    source: str
    lines: list[str]
    tree: ast.Module

    @classmethod
    def from_path(cls, filepath: Path) -> MigrationFile:
        source = filepath.read_text()
        return cls(
            path=filepath,
            source=source,
            lines=source.splitlines(),
            tree=ast.parse(source, filename=str(filepath)),
        )


def _get_noqa_codes(line: str) -> set[str]:
    """Extract noqa codes from a source line."""
    match = re.search(r"#\s*noqa:\s*([\w,\s]+)", line)
    if match:
        return {code.strip() for code in match.group(1).split(",")}
    return set()


def _is_dml_string(node: ast.expr) -> bool:
    """Check if an AST expression is a string literal starting with a DML keyword."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        parts = node.value.strip().upper().split(None, 1)
        return len(parts) > 1 and parts[0] in DML_KEYWORDS
    # f-strings: check the first string fragment
    if isinstance(node, ast.JoinedStr):
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts = value.value.strip().upper().split(None, 1)
                if len(parts) > 1 and parts[0] in DML_KEYWORDS:
                    return True
                break  # only the leading fragment matters
    return False


def _get_sql_string_value(node: ast.expr) -> str | None:
    """Extract the leading SQL string from an AST expression."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value
            break
    return None


def _extract_dml_table_name(node: ast.Call) -> str | None:
    """Extract the target table name from a DML ``op.execute()`` call.

    Returns the lowercase table name, or None if extraction fails.
    """
    if not node.args:
        return None
    arg = node.args[0]
    if isinstance(arg, ast.Call) and arg.args:
        arg = arg.args[0]
    sql = _get_sql_string_value(arg)
    if sql is None:
        return None
    m = _DML_TABLE_RE.match(sql)
    if m is None:
        return None
    table = m.group(1) or m.group(2) or m.group(3)
    return table.lower() if table else None


_DDL_TABLE_FIRST_ARG = frozenset(
    {
        "batch_alter_table",
        "drop_table",
        "create_table",
        "add_column",
        "drop_column",
        "alter_column",
    }
)


def _collect_ddl_table_names(func_node: ast.FunctionDef) -> set[str]:
    """Collect all table names referenced by DDL operations in a function."""
    tables: set[str] = set()

    def _str_arg(call: ast.Call, index: int) -> str | None:
        if index < len(call.args):
            a = call.args[index]
            if isinstance(a, ast.Constant) and isinstance(a.value, str):
                return a.value.lower()
        return None

    for node in ast.walk(func_node):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "op"
        ):
            continue
        attr = node.func.attr
        if attr in _DDL_TABLE_FIRST_ARG:
            name = _str_arg(node, 0)
            if name:
                tables.add(name)
        elif attr == "rename_table":
            for i in range(2):
                name = _str_arg(node, i)
                if name:
                    tables.add(name)
        elif attr in ("create_index", "drop_constraint"):
            name = _str_arg(node, 1)
            if name:
                tables.add(name)
        elif attr == "drop_index":
            name = _str_arg(node, 1)
            if name is None:
                for kw in node.keywords:
                    if kw.arg == "table_name":
                        if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                            name = kw.value.value.lower()
                        break
            if name:
                tables.add(name)

    return tables


def _is_op_execute_with_dml(node: ast.Call) -> bool:
    """Check if a Call node is ``op.execute(<DML string>)``."""
    if not (
        isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "op"
        and node.func.attr == "execute"
    ):
        return False
    if not node.args:
        return False
    arg = node.args[0]
    # Direct string: op.execute("UPDATE ...")
    if _is_dml_string(arg):
        return True
    # Wrapped: op.execute(text("UPDATE ...")) or op.execute(sa.text("UPDATE ..."))
    if isinstance(arg, ast.Call) and arg.args:
        return _is_dml_string(arg.args[0])
    return False


def _is_op_call(node: ast.Call) -> bool:
    """Check if a Call node is ``op.<something>(...)``."""
    return (
        isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "op"
    )


def _find_disable_sqlite_fkeys_line(func_node: ast.FunctionDef) -> int | None:
    """Find the line number of the first ``with disable_sqlite_fkeys(op):`` in a function."""
    # Collect all matches then take min() — ast.walk BFS order does not guarantee
    # line-number order when guards appear at different nesting depths.
    found: list[int] = []
    for node in ast.walk(func_node):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            ctx = item.context_expr
            if isinstance(ctx, ast.Call):
                func = ctx.func
                if (isinstance(func, ast.Name) and func.id == "disable_sqlite_fkeys") or (
                    isinstance(func, ast.Attribute) and func.attr == "disable_sqlite_fkeys"
                ):
                    found.append(node.lineno)
    return min(found) if found else None


def _has_offline_mode_check(func_node: ast.FunctionDef) -> bool:
    """Check if function contains ``context.is_offline_mode()`` call."""
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "is_offline_mode":
                return True
    return False


def _get_upgrade_downgrade_functions(tree: ast.Module) -> list[ast.FunctionDef]:
    """Get ``upgrade()`` and ``downgrade()`` function definitions from module."""
    return [
        node
        for node in ast.iter_child_nodes(tree)
        if isinstance(node, ast.FunctionDef) and node.name in ("upgrade", "downgrade")
    ]


def _line_has_noqa(lines: list[str], lineno: int, code: str) -> bool:
    """Check if a source line has a noqa suppression for the given code."""
    if lineno < 1 or lineno > len(lines):
        return False
    return code in _get_noqa_codes(lines[lineno - 1])


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------


def check_mig001(mf: MigrationFile) -> list[str]:
    """MIG001: DML before ``disable_sqlite_fkeys``."""
    errors: list[str] = []

    for func in _get_upgrade_downgrade_functions(mf.tree):
        guard_line = _find_disable_sqlite_fkeys_line(func)
        if guard_line is None:
            continue  # MIG001 only fires when disable_sqlite_fkeys IS present

        for node in ast.walk(func):
            if not isinstance(node, ast.Call):
                continue
            if node.lineno >= guard_line:
                continue
            if _is_op_execute_with_dml(node) and not _line_has_noqa(mf.lines, node.lineno, "MIG001"):
                errors.append(
                    f"MIG001 {func.name}(): op.execute() with DML at line {node.lineno} "
                    f"before disable_sqlite_fkeys at line {guard_line}"
                )

    return errors


def check_mig002(mf: MigrationFile) -> list[str]:
    """MIG002: DDL before ``disable_sqlite_fkeys``."""
    errors: list[str] = []

    for func in _get_upgrade_downgrade_functions(mf.tree):
        guard_line = _find_disable_sqlite_fkeys_line(func)
        if guard_line is None:
            continue  # MIG002 only fires when disable_sqlite_fkeys IS present

        for node in ast.walk(func):
            if not isinstance(node, ast.Call):
                continue
            if node.lineno >= guard_line:
                continue
            # Skip DML calls — covered by MIG001
            if _is_op_execute_with_dml(node):
                continue
            if _is_op_call(node) and not _line_has_noqa(mf.lines, node.lineno, "MIG002"):
                attr = node.func.attr if isinstance(node.func, ast.Attribute) else "?"
                errors.append(
                    f"MIG002 {func.name}(): op.{attr}() at line {node.lineno} "
                    f"before disable_sqlite_fkeys at line {guard_line}"
                )

    return errors


def check_mig003(mf: MigrationFile) -> list[str]:
    """MIG003: DML without offline-mode guard."""
    errors: list[str] = []

    for func in _get_upgrade_downgrade_functions(mf.tree):
        if _has_offline_mode_check(func):
            continue  # function has the guard

        ddl_tables = _collect_ddl_table_names(func)

        for node in ast.walk(func):
            if not isinstance(node, ast.Call):
                continue
            if not _is_op_execute_with_dml(node):
                continue
            dml_table = _extract_dml_table_name(node)
            if dml_table is not None and dml_table in ddl_tables:
                continue
            if not _line_has_noqa(mf.lines, node.lineno, "MIG003"):
                errors.append(
                    f"MIG003 {func.name}(): op.execute() with DML at line {node.lineno} "
                    f"without context.is_offline_mode() guard"
                )

    return errors


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Check migration files for anti-patterns. Exit 0 if clean, 1 if violations found."""
    check_fns = [check_mig001, check_mig002, check_mig003]
    has_errors = False

    for filepath_str in sys.argv[1:]:
        filepath = Path(filepath_str)
        try:
            mf = MigrationFile.from_path(filepath)
            for check_fn in check_fns:
                for error in check_fn(mf):
                    console.print(f"{filepath}: {error}")
                    has_errors = True
        except (OSError, SyntaxError, UnicodeDecodeError) as exc:
            console.print(f"{filepath}: {exc}")
            has_errors = True

    return 1 if has_errors else 0


if __name__ == "__main__":
    sys.exit(main())
