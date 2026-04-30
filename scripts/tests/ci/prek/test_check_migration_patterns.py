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
"""Unit tests for ``scripts/ci/prek/check_migration_patterns.py``."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest
from ci.prek.check_migration_patterns import (
    MigrationFile,
    _collect_ddl_table_names,
    _extract_dml_table_name,
    _get_noqa_codes,
    _line_has_noqa,
    check_mig001,
    check_mig002,
    check_mig003,
    main,
)


def write_migration(tmp_path: Path, source: str) -> Path:
    """Write *source* to a temp file that looks like a migration file."""
    p = tmp_path / "0099_test_migration.py"
    p.write_text(source)
    return p


def parse_migration(tmp_path: Path, source: str) -> MigrationFile:
    """Write *source* to a temp file and return a parsed ``MigrationFile``."""
    return MigrationFile.from_path(write_migration(tmp_path, source))


class TestGetNoqaCodes:
    def test_single_code(self):
        assert _get_noqa_codes("op.execute(...)  # noqa: MIG001") == {"MIG001"}

    def test_multiple_codes(self):
        assert _get_noqa_codes("op.execute(...)  # noqa: MIG001, MIG003") == {"MIG001", "MIG003"}

    def test_code_with_reason_suffix(self):
        # Reason after '--' should NOT be included in the code set.
        codes = _get_noqa_codes("op.execute(...)  # noqa: MIG003 -- safe in offline mode")
        assert "MIG003" in codes
        assert "--" not in "".join(codes)
        assert "safe" not in "".join(codes)

    def test_no_noqa_comment(self):
        assert _get_noqa_codes("op.execute('UPDATE foo SET x=1')") == set()

    def test_empty_string(self):
        assert _get_noqa_codes("") == set()


class TestLineHasNoqa:
    def test_line_has_matching_code(self):
        lines = ["op.execute('UPDATE ...')  # noqa: MIG001"]
        assert _line_has_noqa(lines, 1, "MIG001") is True

    def test_line_has_different_code(self):
        lines = ["op.execute('UPDATE ...')  # noqa: MIG002"]
        assert _line_has_noqa(lines, 1, "MIG001") is False

    @pytest.mark.parametrize("lineno", [0, 2], ids=["zero", "beyond_end"])
    def test_out_of_bounds(self, lineno):
        assert _line_has_noqa(["some line"], lineno, "MIG001") is False


class TestCheckMig001:
    def test_no_violation_no_guard(self, tmp_path):
        """MIG001 only fires when disable_sqlite_fkeys IS present; no guard → no error."""
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")
"""
        assert check_mig001(parse_migration(tmp_path, src)) == []

    def test_no_violation_dml_inside_guard(self, tmp_path):
        """DML inside the guard block is fine."""
        src = """
def upgrade():
    with disable_sqlite_fkeys(op):
        op.execute("UPDATE dag SET x=1")
"""
        assert check_mig001(parse_migration(tmp_path, src)) == []

    def test_violation_dml_before_guard(self, tmp_path):
        """DML before disable_sqlite_fkeys fires MIG001."""
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig001(parse_migration(tmp_path, src))
        assert len(errors) == 1
        assert "MIG001" in errors[0]

    @pytest.mark.parametrize(
        "dml",
        [
            "INSERT INTO dag VALUES (1)",
            "DELETE FROM dag WHERE id=1",
        ],
        ids=["insert", "delete"],
    )
    def test_violation_dml_keyword(self, tmp_path, dml):
        src = f"""
def upgrade():
    op.execute("{dml}")
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig001(parse_migration(tmp_path, src))
        assert any("MIG001" in e for e in errors)

    def test_violation_text_wrapped(self, tmp_path):
        """op.execute(text("UPDATE ...")) also fires."""
        src = """
def upgrade():
    op.execute(text("UPDATE dag SET x=1"))
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig001(parse_migration(tmp_path, src))
        assert any("MIG001" in e for e in errors)

    def test_noqa_suppresses_mig001(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")  # noqa: MIG001
    with disable_sqlite_fkeys(op):
        pass
"""
        assert check_mig001(parse_migration(tmp_path, src)) == []

    def test_noqa_wrong_code_does_not_suppress(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")  # noqa: MIG002
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig001(parse_migration(tmp_path, src))
        assert any("MIG001" in e for e in errors)

    def test_violation_in_downgrade(self, tmp_path):
        src = """
def downgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig001(parse_migration(tmp_path, src))
        assert any("MIG001" in e for e in errors)


class TestCheckMig002:
    def test_no_violation_ddl_inside_guard(self, tmp_path):
        src = """
def upgrade():
    with disable_sqlite_fkeys(op):
        op.add_column("dag", sa.Column("x", sa.Integer()))
"""
        assert check_mig002(parse_migration(tmp_path, src)) == []

    def test_violation_ddl_before_guard(self, tmp_path):
        src = """
def upgrade():
    op.add_column("dag", sa.Column("x", sa.Integer()))
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig002(parse_migration(tmp_path, src))
        assert any("MIG002" in e for e in errors)

    def test_dml_before_guard_does_not_trigger_mig002(self, tmp_path):
        """DML is MIG001's territory; MIG002 must not double-report it."""
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig002(parse_migration(tmp_path, src))
        assert errors == []

    def test_noqa_suppresses_mig002(self, tmp_path):
        src = """
def upgrade():
    op.add_column("dag", sa.Column("x", sa.Integer()))  # noqa: MIG002
    with disable_sqlite_fkeys(op):
        pass
"""
        assert check_mig002(parse_migration(tmp_path, src)) == []

    def test_violation_batch_alter_table(self, tmp_path):
        src = """
def upgrade():
    op.batch_alter_table("dag")
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig002(parse_migration(tmp_path, src))
        assert any("MIG002" in e for e in errors)


class TestCheckMig003:
    def test_no_violation_no_dml(self, tmp_path):
        src = """
def upgrade():
    op.add_column("dag", sa.Column("x", sa.Integer()))
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_no_violation_offline_guard_present(self, tmp_path):
        src = """
def upgrade():
    if not context.is_offline_mode():
        op.execute("UPDATE dag SET x=1")
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_violation_dml_without_guard(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")
"""
        errors = check_mig003(parse_migration(tmp_path, src))
        assert any("MIG003" in e for e in errors)

    def test_noqa_suppresses_mig003(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")  # noqa: MIG003
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_violation_in_downgrade(self, tmp_path):
        src = """
def downgrade():
    op.execute("DELETE FROM dag WHERE id=1")
"""
        errors = check_mig003(parse_migration(tmp_path, src))
        assert any("MIG003" in e for e in errors)


def _parse_call(code: str) -> ast.Call:
    """Parse a single expression and return the Call node."""
    node = ast.parse(code, mode="eval").body
    assert isinstance(node, ast.Call)
    return node


def _parse_func(code: str) -> ast.FunctionDef:
    """Parse code and return the first FunctionDef."""
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            return node
    raise ValueError("No function found")


class TestExtractDmlTableName:
    @pytest.mark.parametrize(
        "sql, expected",
        [
            ('op.execute("UPDATE dag SET x=1")', "dag"),
            ('op.execute("DELETE FROM dag_code")', "dag_code"),
            ('op.execute("INSERT INTO new_table SELECT * FROM old")', "new_table"),
            ('op.execute("INSERT IGNORE INTO dag_bundle (name) VALUES (1)")', "dag_bundle"),
            ('op.execute("INSERT OR IGNORE INTO dag_bundle (name) VALUES (1)")', "dag_bundle"),
            ("op.execute(\"update dag_run set run_type = 'x'\")", "dag_run"),
            ('op.execute(text("UPDATE slot_pool SET x=1"))', "slot_pool"),
            ('op.execute(sa.text("UPDATE slot_pool SET x=1"))', "slot_pool"),
            ('op.execute("UPDATE `dag` SET x=1")', "dag"),
        ],
        ids=[
            "update",
            "delete_from",
            "insert_into",
            "insert_ignore",
            "insert_or_ignore",
            "lowercase_sql",
            "text_wrapped",
            "sa_text_wrapped",
            "backtick_quoted",
        ],
    )
    def test_extracts_table(self, sql, expected):
        assert _extract_dml_table_name(_parse_call(sql)) == expected

    @pytest.mark.parametrize(
        "sql",
        [
            "op.execute()",
            'op.execute("SELECT * FROM dag")',
            "op.execute(some_var)",
        ],
        ids=["no_args", "non_dml", "variable_arg"],
    )
    def test_returns_none(self, sql):
        assert _extract_dml_table_name(_parse_call(sql)) is None


class TestCollectDdlTableNames:
    def test_batch_alter_table(self):
        func = _parse_func('def upgrade():\n    with op.batch_alter_table("dag") as b: pass')
        assert _collect_ddl_table_names(func) == {"dag"}

    def test_rename_table_collects_both(self):
        func = _parse_func('def upgrade():\n    op.rename_table("old", "new")')
        tables = _collect_ddl_table_names(func)
        assert tables == {"old", "new"}

    def test_add_column(self):
        func = _parse_func('def upgrade():\n    op.add_column("dag_run", col)')
        assert "dag_run" in _collect_ddl_table_names(func)

    def test_variable_arg_skipped(self):
        func = _parse_func("def upgrade():\n    op.batch_alter_table(table_name)")
        assert _collect_ddl_table_names(func) == set()

    def test_multiple_tables(self):
        src = (
            "def upgrade():\n"
            '    with op.batch_alter_table("dag") as b: pass\n'
            '    with op.batch_alter_table("task_instance") as b: pass\n'
        )
        func = _parse_func(src)
        assert _collect_ddl_table_names(func) == {"dag", "task_instance"}


class TestCheckMig003SameTableDdl:
    def test_no_violation_dml_with_same_table_ddl(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x='' WHERE x IS NULL")
    with op.batch_alter_table("dag") as batch_op:
        batch_op.alter_column("x", nullable=False)
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_violation_dml_different_table_than_ddl(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag_run SET run_type='x'")
    with op.batch_alter_table("dag") as batch_op:
        batch_op.alter_column("x", nullable=False)
"""
        errors = check_mig003(parse_migration(tmp_path, src))
        assert any("MIG003" in e for e in errors)

    def test_no_violation_delete_before_rename_table(self, tmp_path):
        src = """
def downgrade():
    op.execute("DELETE FROM callback_request")
    op.rename_table("callback_request", "callback")
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_no_violation_insert_into_created_table(self, tmp_path):
        src = """
def upgrade():
    op.create_table("new_table", sa.Column("id", sa.Integer()))
    op.execute("INSERT INTO new_table SELECT * FROM old_table")
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_no_violation_multiple_tables(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE connection SET x=1 WHERE x IS NULL")
    op.execute("UPDATE dag SET y=0 WHERE y IS NULL")
    with op.batch_alter_table("connection") as b:
        b.alter_column("x", nullable=False)
    with op.batch_alter_table("dag") as b:
        b.alter_column("y", nullable=False)
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_violation_standalone_dml_no_ddl(self, tmp_path):
        src = """
def upgrade():
    op.execute("update dag_run set run_type = 'asset_triggered'")
"""
        errors = check_mig003(parse_migration(tmp_path, src))
        assert any("MIG003" in e for e in errors)

    def test_no_violation_dml_with_add_column(self, tmp_path):
        src = """
def upgrade():
    op.add_column("dag_run", sa.Column("run_after", sa.DateTime))
    op.execute("update dag_run set run_after = logical_date")
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_no_violation_text_wrapped_dml(self, tmp_path):
        src = """
def upgrade():
    op.execute(text("UPDATE slot_pool SET x = 0"))
    with op.batch_alter_table("slot_pool") as b:
        b.alter_column("x", nullable=False)
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []

    def test_offline_guard_still_takes_precedence(self, tmp_path):
        src = """
def upgrade():
    if not context.is_offline_mode():
        op.execute("UPDATE dag SET x=1")
"""
        assert check_mig003(parse_migration(tmp_path, src)) == []


class TestMain:
    def test_no_args_exits_zero(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["check_migration_patterns.py"])
        assert main() == 0

    def test_clean_file_exits_zero(self, tmp_path, monkeypatch):
        p = write_migration(tmp_path, "def upgrade(): pass\n")
        monkeypatch.setattr(sys, "argv", ["check_migration_patterns.py", str(p)])
        assert main() == 0

    def test_missing_file_exits_one(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["check_migration_patterns.py", str(tmp_path / "missing.py")])
        assert main() == 1

    def test_syntax_error_exits_one(self, tmp_path, monkeypatch):
        p = tmp_path / "bad.py"
        p.write_text("def upgrade(: pass\n")
        monkeypatch.setattr(sys, "argv", ["check_migration_patterns.py", str(p)])
        assert main() == 1

    def test_violation_exits_one(self, tmp_path, monkeypatch):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
"""
        p = write_migration(tmp_path, src)
        monkeypatch.setattr(sys, "argv", ["check_migration_patterns.py", str(p)])
        assert main() == 1

    def test_any_violation_in_batch_exits_one(self, tmp_path, monkeypatch):
        clean = tmp_path / "0001_clean.py"
        clean.write_text("def upgrade(): pass\n")
        violation = tmp_path / "0002_bad.py"
        violation.write_text(
            "def upgrade():\n    op.execute('UPDATE dag SET x=1')\n    with disable_sqlite_fkeys(op): pass\n"
        )
        monkeypatch.setattr(sys, "argv", ["check_migration_patterns.py", str(clean), str(violation)])
        assert main() == 1
