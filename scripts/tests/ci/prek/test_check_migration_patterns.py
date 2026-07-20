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
    p = tmp_path / "0099_test_migration.py"
    p.write_text(source)
    return p


def parse_migration(tmp_path: Path, source: str) -> MigrationFile:
    return MigrationFile.from_path(write_migration(tmp_path, source))


class TestGetNoqaCodes:
    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            pytest.param(
                "op.execute(...)  # noqa: MIG001",
                {"MIG001"},
                id="single-code",
            ),
            pytest.param(
                "op.execute(...)  # noqa: MIG001, MIG003",
                {"MIG001", "MIG003"},
                id="multiple-codes",
            ),
            pytest.param(
                "op.execute('UPDATE foo SET x=1')",
                set(),
                id="no-noqa-comment",
            ),
            pytest.param(
                "",
                set(),
                id="empty-string",
            ),
        ],
    )
    def test_get_noqa_codes(self, line: str, expected: set[str]) -> None:
        assert _get_noqa_codes(line) == expected

    def test_code_with_reason_suffix(self) -> None:
        codes = _get_noqa_codes("op.execute(...)  # noqa: MIG003 -- safe in offline mode")
        assert "MIG003" in codes
        assert "--" not in "".join(codes)
        assert "safe" not in "".join(codes)


@pytest.mark.parametrize(
    ("lines", "lineno", "code", "expected"),
    [
        pytest.param(
            ["op.execute('UPDATE ...')  # noqa: MIG001"],
            1,
            "MIG001",
            True,
            id="matching-code",
        ),
        pytest.param(
            ["op.execute('UPDATE ...')  # noqa: MIG002"],
            1,
            "MIG001",
            False,
            id="different-code",
        ),
        pytest.param(["some line"], 0, "MIG001", False, id="out-of-bounds-zero"),
        pytest.param(["some line"], 2, "MIG001", False, id="out-of-bounds-beyond-end"),
    ],
)
def test_line_has_noqa(lines: list[str], lineno: int, code: str, expected: bool) -> None:
    assert _line_has_noqa(lines, lineno, code) is expected


class TestCheckMig001:
    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x=1")
""",
                id="no-guard-no-error",
            ),
            pytest.param(
                """
def upgrade():
    with disable_sqlite_fkeys(op):
        op.execute("UPDATE dag SET x=1")
""",
                id="dml-inside-guard",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x=1")  # noqa: MIG001
    with disable_sqlite_fkeys(op):
        pass
""",
                id="noqa-suppressed",
            ),
        ],
    )
    def test_no_violation(self, tmp_path, src):
        assert check_mig001(parse_migration(tmp_path, src)) == []

    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
""",
                id="dml-before-guard",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("INSERT INTO dag VALUES (1)")
    with disable_sqlite_fkeys(op):
        pass
""",
                id="insert-before-guard",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("DELETE FROM dag WHERE id=1")
    with disable_sqlite_fkeys(op):
        pass
""",
                id="delete-before-guard",
            ),
            pytest.param(
                """
def upgrade():
    op.execute(text("UPDATE dag SET x=1"))
    with disable_sqlite_fkeys(op):
        pass
""",
                id="text-wrapped",
            ),
            pytest.param(
                """
def downgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
""",
                id="in-downgrade",
            ),
        ],
    )
    def test_violation(self, tmp_path, src):
        errors = check_mig001(parse_migration(tmp_path, src))
        assert any("MIG001" in e for e in errors)

    def test_noqa_wrong_code_does_not_suppress(self, tmp_path):
        src = """
def upgrade():
    op.execute("UPDATE dag SET x=1")  # noqa: MIG002
    with disable_sqlite_fkeys(op):
        pass
"""
        errors = check_mig001(parse_migration(tmp_path, src))
        assert any("MIG001" in e for e in errors)


class TestCheckMig002:
    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    with disable_sqlite_fkeys(op):
        op.add_column("dag", sa.Column("x", sa.Integer()))
""",
                id="ddl-inside-guard",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x=1")
    with disable_sqlite_fkeys(op):
        pass
""",
                id="dml-not-double-reported",
            ),
            pytest.param(
                """
def upgrade():
    op.add_column("dag", sa.Column("x", sa.Integer()))  # noqa: MIG002
    with disable_sqlite_fkeys(op):
        pass
""",
                id="noqa-suppressed",
            ),
        ],
    )
    def test_no_violation(self, tmp_path, src):
        assert check_mig002(parse_migration(tmp_path, src)) == []

    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.add_column("dag", sa.Column("x", sa.Integer()))
    with disable_sqlite_fkeys(op):
        pass
""",
                id="add-column-before-guard",
            ),
            pytest.param(
                """
def upgrade():
    op.batch_alter_table("dag")
    with disable_sqlite_fkeys(op):
        pass
""",
                id="batch-alter-table-before-guard",
            ),
        ],
    )
    def test_violation(self, tmp_path, src):
        errors = check_mig002(parse_migration(tmp_path, src))
        assert any("MIG002" in e for e in errors)


class TestCheckMig003:
    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.add_column("dag", sa.Column("x", sa.Integer()))
""",
                id="no-dml",
            ),
            pytest.param(
                """
def upgrade():
    if not context.is_offline_mode():
        op.execute("UPDATE dag SET x=1")
""",
                id="offline-guard-present",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x=1")  # noqa: MIG003
""",
                id="noqa-suppressed",
            ),
        ],
    )
    def test_no_violation(self, tmp_path, src):
        assert check_mig003(parse_migration(tmp_path, src)) == []

    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x=1")
""",
                id="dml-without-guard",
            ),
            pytest.param(
                """
def downgrade():
    op.execute("DELETE FROM dag WHERE id=1")
""",
                id="in-downgrade",
            ),
        ],
    )
    def test_violation(self, tmp_path, src):
        errors = check_mig003(parse_migration(tmp_path, src))
        assert any("MIG003" in e for e in errors)


def _parse_call(code: str) -> ast.Call:
    node = ast.parse(code, mode="eval").body
    assert isinstance(node, ast.Call)
    return node


def _parse_func(code: str) -> ast.FunctionDef:
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            return node
    raise ValueError("No function found")


class TestExtractDmlTableName:
    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            pytest.param('op.execute("UPDATE dag SET x=1")', "dag", id="update"),
            pytest.param('op.execute("DELETE FROM dag_code")', "dag_code", id="delete-from"),
            pytest.param(
                'op.execute("INSERT INTO new_table SELECT * FROM old")', "new_table", id="insert-into"
            ),
            pytest.param(
                'op.execute("INSERT IGNORE INTO dag_bundle (name) VALUES (1)")',
                "dag_bundle",
                id="insert-ignore",
            ),
            pytest.param(
                'op.execute("INSERT OR IGNORE INTO dag_bundle (name) VALUES (1)")',
                "dag_bundle",
                id="insert-or-ignore",
            ),
            pytest.param("op.execute(\"update dag_run set run_type = 'x'\")", "dag_run", id="lowercase-sql"),
            pytest.param('op.execute(text("UPDATE slot_pool SET x=1"))', "slot_pool", id="text-wrapped"),
            pytest.param(
                'op.execute(sa.text("UPDATE slot_pool SET x=1"))', "slot_pool", id="sa-text-wrapped"
            ),
            pytest.param('op.execute("UPDATE `dag` SET x=1")', "dag", id="backtick-quoted"),
        ],
    )
    def test_extracts_table(self, sql, expected):
        assert _extract_dml_table_name(_parse_call(sql)) == expected

    @pytest.mark.parametrize(
        "sql",
        [
            pytest.param("op.execute()", id="no-args"),
            pytest.param('op.execute("SELECT * FROM dag")', id="non-dml"),
            pytest.param("op.execute(some_var)", id="variable-arg"),
        ],
    )
    def test_returns_none(self, sql):
        assert _extract_dml_table_name(_parse_call(sql)) is None


class TestCollectDdlTableNames:
    @pytest.mark.parametrize(
        ("src", "expected"),
        [
            pytest.param(
                'def upgrade():\n    with op.batch_alter_table("dag") as b: pass',
                {"dag"},
                id="batch-alter-table",
            ),
            pytest.param(
                'def upgrade():\n    op.rename_table("old", "new")',
                {"old", "new"},
                id="rename-table-both",
            ),
            pytest.param(
                'def upgrade():\n    op.add_column("dag_run", col)',
                {"dag_run"},
                id="add-column",
            ),
            pytest.param(
                "def upgrade():\n    op.batch_alter_table(table_name)",
                set(),
                id="variable-arg-skipped",
            ),
            pytest.param(
                "def upgrade():\n"
                '    with op.batch_alter_table("dag") as b: pass\n'
                '    with op.batch_alter_table("task_instance") as b: pass\n',
                {"dag", "task_instance"},
                id="multiple-tables",
            ),
        ],
    )
    def test_collect(self, src, expected):
        func = _parse_func(src)
        assert _collect_ddl_table_names(func) == expected


class TestCheckMig003SameTableDdl:
    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag SET x='' WHERE x IS NULL")
    with op.batch_alter_table("dag") as batch_op:
        batch_op.alter_column("x", nullable=False)
""",
                id="same-table-batch-alter",
            ),
            pytest.param(
                """
def downgrade():
    op.execute("DELETE FROM callback_request")
    op.rename_table("callback_request", "callback")
""",
                id="delete-before-rename",
            ),
            pytest.param(
                """
def upgrade():
    op.create_table("new_table", sa.Column("id", sa.Integer()))
    op.execute("INSERT INTO new_table SELECT * FROM old_table")
""",
                id="insert-into-created-table",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE connection SET x=1 WHERE x IS NULL")
    op.execute("UPDATE dag SET y=0 WHERE y IS NULL")
    with op.batch_alter_table("connection") as b:
        b.alter_column("x", nullable=False)
    with op.batch_alter_table("dag") as b:
        b.alter_column("y", nullable=False)
""",
                id="multiple-tables",
            ),
            pytest.param(
                """
def upgrade():
    op.add_column("dag_run", sa.Column("run_after", sa.DateTime))
    op.execute("update dag_run set run_after = logical_date")
""",
                id="dml-with-add-column",
            ),
            pytest.param(
                """
def upgrade():
    op.execute(text("UPDATE slot_pool SET x = 0"))
    with op.batch_alter_table("slot_pool") as b:
        b.alter_column("x", nullable=False)
""",
                id="text-wrapped-dml",
            ),
            pytest.param(
                """
def upgrade():
    if not context.is_offline_mode():
        op.execute("UPDATE dag SET x=1")
""",
                id="offline-guard-takes-precedence",
            ),
        ],
    )
    def test_no_violation(self, tmp_path, src):
        assert check_mig003(parse_migration(tmp_path, src)) == []

    @pytest.mark.parametrize(
        "src",
        [
            pytest.param(
                """
def upgrade():
    op.execute("UPDATE dag_run SET run_type='x'")
    with op.batch_alter_table("dag") as batch_op:
        batch_op.alter_column("x", nullable=False)
""",
                id="different-table-than-ddl",
            ),
            pytest.param(
                """
def upgrade():
    op.execute("update dag_run set run_type = 'asset_triggered'")
""",
                id="standalone-dml-no-ddl",
            ),
        ],
    )
    def test_violation(self, tmp_path, src):
        errors = check_mig003(parse_migration(tmp_path, src))
        assert any("MIG003" in e for e in errors)


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
