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
import textwrap

import pytest
from ci.prek import check_mapped_column_nullable_annotations as hook
from ci.prek.check_mapped_column_nullable_annotations import check_files, iter_mismatches, main

MODEL_PREAMBLE = """\
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union

from sqlalchemy import DateTime, Integer, orm
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

IS_NULLABLE = True


class Base(DeclarativeBase):
    pass


class Model(Base):
    __tablename__ = "model"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
"""
COLUMN_LINE = MODEL_PREAMBLE.count("\n") + 1
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def model_with(attribute_line: str) -> str:
    return MODEL_PREAMBLE + textwrap.indent(textwrap.dedent(attribute_line), "    ")


class TestIterMismatches:
    @pytest.mark.parametrize(
        ("attribute_line", "expected_annotation"),
        [
            pytest.param(
                "created_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)",
                "Mapped[datetime]",
                id="plain",
            ),
            pytest.param(
                "trigger_id: Mapped[int] = mapped_column(Integer, nullable=True, default=None)",
                "Mapped[int]",
                id="other-kwargs",
            ),
            pytest.param(
                'created_at: Mapped["datetime"] = mapped_column(DateTime, nullable=True)',
                "Mapped['datetime']",
                id="string-inner",
            ),
            pytest.param(
                'created_at: "Mapped[datetime]" = mapped_column(DateTime, nullable=True)',
                "Mapped[datetime]",
                id="string-whole",
            ),
            pytest.param(
                "created_at: orm.Mapped[datetime] = orm.mapped_column(DateTime, nullable=True)",
                "orm.Mapped[datetime]",
                id="qualified",
            ),
        ],
    )
    def test_reports_nullable_column_whose_annotation_cannot_be_none(
        self, write_python_file, attribute_line, expected_annotation
    ):
        path = write_python_file(model_with(attribute_line))

        mismatches = list(iter_mismatches(path))

        assert len(mismatches) == 1
        mismatch = mismatches[0]
        assert mismatch.path == path
        assert mismatch.lineno == COLUMN_LINE
        assert mismatch.attribute == attribute_line.split(":")[0]
        assert mismatch.annotation == expected_annotation

    @pytest.mark.parametrize(
        "annotation",
        [
            "Mapped[datetime | None]",
            "Mapped[None | datetime]",
            "Mapped[Optional[datetime]]",
            "Mapped[Union[datetime, None]]",
            "Mapped[Union[None, datetime]]",
            "Mapped[Any]",
            "Mapped[dict[str, Any] | None]",
            'Mapped["datetime | None"]',
            '"Mapped[datetime | None]"',
        ],
    )
    def test_accepts_annotations_that_admit_none(self, write_python_file, annotation):
        path = write_python_file(
            model_with(f"created_at: {annotation} = mapped_column(DateTime, nullable=True)")
        )

        assert list(iter_mismatches(path)) == []

    @pytest.mark.parametrize(
        "attribute_line",
        [
            pytest.param("created_at: Mapped[datetime] = mapped_column(DateTime)", id="nullable-derived"),
            pytest.param(
                "created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)", id="not-nullable"
            ),
            pytest.param(
                "created_at: Mapped[datetime] = mapped_column(DateTime, nullable=IS_NULLABLE)",
                id="non-literal-nullable",
            ),
            pytest.param(
                "created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=False)",
                id="optional-on-not-null-is-not-reported",
            ),
            pytest.param("children: Mapped[list[Model]] = relationship(Model)", id="relationship"),
            pytest.param("created_at = mapped_column(DateTime, nullable=True)", id="unannotated"),
            pytest.param("created_at: Mapped[datetime]", id="no-value"),
            pytest.param("count: int = 0", id="not-mapped"),
        ],
    )
    def test_ignores_attributes_outside_the_rule(self, write_python_file, attribute_line):
        path = write_python_file(model_with(attribute_line))

        assert list(iter_mismatches(path)) == []

    def test_reports_multiline_call_at_the_annotation_line(self, write_python_file):
        path = write_python_file(
            model_with(
                """\
                updated_at: Mapped[datetime] = mapped_column(
                    DateTime,
                    default=datetime.now,
                    nullable=True,
                )
                """
            )
        )

        assert [(m.attribute, m.lineno) for m in iter_mismatches(path)] == [("updated_at", COLUMN_LINE)]

    def test_reports_every_mismatch_in_a_file(self, write_python_file):
        path = write_python_file(
            model_with(
                """\
                created_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
                updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
                trigger_id: Mapped[int] = mapped_column(Integer, nullable=True)
                """
            )
        )

        assert [m.attribute for m in iter_mismatches(path)] == ["created_at", "trigger_id"]

    def test_skips_files_without_mapped_column(self, write_python_file):
        path = write_python_file("created_at: Mapped[datetime] = Column(DateTime, nullable=True)\n")

        assert list(iter_mismatches(path)) == []

    def test_skips_unparsable_and_missing_files(self, write_python_file, tmp_path):
        broken = write_python_file("created_at: Mapped[datetime] = mapped_column(DateTime, nullable=True\n")

        assert list(iter_mismatches(broken)) == []
        assert list(iter_mismatches(tmp_path / "missing.py")) == []


class TestCheckFiles:
    def test_reports_each_mismatch_with_location_and_fails(self, write_python_file, capsys):
        path = write_python_file(
            model_with("created_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)")
        )

        assert check_files([path]) == 1

        output = _ANSI_RE.sub("", capsys.readouterr().out)
        assert f"{path}:{COLUMN_LINE}" in output
        assert "created_at" in output
        assert "Mapped[X | None]" in output

    def test_passes_silently_when_clean(self, write_python_file, capsys):
        path = write_python_file(
            model_with("created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)")
        )

        assert check_files([path]) == 0
        assert capsys.readouterr().out == ""


class TestMain:
    def test_checks_given_files(self, write_python_file):
        path = write_python_file(
            model_with("created_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)")
        )

        assert main([str(path)]) == 1

    def test_no_files_is_a_noop(self):
        assert main([]) == 0

    def test_all_files_walks_scan_roots_and_skips_hidden_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)
        monkeypatch.setattr(hook, "SCAN_ROOTS", ("dist",))
        bad = model_with("created_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)")
        for relative in (
            "dist/src/pkg/models.py",
            "dist/.venv/lib/models.py",
            "dist/node_modules/x/models.py",
        ):
            target = tmp_path / relative
            target.parent.mkdir(parents=True)
            target.write_text(bad)

        assert main(["--all-files"]) == 1
        assert [str(path.relative_to(tmp_path)) for path in hook.iter_python_files([tmp_path / "dist"])] == [
            "dist/src/pkg/models.py"
        ]
