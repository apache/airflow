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
from __future__ import annotations

import sys

import pytest
import structlog

from airflow_shared.logging.tracebacks import format_error_detail


def _render(exc: BaseException) -> str:
    # Same transformer settings as the JSON log output in airflow_shared.logging.structlog.
    transformer = structlog.tracebacks.ExceptionDictTransformer(use_rich=False, show_locals=False)
    rendered = format_error_detail(transformer((type(exc), exc, exc.__traceback__)))
    assert rendered is not None
    return rendered


def _raise(exc: BaseException):
    raise exc


def _nest_groups(depth: int) -> list:
    stacks = [{"exc_type": "ValueError", "exc_value": "leaf", "frames": []}]
    for _ in range(depth):
        stacks = [{"exc_type": "ExceptionGroup", "is_group": True, "frames": [], "exceptions": [stacks]}]
    return stacks


class TestFormatErrorDetail:
    @pytest.mark.parametrize(
        "error_detail",
        [
            pytest.param(None, id="none"),
            pytest.param([], id="empty-list"),
            pytest.param("boom", id="string"),
            pytest.param(["not a dict"], id="non-dict-item"),
            pytest.param([{"exc_type": "E", "exc_notes": None}], id="notes-none"),
            pytest.param([{"exc_type": "E", "exc_notes": [1, 2]}], id="notes-not-str"),
            pytest.param([{"exc_type": "E", "frames": 5}], id="frames-not-list"),
            pytest.param([{"exc_type": 5, "exc_value": ""}], id="type-not-str"),
            pytest.param(_nest_groups(sys.getrecursionlimit()), id="groups-nested-too-deep"),
        ],
    )
    def test_returns_none_when_it_cannot_render(self, error_detail):
        assert format_error_detail(error_detail) is None

    def test_explicit_cause_prints_cause_first(self):
        try:
            try:
                _raise(ValueError("inner"))
            except ValueError as e:
                raise RuntimeError("outer") from e
        except RuntimeError as e:
            rendered = _render(e)

        inner, sep, outer = rendered.partition(
            "\n\nThe above exception was the direct cause of the following exception:\n\n"
        )
        assert sep
        assert inner.startswith("Traceback (most recent call last):\n")
        assert inner.endswith("ValueError: inner")
        assert outer.startswith("Traceback (most recent call last):\n")
        assert outer.endswith("RuntimeError: outer")
        assert "During handling" not in rendered

    def test_implicit_context(self):
        try:
            try:
                _raise(KeyError("missing"))
            except KeyError:
                _raise(TypeError("while handling"))
        except TypeError as e:
            rendered = _render(e)

        first, sep, second = rendered.partition(
            "\n\nDuring handling of the above exception, another exception occurred:\n\n"
        )
        assert sep
        assert first.endswith("KeyError: 'missing'")
        assert second.startswith("Traceback (most recent call last):\n")
        assert second.endswith("TypeError: while handling")
        assert "direct cause" not in rendered

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="ExceptionGroup needs Python 3.11+")
    def test_exception_group_renders_every_sub_exception(self):
        try:
            _raise(ValueError("one\nmore"))
        except ValueError as e:
            one = e
        try:
            _raise(ExceptionGroup("many", [one, TypeError("two")]))  # noqa: F821
        except ExceptionGroup as e:  # noqa: F821
            rendered = _render(e)

        lines = rendered.splitlines()
        assert lines[0] == "Exception Group Traceback (most recent call last):"
        assert "ExceptionGroup: many (2 sub-exceptions)" in lines
        assert lines.index("  +---- 1 ----") < lines.index("  | ValueError: one") < lines.index("  | more")
        # TypeError("two") was never raised, so like Python it has no traceback header.
        assert lines[lines.index("  +---- 2 ----") + 1 :] == ["  | TypeError: two"]

    def test_skipped_frames_notes_and_empty_value(self):
        detail = [
            {
                "exc_type": "RecursionError",
                "exc_value": "",
                "exc_notes": ["first note", "second note"],
                "frames": [
                    {"filename": "/dags/t.py", "lineno": 3, "name": "f"},
                    {"filename": "", "lineno": -1, "name": "Skipped frames: 40"},
                    {"filename": "/dags/t.py", "lineno": 3, "name": "f"},
                ],
            }
        ]

        assert format_error_detail(detail) == (
            "Traceback (most recent call last):\n"
            '  File "/dags/t.py", line 3, in f\n'
            "  [Skipped frames: 40]\n"
            '  File "/dags/t.py", line 3, in f\n'
            "RecursionError\n"
            "first note\n"
            "second note"
        )
