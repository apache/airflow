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

import pytest

from airflow_shared.logging.tracebacks import format_exception_dicts


def stack(exc_type, exc_value, frames=(), **overrides):
    """Build one entry of an ``ExceptionDictTransformer`` payload."""
    return {
        "exc_type": exc_type,
        "exc_value": exc_value,
        "exc_notes": [],
        "syntax_error": None,
        "is_cause": False,
        "frames": [{"filename": f, "lineno": n, "name": fn} for f, n, fn in frames],
        "is_group": False,
        "exceptions": [],
        **overrides,
    }


def corrupt(**overrides):
    """Build a payload entry whose shape is valid except for the overridden keys."""
    return {**stack("ValueError", "boom"), **overrides}


class TestFormatExceptionDicts:
    def test_single_exception_renders_like_a_python_traceback(self):
        payload = [stack("ValueError", "boom", [("/app/dags/my_dag.py", 12, "run")])]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "/app/dags/my_dag.py", line 12, in run\n'
            "ValueError: boom"
        )

    def test_cause_chain_is_ordered_innermost_first_with_direct_cause_banner(self):
        payload = [
            stack("RuntimeError", "outer", [("<string>", 12, "<module>")]),
            stack(
                "ValueError",
                "inner boom",
                [("<string>", 10, "<module>"), ("<string>", 7, "boom")],
                is_cause=True,
            ),
        ]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "<string>", line 10, in <module>\n'
            '  File "<string>", line 7, in boom\n'
            "ValueError: inner boom\n"
            "\n"
            "The above exception was the direct cause of the following exception:\n"
            "\n"
            "Traceback (most recent call last):\n"
            '  File "<string>", line 12, in <module>\n'
            "RuntimeError: outer"
        )

    def test_notes_are_appended_after_the_exception_line(self):
        payload = [
            stack(
                "RuntimeError",
                "noted",
                [("<string>", 3, "<module>")],
                exc_notes=["note one", "note two"],
            )
        ]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "<string>", line 3, in <module>\n'
            "RuntimeError: noted\n"
            "note one\n"
            "note two"
        )

    def test_exception_without_frames_omits_the_traceback_header(self):
        assert format_exception_dicts([stack("ValueError", "first")]) == "ValueError: first"

    def test_frame_source_line_is_indented_under_its_file_line(self):
        payload = [stack("ValueError", "boom")]
        payload[0]["frames"] = [
            {"filename": "/app/x.py", "lineno": 4, "name": "run", "line": "    raise ValueError('boom')"}
        ]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "/app/x.py", line 4, in run\n'
            "    raise ValueError('boom')\n"
            "ValueError: boom"
        )

    def test_truncated_frame_sentinel_renders_as_a_note_not_a_bogus_file_line(self):
        payload = [stack("RecursionError", "too deep")]
        payload[0]["frames"] = [
            {"filename": "/app/x.py", "lineno": 4, "name": "run"},
            {"filename": "", "lineno": -1, "name": "Skipped frames: 12"},
            {"filename": "/app/x.py", "lineno": 4, "name": "run"},
        ]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "/app/x.py", line 4, in run\n'
            "  [Skipped frames: 12]\n"
            '  File "/app/x.py", line 4, in run\n'
            "RecursionError: too deep"
        )

    def test_syntax_error_renders_offending_line_with_a_caret(self):
        payload = [
            stack(
                "SyntaxError",
                "invalid syntax (badfile.py, line 1)",
                [("<string>", 4, "<module>")],
                syntax_error={
                    "offset": 7,
                    "filename": "badfile.py",
                    "line": "def f(:\n",
                    "lineno": 1,
                    "msg": "invalid syntax",
                },
            )
        ]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "<string>", line 4, in <module>\n'
            '  File "badfile.py", line 1\n'
            "    def f(:\n"
            "          ^\n"
            "SyntaxError: invalid syntax"
        )

    def test_syntax_error_caret_accounts_for_stripped_indentation(self):
        payload = [
            stack(
                "SyntaxError",
                "invalid syntax (b.py, line 2)",
                syntax_error={
                    "offset": 11,
                    "filename": "b.py",
                    "line": "      x ==== 1\n",
                    "lineno": 2,
                    "msg": "invalid syntax",
                },
            )
        ]

        assert format_exception_dicts(payload) == (
            '  File "b.py", line 2\n    x ==== 1\n        ^\nSyntaxError: invalid syntax'
        )

    def test_syntax_error_with_out_of_range_offset_drops_the_caret(self):
        payload = [
            stack(
                "SyntaxError",
                "bad",
                syntax_error={
                    "offset": 999,
                    "filename": "b.py",
                    "line": "x = 1\n",
                    "lineno": 2,
                    "msg": "invalid syntax",
                },
            )
        ]

        assert format_exception_dicts(payload) == (
            '  File "b.py", line 2\n    x = 1\nSyntaxError: invalid syntax'
        )

    def test_exception_group_renders_numbered_sub_exception_blocks(self):
        payload = [
            stack(
                "ExceptionGroup",
                "group boom (2 sub-exceptions)",
                [("<string>", 4, "<module>")],
                is_group=True,
                exceptions=[
                    [stack("ValueError", "first")],
                    [stack("KeyError", "'second'")],
                ],
            )
        ]

        assert format_exception_dicts(payload) == (
            "  + Exception Group Traceback (most recent call last):\n"
            '  |   File "<string>", line 4, in <module>\n'
            "  | ExceptionGroup: group boom (2 sub-exceptions)\n"
            "  +-+---------------- 1 ----------------\n"
            "    | ValueError: first\n"
            "    +---------------- 2 ----------------\n"
            "    | KeyError: 'second'\n"
            "    +------------------------------------"
        )

    def test_nested_exception_group_indents_and_keeps_inner_cause_chains(self):
        payload = [
            stack(
                "ExceptionGroup",
                "outer group (2 sub-exceptions)",
                [("<string>", 14, "<module>")],
                is_group=True,
                exceptions=[
                    [
                        stack(
                            "TypeError", "wrapped", [("<string>", 10, "<module>"), ("<string>", 7, "inner")]
                        ),
                        stack("ValueError", "root cause", [("<string>", 5, "inner")], is_cause=True),
                    ],
                    [
                        stack(
                            "ExceptionGroup",
                            "nested group (1 sub-exception)",
                            [("<string>", 12, "<module>")],
                            is_group=True,
                            exceptions=[[stack("OSError", "io fail")]],
                        )
                    ],
                ],
            )
        ]

        assert format_exception_dicts(payload) == (
            "  + Exception Group Traceback (most recent call last):\n"
            '  |   File "<string>", line 14, in <module>\n'
            "  | ExceptionGroup: outer group (2 sub-exceptions)\n"
            "  +-+---------------- 1 ----------------\n"
            "    | Traceback (most recent call last):\n"
            '    |   File "<string>", line 5, in inner\n'
            "    | ValueError: root cause\n"
            "    | \n"
            "    | The above exception was the direct cause of the following exception:\n"
            "    | \n"
            "    | Traceback (most recent call last):\n"
            '    |   File "<string>", line 10, in <module>\n'
            '    |   File "<string>", line 7, in inner\n'
            "    | TypeError: wrapped\n"
            "    +---------------- 2 ----------------\n"
            "    | Exception Group Traceback (most recent call last):\n"
            '    |   File "<string>", line 12, in <module>\n'
            "    | ExceptionGroup: nested group (1 sub-exception)\n"
            "    +-+---------------- 1 ----------------\n"
            "      | OSError: io fail\n"
            "      +------------------------------------"
        )

    def test_context_chain_uses_during_handling_banner(self):
        payload = [
            stack("RuntimeError", "outer", [("<string>", 12, "<module>")]),
            stack("ValueError", "inner boom", [("<string>", 7, "boom")], is_cause=False),
        ]

        assert format_exception_dicts(payload) == (
            "Traceback (most recent call last):\n"
            '  File "<string>", line 7, in boom\n'
            "ValueError: inner boom\n"
            "\n"
            "During handling of the above exception, another exception occurred:\n"
            "\n"
            "Traceback (most recent call last):\n"
            '  File "<string>", line 12, in <module>\n'
            "RuntimeError: outer"
        )


class TestMalformedPayloads:
    """A worker sends these dicts over as JSON, so the renderer must never take the log line down."""

    @pytest.mark.parametrize(
        "payload",
        [
            None,
            {},
            "boom",
            42,
            [],
            {"exc_type": "ValueError"},
            [None],
            ["ValueError: boom"],
            [{"exc_value": "no type here"}],
            [corrupt(frames="not a list")],
            [corrupt(frames=["not a frame"])],
            [corrupt(frames=[{"filename": "x.py"}, None])],
            [corrupt(syntax_error="not a dict")],
            [corrupt(exc_notes="not a list")],
            [corrupt(is_group=True, exceptions=["not a chain"])],
            [corrupt(is_group=True, exceptions=[[None]])],
        ],
    )
    def test_unusable_payload_falls_back_instead_of_raising(self, payload):
        assert format_exception_dicts(payload) is None

    def test_entry_missing_optional_keys_still_renders(self):
        assert format_exception_dicts([{"exc_type": "ValueError", "exc_value": "boom"}]) == (
            "ValueError: boom"
        )

    def test_deeply_nested_groups_are_truncated_rather_than_recursing(self):
        payload = stack("ValueError", "innermost")
        for level in range(40):
            payload = stack(
                "ExceptionGroup",
                f"level {level} (1 sub-exception)",
                [("<string>", 1, "<module>")],
                is_group=True,
                exceptions=[[payload]],
            )

        rendered = format_exception_dicts([payload])

        assert rendered is not None
        assert "more nested sub-exceptions)" in rendered
        assert "ValueError: innermost" not in rendered


# Compiling from a name that is not on disk keeps `linecache` from finding source, so CPython
# omits the source lines that structlog never captures. What is left is directly comparable.
_HARNESS = """
import traceback
from structlog.tracebacks import ExceptionDictTransformer
try:
{body}
except BaseException as exc:
    info = (type(exc), exc, exc.__traceback__)
    stdlib = "".join(traceback.format_exception(*info))
    dicts = ExceptionDictTransformer(use_rich=False, show_locals=False)(info)
"""


def run_in_synthetic_module(body: str) -> dict:
    namespace: dict = {}
    exec(compile(_HARNESS.format(body=body), "<synthetic-dag>", "exec"), namespace)
    return namespace


class TestAgainstRealTransformerOutput:
    """Round-trip real structlog payloads and compare against CPython's own renderer."""

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(
                "    try:\n"
                "        raise ValueError('inner')\n"
                "    except ValueError as e:\n"
                "        raise RuntimeError('outer') from e",
                id="cause-chain",
            ),
            pytest.param(
                "    try:\n"
                "        raise ValueError('inner')\n"
                "    except ValueError:\n"
                "        raise RuntimeError('outer')",
                id="context-chain",
            ),
            pytest.param(
                "    e = RuntimeError('noted')\n"
                "    e.add_note('note one')\n"
                "    e.add_note('note two')\n"
                "    raise e",
                id="notes",
            ),
            pytest.param(
                "    raise ExceptionGroup('boom', [ValueError('first'), KeyError('second')])",
                id="exception-group",
            ),
            pytest.param(
                "    raise ExceptionGroup(\n"
                "        'outer', [ValueError('a'), ExceptionGroup('inner', [OSError('io')])]\n"
                "    )",
                id="nested-exception-group",
            ),
            pytest.param("    compile('def f(:', 'bad.py', 'exec')", id="syntax-error"),
        ],
    )
    def test_output_matches_cpython_traceback(self, body):
        namespace = run_in_synthetic_module(body)

        assert format_exception_dicts(namespace["dicts"]) + "\n" == namespace["stdlib"]

    def test_over_long_traceback_reports_the_frames_structlog_dropped(self):
        namespace = run_in_synthetic_module("    def r(n):\n        return r(n + 1)\n    r(0)")

        rendered = format_exception_dicts(namespace["dicts"])

        assert rendered is not None
        # structlog keeps the first and last 25 frames and replaces the rest with a counter,
        # so this can never match CPython, which instead collapses repeats as it walks.
        assert len([line for line in rendered.splitlines() if line.startswith('  File "')]) == 50
        assert any(line.startswith("  [Skipped frames: ") for line in rendered.splitlines())
        assert rendered.endswith("RecursionError: maximum recursion depth exceeded")

    def test_caret_is_single_because_structlog_drops_the_end_offset(self):
        namespace = run_in_synthetic_module("    compile('x ==== 1', 'bad.py', 'exec')")

        rendered = format_exception_dicts(namespace["dicts"])

        assert "    x ==== 1\n        ^\n" in rendered
        # CPython would underline the whole operator, but `end_offset` is not in the payload.
        assert "^^" in namespace["stdlib"]
