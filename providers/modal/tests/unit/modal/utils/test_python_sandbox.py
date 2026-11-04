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

import textwrap

import pytest

from airflow.providers.modal.decorators.modal import _ModalSandboxOperator
from airflow.providers.modal.utils.python_sandbox import (
    ENTRYPOINT,
    RESULT_PREFIX,
    build_payload,
    find_result,
    is_result_line,
    wait_for_exit,
)

from unit.modal.local_sandbox import STDIN_BUFFER_BYTES, TIMEOUT_RETURNCODE, LocalSandbox


def _run(tmp_path, source: str, function_name: str, *args, **kwargs) -> LocalSandbox:
    sandbox = LocalSandbox(*ENTRYPOINT, tmp_path=tmp_path)
    payload = build_payload(
        source=textwrap.dedent(source), function_name=function_name, args=args, kwargs=kwargs
    )
    # LocalSandbox stands in for modal.Sandbox with the same stdin surface.
    _ModalSandboxOperator._send_payload(sandbox, payload)  # type: ignore[arg-type]
    return sandbox


class TestBootstrap:
    def test_round_trips_arguments_and_return_value(self, tmp_path):
        source = """
        def summarize(values, *, scale):
            import statistics

            return {"mean": statistics.mean(values) * scale, "n": len(values)}
        """

        sandbox = _run(tmp_path, source, "summarize", [1, 2, 3], scale=10)

        assert sandbox.returncode == 0
        assert find_result(sandbox.stdout) == {"mean": 20, "n": 3}

    def test_none_return_value(self, tmp_path):
        sandbox = _run(tmp_path, "def noop():\n    pass\n", "noop")

        assert sandbox.returncode == 0
        assert find_result(sandbox.stdout) is None

    def test_output_without_trailing_newline_does_not_hide_the_result(self, tmp_path):
        source = """
        def chatty():
            import sys

            sys.stdout.write("no newline at the end")
            return 42
        """

        sandbox = _run(tmp_path, source, "chatty")

        lines = list(sandbox.stdout)
        assert "no newline at the end\n" in lines
        assert find_result(lines) == 42

    def test_a_printed_line_that_looks_like_a_result_does_not_win(self, tmp_path):
        source = f"""
        def spoof():
            print("{RESULT_PREFIX}gARLAS4=")
            return "real"
        """

        sandbox = _run(tmp_path, source, "spoof")

        assert find_result(sandbox.stdout) == "real"

    def test_exception_exits_nonzero_with_traceback_in_stdout(self, tmp_path):
        source = """
        def fail():
            print("before the error")
            raise ValueError("boom")
        """

        sandbox = _run(tmp_path, source, "fail")

        assert sandbox.returncode == 1
        lines = [line.rstrip("\n") for line in sandbox.stdout]
        assert lines[0] == "before the error"
        assert lines[-1] == "ValueError: boom"
        assert '    raise ValueError("boom")' in lines, "traceback should quote the function's source"
        assert not any(is_result_line(line) for line in lines)

    def test_arguments_larger_than_the_stdin_buffer_arrive_whole(self, tmp_path):
        big = "x" * (STDIN_BUFFER_BYTES + 1)

        sandbox = _run(tmp_path, "def size(text):\n    return len(text)\n", "size", big)

        assert sandbox.returncode == 0
        assert find_result(sandbox.stdout) == len(big)

    def test_annotations_naming_types_the_sandbox_lacks_are_not_evaluated(self, tmp_path):
        source = """
        def identity(frame: pd.DataFrame, rows: Sequence[Row]) -> pd.DataFrame:
            return frame
        """

        sandbox = _run(tmp_path, source, "identity", {"a": 1}, [])

        assert sandbox.returncode == 0, "".join(sandbox.stdout)
        assert find_result(sandbox.stdout) == {"a": 1}


def test_wait_for_exit_turns_a_timeout_into_its_exit_code(tmp_path):
    sandbox = LocalSandbox(*ENTRYPOINT, tmp_path=tmp_path, timeout=1)
    payload = build_payload(
        source="def nap():\n    import time\n    time.sleep(30)\n", function_name="nap", args=(), kwargs={}
    )
    _ModalSandboxOperator._send_payload(sandbox, payload)

    wait_for_exit(sandbox)

    assert sandbox.returncode == TIMEOUT_RETURNCODE


def test_find_result_without_result_line_raises():
    with pytest.raises(ValueError, match="no result line"):
        find_result(["just output\n"])
