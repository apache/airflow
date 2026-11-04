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

from airflow.providers.modal.utils.python_sandbox import (
    ENTRYPOINT,
    TIMEOUT_RETURNCODE,
    build_payload,
    decode_result,
    send_payload,
    wait_for_exit,
)

from unit.modal.local_sandbox import STDIN_BUFFER_BYTES, LocalSandbox


def _run(tmp_path, source: str, function_name: str, *args, timeout: int | None = None, **kwargs):
    sandbox = LocalSandbox(*ENTRYPOINT, tmp_path=tmp_path, timeout=timeout)
    payload = build_payload(
        source=textwrap.dedent(source), function_name=function_name, args=args, kwargs=kwargs
    )
    # LocalSandbox stands in for modal.Sandbox with the same surface.
    send_payload(sandbox, payload)  # type: ignore[arg-type]
    wait_for_exit(sandbox)  # type: ignore[arg-type]
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
        assert decode_result(sandbox.stdout.read()) == {"mean": 20, "n": 3}

    def test_none_return_value(self, tmp_path):
        sandbox = _run(tmp_path, "def noop():\n    pass\n", "noop")

        assert sandbox.returncode == 0
        assert decode_result(sandbox.stdout.read()) is None

    def test_everything_the_function_prints_goes_to_stderr_and_stdout_holds_only_the_result(self, tmp_path):
        source = """
        def chatty():
            import subprocess
            import sys

            print("from print")
            print("from stderr", file=sys.stderr)
            subprocess.run([sys.executable, "-c", "print('from a subprocess')"], check=True)
            sys.stdout.write("no newline at the end")
            return 42
        """

        sandbox = _run(tmp_path, source, "chatty")

        assert list(sandbox.stderr) == [
            "from print\n",
            "from stderr\n",
            "from a subprocess\n",
            "no newline at the end",
        ]
        assert decode_result(sandbox.stdout.read()) == 42

    def test_exception_exits_nonzero_with_traceback_on_stderr_and_no_result(self, tmp_path):
        source = """
        def fail():
            print("before the error")
            raise ValueError("boom")
        """

        sandbox = _run(tmp_path, source, "fail")

        assert sandbox.returncode == 1
        lines = [line.rstrip("\n") for line in sandbox.stderr]
        assert lines[0] == "before the error"
        assert lines[-1] == "ValueError: boom"
        assert '    raise ValueError("boom")' in lines, "traceback should quote the function's source"
        assert sandbox.stdout.read() == ""

    def test_classes_defined_in_the_function_work(self, tmp_path):
        source = """
        def records(rows):
            from dataclasses import asdict, dataclass

            @dataclass
            class Row:
                id: int
                name: str

            return [asdict(Row(**row)) for row in rows]
        """

        sandbox = _run(tmp_path, source, "records", [{"id": 1, "name": "ada"}])

        assert sandbox.returncode == 0, sandbox.stderr.read()
        assert decode_result(sandbox.stdout.read()) == [{"id": 1, "name": "ada"}]

    def test_arguments_larger_than_the_stdin_buffer_arrive_whole(self, tmp_path):
        big = "x" * (STDIN_BUFFER_BYTES + 1)

        sandbox = _run(tmp_path, "def size(text):\n    return len(text)\n", "size", big)

        assert sandbox.returncode == 0
        assert decode_result(sandbox.stdout.read()) == len(big)

    def test_annotations_naming_types_the_sandbox_lacks_are_not_evaluated(self, tmp_path):
        source = """
        def identity(frame: pd.DataFrame, rows: Sequence[Row]) -> pd.DataFrame:
            return frame
        """

        sandbox = _run(tmp_path, source, "identity", {"a": 1}, [])

        assert sandbox.returncode == 0, sandbox.stderr.read()
        assert decode_result(sandbox.stdout.read()) == {"a": 1}


def test_wait_for_exit_turns_a_timeout_into_its_exit_code(tmp_path):
    # Modal's wait raises SandboxTimeoutError here even with raise_on_termination=False.
    sandbox = _run(tmp_path, "def nap():\n    import time\n    time.sleep(30)\n", "nap", timeout=1)

    assert sandbox.returncode == TIMEOUT_RETURNCODE


class TestDecodeResult:
    def test_empty_stdout_raises(self):
        with pytest.raises(ValueError, match="wrote no return value"):
            decode_result("")

    @pytest.mark.parametrize("dropped", [1, 2, 3, 4, 5, 6, 7, 1000, 1001, 1002, 1003])
    def test_a_value_modal_kept_only_the_end_of_raises_a_clear_error(self, tmp_path, dropped):
        # Modal drops the start of output it cannot keep. Cuts at every base64 alignment and
        # inside the length header must all be caught, not decoded into a wrong value.
        sandbox = _run(tmp_path, "def big():\n    return list(range(5000))\n", "big")
        whole = sandbox.stdout.read()

        with pytest.raises(ValueError, match="too large for Modal to keep"):
            decode_result(whole[dropped:])
