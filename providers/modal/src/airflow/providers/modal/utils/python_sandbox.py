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
How ``@task.modal`` runs a function in a Modal sandbox and gets its return value back.

The sandbox's command is a short bootstrap program (:data:`BOOTSTRAP`) run with ``python -c``.
It reads one JSON payload from stdin holding the function's source and its pickled arguments,
defines the function, calls it, and prints the pickled return value as one base64 line that
starts with :data:`RESULT_PREFIX`. Stderr is merged into stdout, so the task log keeps the
function's output and any traceback in order. A function that raises makes the bootstrap exit
nonzero with the traceback on stdout and no result line.

The bootstrap needs only the Python standard library, so the image needs neither Airflow nor
the Modal client. The shared virtualenv script template is deliberately not used: it
reconnects to an Airflow supervisor when it finds Airflow installed, and a sandbox has none.

Sandbox stdout outlives the sandbox and can be read through a new handle from
``Sandbox.from_id``, which is what lets the deferrable path collect the return value in a
different process from the one that started the sandbox. This module is shared by the
decorator, on a worker, and the trigger, on the triggerer, so it imports nothing from Airflow.
"""

from __future__ import annotations

import base64
import contextlib
import json
import pickle
from collections.abc import Iterable
from typing import Any

import modal

RESULT_PREFIX = "__airflow_modal_task_result__:"

#: Bytes written to sandbox stdin between drains: half of the Modal SDK's 2 MiB stdin buffer,
#: which raises ``BufferError`` when a single write would overflow it.
STDIN_CHUNK_BYTES = 1024 * 1024

BOOTSTRAP = r"""
import __future__, base64, json, os, pickle, sys, tempfile

os.dup2(1, 2)
payload = json.loads(sys.stdin.read())
# Compiling from a file, rather than a string, lets tracebacks show the function's source lines.
path = os.path.join(tempfile.mkdtemp(prefix="airflow-modal-task-"), payload["function_name"] + ".py")
with open(path, "w") as f:
    f.write(payload["source"])
namespace = {"__name__": "__airflow_modal_task__"}
# Postponed annotations, as in a Dag file with `from __future__ import annotations`: the
# function's annotations may name types that only the Dag file imported.
code = compile(
    payload["source"], path, "exec", flags=__future__.annotations.compiler_flag, dont_inherit=True
)
exec(code, namespace)
arguments = pickle.loads(base64.b64decode(payload["arguments"]))
result = namespace[payload["function_name"]](*arguments["args"], **arguments["kwargs"])
pickled = pickle.dumps(result) if result is not None else b""
sys.stdout.write("\n" + payload["result_prefix"] + base64.b64encode(pickled).decode() + "\n")
sys.stdout.flush()
"""

#: The sandbox command: run :data:`BOOTSTRAP` with the image's ``python``, unbuffered so
#: output reaches the task log as it is printed.
ENTRYPOINT: tuple[str, ...] = ("python", "-u", "-c", BOOTSTRAP)


def build_payload(*, source: str, function_name: str, args: Iterable[Any], kwargs: dict[str, Any]) -> str:
    """
    Return the stdin payload for :data:`BOOTSTRAP`.

    :param source: Source of the function, without its decorators.
    :param function_name: Name the source defines the function under.
    :param args: Positional arguments to call it with.
    :param kwargs: Keyword arguments to call it with.
    """
    arguments = pickle.dumps({"args": list(args), "kwargs": kwargs})
    return json.dumps(
        {
            "source": source,
            "function_name": function_name,
            "arguments": base64.b64encode(arguments).decode(),
            "result_prefix": RESULT_PREFIX,
        }
    )


def wait_for_exit(sandbox: modal.Sandbox) -> None:
    """
    Wait for the sandbox to exit, leaving its exit code on ``sandbox.returncode``.

    ``Sandbox.wait`` raises ``SandboxTimeoutError`` for a sandbox that hit its timeout even with
    ``raise_on_termination=False``. The exit code (124) is recorded before it raises, so the
    caller reports a timeout the same way as any other exit code.
    """
    with contextlib.suppress(modal.exception.SandboxTimeoutError):
        sandbox.wait(raise_on_termination=False)


async def await_exit(sandbox: modal.Sandbox) -> None:
    """Async counterpart of :func:`wait_for_exit`, for the triggerer's event loop."""
    with contextlib.suppress(modal.exception.SandboxTimeoutError):
        await sandbox.wait.aio(raise_on_termination=False)


def is_result_line(line: str) -> bool:
    """Whether ``line`` is the bootstrap's result line rather than output from the function."""
    return line.startswith(RESULT_PREFIX)


def find_result(lines: Iterable[str]) -> Any:
    """
    Return the function's return value from the sandbox's stdout lines.

    The last result line wins: the bootstrap prints it after the function has returned, so a
    line the function itself printed with the same prefix always comes earlier.

    :raises ValueError: When no line carries a result.
    """
    result_line = None
    for line in lines:
        if is_result_line(line):
            result_line = line
    if result_line is None:
        raise ValueError("The sandbox exited cleanly but printed no result line.")
    pickled = base64.b64decode(result_line[len(RESULT_PREFIX) :].strip())
    if not pickled:
        return None
    return pickle.loads(pickled)
