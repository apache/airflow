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
defines the function and calls it. Everything the function prints, including what its
subprocesses write and any traceback, goes to the sandbox's stderr, in order; the task log
follows that stream. Stdout carries only the return value, pickled, base64-encoded and
prefixed with its length (``<length>:<base64>``), written once the function has returned, so a
function that raises leaves stdout empty and exits nonzero.

The bootstrap needs only the Python standard library, so the image needs neither Airflow nor
the Modal client. The shared virtualenv script template is deliberately not used: it
reconnects to an Airflow supervisor when it finds Airflow installed, and a sandbox has none.

Both streams outlive the sandbox and can be read through a new handle from
``Sandbox.from_id``, which is what lets the deferrable path follow the output on the triggerer
and collect the return value back on a worker. Both the decorator, on a worker, and the
trigger, on the triggerer, use this module.
"""

from __future__ import annotations

import base64
import contextlib
import json
import pickle
from collections.abc import Iterable
from typing import Any

import modal

BOOTSTRAP = r"""
import __future__, base64, json, os, pickle, sys, tempfile, types

# Keep the real stdout for the result, and point fd 1 at stderr so everything else the
# function prints, from Python or from a subprocess, lands on stderr.
result = os.fdopen(os.dup(1), "wb")
os.dup2(2, 1)
payload = json.loads(sys.stdin.read())
# Compiling from a file, rather than a string, lets tracebacks show the function's source lines.
path = os.path.join(tempfile.mkdtemp(prefix="airflow-modal-task-"), payload["function_name"] + ".py")
with open(path, "w") as f:
    f.write(payload["source"])
# A real, registered module: dataclasses and other tools look a class's module up in sys.modules.
module = types.ModuleType("__airflow_modal_task__")
sys.modules[module.__name__] = module
# Postponed annotations, as in a Dag file with `from __future__ import annotations`: the
# function's annotations may name types that only the Dag file imported.
code = compile(
    payload["source"], path, "exec", flags=__future__.annotations.compiler_flag, dont_inherit=True
)
exec(code, module.__dict__)
arguments = pickle.loads(base64.b64decode(payload["arguments"]))
value = getattr(module, payload["function_name"])(*arguments["args"], **arguments["kwargs"])
# Length-prefixed, so a value Modal kept only the end of is detected instead of misread.
encoded = base64.b64encode(pickle.dumps(value))
result.write(b"%d:" % len(encoded) + encoded)
result.close()
"""

#: The sandbox command: run :data:`BOOTSTRAP` with the image's ``python``, unbuffered so
#: output reaches the task log as it is printed.
ENTRYPOINT: tuple[str, ...] = ("python", "-u", "-c", BOOTSTRAP)

#: Exit codes the Modal SDK reports for a sandbox that did not exit on its own.
TIMEOUT_RETURNCODE = 124
TERMINATED_RETURNCODE = 137

#: Bytes written to sandbox stdin between drains: half of the Modal SDK's 2 MiB stdin buffer,
#: which raises ``BufferError`` when a single write would overflow it.
STDIN_CHUNK_BYTES = 1024 * 1024


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
        }
    )


def send_payload(sandbox: modal.Sandbox, payload: str) -> None:
    """Write the payload to the sandbox's stdin in pieces that fit the SDK's buffer, then close it."""
    data = memoryview(payload.encode())
    for start in range(0, len(data), STDIN_CHUNK_BYTES):
        sandbox.stdin.write(data[start : start + STDIN_CHUNK_BYTES])
        sandbox.stdin.drain()
    sandbox.stdin.write_eof()
    sandbox.stdin.drain()


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


def decode_result(stdout: str) -> Any:
    """
    Return the function's return value from the sandbox's stdout.

    :raises ValueError: When stdout is empty, which happens when the function did not return,
        or does not hold a whole value, which happens when Modal kept only the end of a large one.
    """
    if not stdout:
        raise ValueError("The sandbox exited cleanly but wrote no return value.")
    # Modal drops the start of output it cannot keep, which takes the length header with it
    # or leaves a header that no longer matches.
    header, separator, encoded = stdout.partition(":")
    if not separator or not header.isdigit() or int(header) != len(encoded):
        raise ValueError(
            f"The return value ({len(stdout)} characters read back) is incomplete, most likely "
            "because it was too large for Modal to keep. Write large results to a Modal volume "
            "and return their path instead."
        )
    return pickle.loads(base64.b64decode(encoded))
