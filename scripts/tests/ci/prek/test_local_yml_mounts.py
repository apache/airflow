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
"""Tests for the exit code of local_yml_mounts.py."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[3] / "ci" / "prek" / "local_yml_mounts.py"

CALL_LOG_NAME = "breeze_calls.txt"

BREEZE_STUB = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import os
    import sys

    with open(os.environ["BREEZE_STUB_CALL_LOG"], "a") as call_log:
        call_log.write(" ".join(sys.argv[1:]) + "\\n")

    sys.exit(int(os.environ["BREEZE_STUB_RETURNCODE"]))
    """
)


def run_script(tmp_path: Path, breeze_returncode: int) -> subprocess.CompletedProcess:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    breeze_stub = bin_dir / "breeze"
    breeze_stub.write_text(BREEZE_STUB)
    breeze_stub.chmod(0o755)

    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "BREEZE_STUB_CALL_LOG": os.fspath(tmp_path / CALL_LOG_NAME),
        "BREEZE_STUB_RETURNCODE": str(breeze_returncode),
    }
    # initialize_breeze_prek honours this by exiting 0 before it ever reaches breeze.
    env.pop("SKIP_BREEZE_PREK_HOOKS", None)

    # The script exits when imported as a module, so it has to run as a subprocess.
    return subprocess.run(
        [sys.executable, os.fspath(SCRIPT_PATH)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("breeze_returncode", [0, 1, 2])
def test_hook_exits_with_the_breeze_return_code(tmp_path, breeze_returncode):
    result = run_script(tmp_path, breeze_returncode)

    # Without this, an exit from initialize_breeze_prek's own preflight would satisfy the next line.
    assert (tmp_path / CALL_LOG_NAME).exists(), f"breeze was never invoked: {result.stdout}{result.stderr}"
    assert result.returncode == breeze_returncode
