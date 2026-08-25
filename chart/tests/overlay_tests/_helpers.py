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

"""Non-fixture helpers shared by overlay tests and ``conftest.py``.

These live in a regular module (not in ``conftest.py``) because pytest's
``conftest`` module is special: the bare ``from conftest import …`` form
resolves to whichever conftest sits highest on sys.path, which under
``chart/tests/`` is the empty parent ``chart/tests/conftest.py`` rather
than this directory's local one. A sibling module sidesteps that and
both ``conftest.py`` and ``test_*.py`` can pull from here unambiguously.
"""

from __future__ import annotations

import json
import subprocess


def kubectl(*args: str) -> subprocess.CompletedProcess[str]:
    """One-shot kubectl invocation capturing text stdout/stderr.

    No retry: tests should express waits via kubectl itself
    (``kubectl wait``, ``kubectl rollout status``) instead of polling
    from the call site. Use ``check=False`` semantics — callers inspect
    ``returncode`` so they can produce a clear failure message
    including the captured output.
    """
    return subprocess.run(["kubectl", *args], check=False, capture_output=True, text=True)


def get_secret_data(name: str, namespace: str) -> dict[str, str]:
    """Return the ``data`` map of a Secret (base64-encoded values).

    Fails the calling test if the Secret does not exist or the kubectl
    invocation errored.
    """
    result = kubectl("get", "secret", name, "-n", namespace, "-o", "json")
    assert result.returncode == 0, f"Secret {name} not found in {namespace}: {result.stderr}"
    return json.loads(result.stdout).get("data", {}) or {}
