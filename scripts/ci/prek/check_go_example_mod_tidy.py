#!/usr/bin/env python3
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
Keep the lang-SDK Go example module tidy against the Go SDK.

``kubernetes-tests/lang_sdk/go_example`` is a **separate** Go module that
resolves the SDK from the in-repo sources::

    replace github.com/apache/airflow/go-sdk => ../../../go-sdk

Because of that ``replace`` it carries its own copy of the SDK's indirect
requirements. Nothing re-tidies it when a dependency moves inside
``/go-sdk`` — and Dependabot bumps exactly one module per PR. The example
module is then left pinning the old versions, Go refuses to build an
inconsistent module, and ``Kubernetes tests / K8S Lang-SDK`` fails at the
"Build Go bundle" step::

    go: updates to go.mod needed; to update it:
            go mod tidy

The damage is not limited to the bump PR: once it merges, that job is red on
*every* pull request until someone notices and tidies the example module by
hand. This happened with #70226 (``google.golang.org/grpc`` 1.79.3 -> 1.82.1
in ``/go-sdk`` only) and was cleaned up after the fact by #70561.

Note that Dependabot **security** updates do not consult
``.github/dependabot.yml`` at all, so no amount of per-directory config
prevents this — and a second Dependabot PR for the example module would merge
at a different time, leaving ``main`` red in between. The drift has to fail
the bump PR itself, which is what this check does.

The check is ``go mod tidy -diff`` in the example module: it is the exact
question the failing CI step asks, it never writes to the working tree, and it
exits non-zero when the module is untidy.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_go_example_mod_tidy.py

Exits 0 if the example module is tidy, 1 otherwise.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
EXAMPLE_MODULE = pathlib.Path("kubernetes-tests/lang_sdk/go_example")
GO_SDK_MODULE = pathlib.Path("go-sdk")


def run_tidy_diff(module_dir: pathlib.Path, go_binary: str = "go") -> tuple[int, str]:
    """Ask Go whether ``module_dir`` is tidy. Returns ``(returncode, combined_output)``."""
    completed = subprocess.run(
        [go_binary, "mod", "tidy", "-diff"],
        cwd=module_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, (completed.stdout + completed.stderr).strip()


def format_report(returncode: int, output: str) -> tuple[int, str]:
    """Turn a ``go mod tidy -diff`` result into ``(exit_code, report)``."""
    if returncode == 0:
        return 0, f"OK: {EXAMPLE_MODULE} is tidy against {GO_SDK_MODULE}."
    lines = [
        f"ERROR: {EXAMPLE_MODULE} is not tidy.",
        "",
        f"It is a separate Go module that resolves the SDK via a `replace` onto {GO_SDK_MODULE},",
        "so it keeps its own copy of the SDK's indirect requirements. A dependency moved in",
        f"{GO_SDK_MODULE} without this module being re-tidied, which breaks the",
        "'Kubernetes tests / K8S Lang-SDK' bundle build on every pull request once merged.",
        "",
        "Fix it in this PR by running:",
        "",
        f"    (cd {EXAMPLE_MODULE} && go mod tidy)",
        "",
        "and committing the resulting go.mod / go.sum changes.",
        "",
        "`go mod tidy -diff` reported:",
        "",
        output or "(no output)",
    ]
    return 1, "\n".join(lines)


def main() -> int:
    module_dir = REPO_ROOT / EXAMPLE_MODULE
    if not (module_dir / "go.mod").is_file():
        print(f"ERROR: {EXAMPLE_MODULE}/go.mod not found — has the example module moved?")
        return 1
    if shutil.which("go") is None:
        if os.environ.get("CI"):
            print("ERROR: `go` is not on PATH but this is a CI run — the toolchain is required here.")
            return 1
        print(f"SKIPPED: `go` is not on PATH, cannot verify that {EXAMPLE_MODULE} is tidy.")
        return 0
    returncode, output = run_tidy_diff(module_dir)
    exit_code, report = format_report(returncode, output)
    print(report)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
