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
Keep the lang-SDK Go modules tidy against the Go SDK.

Two modules are **separate** Go modules that resolve the SDK from the in-repo
sources, each through a ``replace`` onto ``go-sdk``::

    kubernetes-tests/lang_sdk/go_example   (``Kubernetes tests / K8S Lang-SDK``)
    airflow-e2e-tests/go-test-bundle       (``Go SDK e2e test``)

Because of that ``replace`` each carries its own copy of the SDK's indirect
requirements. Nothing re-tidies them when a dependency moves inside
``/go-sdk``, and Dependabot bumps exactly one module per PR. A module is
then left pinning the old versions, Go refuses to build an inconsistent
module, and the job that builds it fails where it does (the "Build Go bundle"
step of the K8S Lang-SDK job, or the test step of the Go SDK e2e job, which
packs the bundle)::

    go: updates to go.mod needed; to update it:
            go mod tidy

The damage is not limited to the bump PR: once it merges, that job is red on
*every* pull request until someone notices and tidies the module by
hand. This happened with #70226 (``google.golang.org/grpc`` 1.79.3 -> 1.82.1
in ``/go-sdk`` only) and was cleaned up after the fact by #70561.

Note that Dependabot **security** updates do not consult
``.github/dependabot.yml`` at all, so no amount of per-directory config
prevents this, and a second Dependabot PR for a module would merge
at a different time, leaving ``main`` red in between. The drift has to fail
the bump PR itself, which is what this check does.

The check is ``go mod tidy -diff`` in each module: it is the exact
question the failing CI step asks, it never writes to the working tree, and it
exits non-zero when a module is untidy.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_go_example_mod_tidy.py

Exits 0 if every module is tidy, 1 otherwise.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
# Each module with the CI job that its drift turns red.
GO_MODULES = {
    pathlib.Path("kubernetes-tests/lang_sdk/go_example"): "Kubernetes tests / K8S Lang-SDK",
    pathlib.Path("airflow-e2e-tests/go-test-bundle"): "Go SDK e2e test",
}
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


def format_report(module: pathlib.Path, job: str, returncode: int, output: str) -> tuple[int, str]:
    """Turn a ``go mod tidy -diff`` result for ``module`` into ``(exit_code, report)``."""
    if returncode == 0:
        return 0, f"OK: {module} is tidy against {GO_SDK_MODULE}."
    lines = [
        f"ERROR: {module} is not tidy.",
        "",
        f"It is a separate Go module that resolves the SDK via a `replace` onto {GO_SDK_MODULE},",
        "so it keeps its own copy of the SDK's indirect requirements. A dependency moved in",
        f"{GO_SDK_MODULE} without this module being re-tidied, which breaks the",
        f"{job!r} bundle build on every pull request once merged.",
        "",
        "Fix it in this PR by running:",
        "",
        f"    (cd {module} && go mod tidy)",
        "",
        "and committing the resulting go.mod / go.sum changes.",
        "",
        "`go mod tidy -diff` reported:",
        "",
        output or "(no output)",
    ]
    return 1, "\n".join(lines)


def main() -> int:
    for module in GO_MODULES:
        if not (REPO_ROOT / module / "go.mod").is_file():
            print(f"ERROR: {module}/go.mod not found. Has the module moved?")
            return 1
    if shutil.which("go") is None:
        if os.environ.get("CI"):
            print("ERROR: `go` is not on PATH but this is a CI run — the toolchain is required here.")
            return 1
        print("SKIPPED: `go` is not on PATH, cannot verify that the Go modules are tidy.")
        return 0
    exit_code = 0
    for module, job in GO_MODULES.items():
        returncode, output = run_tidy_diff(REPO_ROOT / module)
        module_exit_code, report = format_report(module, job, returncode, output)
        print(report)
        exit_code = max(exit_code, module_exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
