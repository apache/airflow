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

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from airflow_e2e_tests.constants import AIRFLOW_ROOT_PATH, GO_BUILDER_IMAGE, GO_SDK_BIN_PATH

if TYPE_CHECKING:
    from collections.abc import Sequence


def run_go(
    args: Sequence[str | Path], *, module: Path, native: bool, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """
    Run ``go <args>`` in the Go module at *module*, with the host toolchain or in ``GO_BUILDER_IMAGE``.

    ``native`` uses the host ``go``, which CI provisions with restored module and build caches. Otherwise
    ``go`` runs in the pinned image, so a dev host needs no Go installed:

    * --user keeps build outputs owned by the current user (not root).
    * HOME points at a writable, gitignored dir under go-sdk/bin so the Go build
      and module caches persist between runs (first run downloads modules once;
      subsequent runs skip straight to compilation).

    The repository is mounted at ``/repo``, so *module* and every ``Path`` in *args* must be inside it.
    Output is captured, so concurrent builds do not interleave. CGO is disabled: the bundles are static,
    so the stock Airflow image runs them without a Go toolchain.
    """
    if native:
        return subprocess.run(
            ["go", *map(str, args)],
            cwd=module,
            env={**os.environ, "CGO_ENABLED": "0"},
            check=check,
            capture_output=True,
            text=True,
        )
    return subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            f"HOME={_get_container_path(GO_SDK_BIN_PATH)}/.home",
            "-e",
            "CGO_ENABLED=0",
            "-v",
            f"{AIRFLOW_ROOT_PATH}:/repo",
            "-w",
            _get_container_path(module),
            GO_BUILDER_IMAGE,
            "go",
            *(_get_container_path(arg) if isinstance(arg, Path) else arg for arg in args),
        ],
        check=check,
        capture_output=True,
        text=True,
    )


def _get_container_path(path: Path) -> str:
    return f"/repo/{path.relative_to(AIRFLOW_ROOT_PATH)}"
