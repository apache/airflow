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
Issue Dag processor session tokens.

Only trusted provisioning runs this: it needs the Execution API signing key, which a Dag processor
must never hold, because the processor runs the code it parses.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from typing import TYPE_CHECKING

from airflow.api_fastapi.execution_api.app import _jwt_generator

if TYPE_CHECKING:
    from collections.abc import Collection
    from pathlib import Path
    from uuid import UUID


def generate_dag_processor_token(*, session_id: UUID, bundle_names: Collection[str], valid_for: float) -> str:
    """Return a ``dag_processor`` token for the session, granting it the given Dag bundles."""
    return _jwt_generator().generate(
        extras={"sub": str(session_id), "scope": "dag_processor", "dag_bundles": sorted(bundle_names)},
        valid_for=valid_for,
    )


def write_token_file(path: Path, token: str) -> None:
    """Replace the token file in one step, so a processor rereading it never sees a partial token."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.write(token)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise
