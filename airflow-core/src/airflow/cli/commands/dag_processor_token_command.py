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
"""Dag processor token command."""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import time
from pathlib import Path

import uuid6

from airflow.api_fastapi.execution_api.app import create_jwt_generator
from airflow.api_fastapi.execution_api.dag_processor_tokens import generate_dag_processor_session_token
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import _load_bundle_config_snapshot
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


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


# Not wrapped in ``action_cli``: its audit logging writes to the metadata database, which a provisioning
# component that only holds the signing key cannot reach.
@providers_configuration_loaded
def dag_processor_token(args) -> None:
    """Write a Dag processor session token to a file, and with ``--rotate`` keep replacing it."""
    # Names only: provisioning runs where the signing key is, which may not have the bundle classes installed.
    configured = _load_bundle_config_snapshot().names
    bundle_names = set(args.bundle_name or configured)
    if unknown := bundle_names - configured:
        raise SystemExit(f"Bundles not found: {', '.join(sorted(unknown))}")

    valid_for = args.valid_for or conf.getint("execution_api", "jwt_expiration_time")
    token_file = Path(args.token_file)
    session_id = uuid6.uuid7()
    log.info("Issuing Dag processor session %s for bundles %s", session_id, ", ".join(sorted(bundle_names)))
    while True:
        token = generate_dag_processor_session_token(
            create_jwt_generator(), session_id=session_id, bundle_names=bundle_names, valid_for=valid_for
        )
        write_token_file(token_file, token)
        if not args.rotate:
            return
        time.sleep(valid_for / 2)
