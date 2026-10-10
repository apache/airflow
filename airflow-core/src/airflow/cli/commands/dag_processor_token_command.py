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
import stat
import tempfile
import time
from pathlib import Path
from uuid import UUID

import jwt
import uuid6

from airflow.api_fastapi.execution_api.app import create_jwt_generator
from airflow.api_fastapi.execution_api.dag_processor_tokens import generate_dag_processor_session_token
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import get_configured_bundle_names
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


def write_token_file(path: Path, token: str) -> None:
    """Atomically replace the token file, keeping its mode and group, or leave the old file untouched."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as tmp:
            try:
                existing = path.stat()
            except FileNotFoundError:
                existing = None
            if existing:
                os.fchmod(tmp.fileno(), stat.S_IMODE(existing.st_mode))
                os.fchown(tmp.fileno(), -1, existing.st_gid)
            tmp.write(token)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise


def get_session_id(token_file: Path, bundle_names: set[str]) -> UUID:
    """Reuse the session of the token already in the file, so a restart keeps the processors' Jobs."""
    with contextlib.suppress(OSError, KeyError, TypeError, ValueError, jwt.PyJWTError):
        # Unverified: the file is this command's own output.
        claims = jwt.decode(token_file.read_text(), options={"verify_signature": False})
        if claims["scope"] == "dag_processor_session" and set(claims["dag_bundles"]) == bundle_names:
            return UUID(claims["sub"])
    return uuid6.uuid7()


# Not wrapped in ``action_cli``: its audit logging writes to the metadata database, which a provisioning
# component that only holds the signing key cannot reach.
@providers_configuration_loaded
def dag_processor_token(args) -> None:
    """Write a Dag processor session token to a file, and with ``--rotate`` keep replacing it."""
    # Names only: provisioning runs where the signing key is, which may not have the bundle classes installed.
    configured = get_configured_bundle_names()
    bundle_names = set(args.bundle_name or configured)
    if unknown := bundle_names - configured:
        raise SystemExit(f"Bundles not found: {', '.join(sorted(unknown))}")

    valid_for = args.valid_for or conf.getint("execution_api", "jwt_expiration_time")
    token_file = Path(args.token_file)
    session_id = get_session_id(token_file, bundle_names)
    log.info("Issuing Dag processor session %s for bundles %s", session_id, ", ".join(sorted(bundle_names)))
    issued = False
    failures = 0
    while True:
        try:
            token = generate_dag_processor_session_token(
                create_jwt_generator(), session_id=session_id, bundle_names=bundle_names, valid_for=valid_for
            )
            write_token_file(token_file, token)
        except Exception:
            # Without a token to keep, or without rotation, the operator must see the failure.
            if not (args.rotate and issued):
                raise
            failures += 1
            log.exception("Could not rotate the session token; the previous one stays valid until it expires")
            time.sleep(min(2 ** min(failures, 10), valid_for / 4))
            continue
        issued = True
        failures = 0
        if not args.rotate:
            return
        time.sleep(valid_for / 2)
