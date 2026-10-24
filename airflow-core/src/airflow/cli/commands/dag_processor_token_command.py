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
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import jwt
import uuid6

from airflow.api_fastapi.execution_api.app import create_jwt_generator
from airflow.api_fastapi.execution_api.dag_processor_tokens import generate_dag_processor_session_token
from airflow.cli.commands.dag_processor_token_kubernetes import (
    KubernetesSecretTokenError,
    KubernetesSecretTokenStore,
)
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import get_configured_bundle_names
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

if TYPE_CHECKING:
    from collections.abc import Callable

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
    with contextlib.suppress(OSError, ValueError):
        return get_session_id_from_token(token_file.read_text(), bundle_names)
    return uuid6.uuid7()


def get_session_id_from_token(token: str | None, bundle_names: set[str]) -> UUID:
    """Reuse the session in a previous provisioner's output for the same bundle grant."""
    if not token:
        return uuid6.uuid7()
    with contextlib.suppress(KeyError, TypeError, ValueError, jwt.PyJWTError):
        # Unverified: only trusted provisioners can write this file or Secret.
        claims = jwt.decode(token, options={"verify_signature": False})
        if claims["scope"] == "dag_processor_session" and set(claims["dag_bundles"]) == bundle_names:
            return UUID(claims["sub"])
    return uuid6.uuid7()


# Not wrapped in ``action_cli``: its audit logging writes to the metadata database, which a provisioning
# component that only holds the signing key cannot reach.
@providers_configuration_loaded
def dag_processor_token(args) -> None:
    """Publish a Dag processor session token, and with ``--rotate`` keep replacing it."""
    if bool(args.token_file) == bool(args.kubernetes_secret):
        raise SystemExit("Choose exactly one of --token-file or --kubernetes-secret")
    if bool(args.kubernetes_secret) != bool(args.kubernetes_namespace):
        raise SystemExit("--kubernetes-secret and --kubernetes-namespace must be supplied together")
    # Names only: provisioning runs where the signing key is, which may not have the bundle classes installed.
    configured = get_configured_bundle_names()
    bundle_names = set(args.bundle_name or configured)
    if unknown := bundle_names - configured:
        raise SystemExit(f"Bundles not found: {', '.join(sorted(unknown))}")

    valid_for = args.valid_for or conf.getint("execution_api", "jwt_expiration_time")
    if args.kubernetes_secret:
        with KubernetesSecretTokenStore(args.kubernetes_secret, args.kubernetes_namespace) as store:
            _run_token_rotation(
                bundle_names,
                valid_for,
                get_session=lambda: get_session_id_from_token(store.read_token(), bundle_names),
                publish_token=store.write_token,
                rotate=args.rotate,
                retry_initial_publication=True,
            )
        return

    token_file = Path(args.token_file)
    session_id = get_session_id(token_file, bundle_names)
    _run_token_rotation(
        bundle_names,
        valid_for,
        get_session=lambda: session_id,
        publish_token=partial(write_token_file, token_file),
        rotate=args.rotate,
    )


def _run_token_rotation(
    bundle_names: set[str],
    valid_for: int,
    *,
    get_session: Callable[[], UUID],
    publish_token: Callable[[str], None],
    rotate: bool,
    retry_initial_publication: bool = False,
) -> None:
    issued = False
    failures = 0
    while True:
        try:
            session_id = get_session()
            token = generate_dag_processor_session_token(
                create_jwt_generator(), session_id=session_id, bundle_names=bundle_names, valid_for=valid_for
            )
            publish_token(token)
        except Exception as exc:
            # A failed first Kubernetes PATCH may already have committed; re-read its session on retry.
            if not rotate or not (
                issued or (retry_initial_publication and isinstance(exc, KubernetesSecretTokenError))
            ):
                raise
            failures += 1
            log.exception("Could not publish the session token; any previous token keeps its original expiry")
            time.sleep(min(2 ** min(failures, 10), valid_for / 4))
            continue
        issued = True
        failures = 0
        log.info(
            "Published Dag processor session %s for bundles %s", session_id, ", ".join(sorted(bundle_names))
        )
        if not rotate:
            return
        time.sleep(valid_for / 2)
