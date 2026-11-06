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
Issue Dag processor session, Job, and parsing tokens.

Only trusted provisioning and the API server issue these tokens. A Dag processor must never hold
the signing key, because the processor runs the code it parses.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, NamedTuple

from airflow._shared.timezones import timezone

if TYPE_CHECKING:
    from collections.abc import Collection
    from uuid import UUID

    from airflow.api_fastapi.auth.tokens import JWTGenerator


class IssuedToken(NamedTuple):
    """A token with the seconds left until it expires."""

    token: str
    expires_in: int


class ExpiredDagProcessorToken(ValueError):
    """The parent credential expired before a Job or parsing credential could be issued."""


def _get_lifetime(generator: JWTGenerator, parent_expiry: float, message: str) -> int:
    """Return whole seconds a child token may live: the signer's lifetime, capped by its parent's expiry."""
    lifetime = math.floor(min(generator.valid_for, parent_expiry - timezone.utcnow().timestamp()))
    if lifetime < 1:
        raise ExpiredDagProcessorToken(message)
    return lifetime


def generate_dag_processor_session_token(
    generator: JWTGenerator, *, session_id: UUID, bundle_names: Collection[str], valid_for: float
) -> str:
    """Return a ``dag_processor_session`` token, which a Dag processor exchanges for a Job token."""
    return generator.generate(
        extras={
            "sub": str(session_id),
            "scope": "dag_processor_session",
            "dag_bundles": sorted(bundle_names),
        },
        valid_for=valid_for,
    )


def generate_dag_processor_token(
    generator: JWTGenerator,
    *,
    session_id: UUID,
    job_id: int,
    bundle_names: Collection[str],
    session_expiry: float,
) -> IssuedToken:
    """Issue a Job credential that cannot outlive its provisioned session credential."""
    lifetime = _get_lifetime(generator, session_expiry, "Session credential has expired")
    token = generator.generate(
        extras={
            "sub": str(session_id),
            "scope": "dag_processor",
            "dag_bundles": sorted(bundle_names),
            "job_id": job_id,
        },
        valid_for=lifetime,
    )
    return IssuedToken(token, lifetime)


def generate_dag_parse_token(
    generator: JWTGenerator,
    *,
    session_id: UUID,
    job_id: int,
    attempt_id: UUID,
    bundle_name: str,
    relative_fileloc: str,
    processor_expiry: float,
) -> IssuedToken:
    """Issue a parsing credential that cannot outlive its parent Job credential."""
    lifetime = _get_lifetime(generator, processor_expiry, "Processor credential has expired")
    token = generator.generate(
        extras={
            "sub": str(attempt_id),
            "scope": "dag_parse",
            "session_id": str(session_id),
            "job_id": job_id,
            "dag_bundles": [bundle_name],
            "relative_fileloc": relative_fileloc,
        },
        valid_for=lifetime,
    )
    return IssuedToken(token, lifetime)
