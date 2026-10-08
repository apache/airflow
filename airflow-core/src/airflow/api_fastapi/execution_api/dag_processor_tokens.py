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

from typing import TYPE_CHECKING

from airflow._shared.timezones import timezone

if TYPE_CHECKING:
    from collections.abc import Collection
    from uuid import UUID

    from airflow.api_fastapi.auth.tokens import JWTGenerator


class ExpiredDagProcessorToken(ValueError):
    """The parent credential expired before a Job or parsing credential could be issued."""


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
) -> str:
    """Issue a Job credential that cannot outlive its provisioned session credential."""
    remaining = session_expiry - timezone.utcnow().timestamp()
    if remaining <= 0:
        raise ExpiredDagProcessorToken("Session credential has expired")
    return generator.generate(
        extras={
            "sub": str(session_id),
            "scope": "dag_processor",
            "dag_bundles": sorted(bundle_names),
            "job_id": job_id,
        },
        valid_for=min(generator.valid_for, remaining),
    )


def generate_dag_parse_token(
    generator: JWTGenerator,
    *,
    session_id: UUID,
    job_id: int,
    attempt_id: UUID,
    bundle_name: str,
    relative_fileloc: str,
    processor_expiry: float,
) -> str:
    """Issue a parsing credential that cannot outlive its parent Job credential."""
    remaining = processor_expiry - timezone.utcnow().timestamp()
    if remaining <= 0:
        raise ExpiredDagProcessorToken("Processor credential has expired")
    return generator.generate(
        extras={
            "sub": str(attempt_id),
            "scope": "dag_parse",
            "session_id": str(session_id),
            "job_id": job_id,
            "dag_bundles": [bundle_name],
            "relative_fileloc": relative_fileloc,
        },
        valid_for=min(generator.valid_for, remaining),
    )
