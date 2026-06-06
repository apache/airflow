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
Mint and provision the bearer token the DAG processor presents to the API server (AIP-92).

The DAG processor parses (and forks) user code, so it must never hold the deployment signing key
or mint its own token. A *trusted* component runs the helpers here -- the deployment's provisioning
step (a Helm init container, a docker-compose init service) or ``airflow standalone`` -- mints the
token and writes it to ``[dag_processor] api_token_path``. The processor only ever reads that file
(re-reading it as it is rotated), so it carries a token without being able to forge one.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_args
from airflow.configuration import conf

log = logging.getLogger(__name__)

# The DAG processor is not a task instance. The Execution API accepts a non-task ``sub`` (its token
# ``id`` is then ``None``; only task-bound routes require a real id), so this is a plain identifier
# rather than a task-instance UUID.
DAG_PROCESSOR_TOKEN_SUBJECT = "dag-processor"


def mint_dag_processor_token(
    *,
    valid_for: int | None = None,
    make_secret_key_if_needed: bool = False,
) -> str:
    """
    Mint the bearer token the DAG processor presents to the API server.

    Trusted callers only. The token carries *both* the Execution and DAG Processing audiences (the
    processor calls both APIs) under :data:`DAG_PROCESSOR_TOKEN_SUBJECT`. Signing uses whatever
    ``get_signing_args`` resolves -- the symmetric ``[api_auth] jwt_secret`` or, where configured, an
    asymmetric private key -- so the same minting works for a single deployment or a JWKS-based
    control plane without change here.

    :param valid_for: token lifetime in seconds; defaults to ``[dag_processor] jwt_expiration_time``.
    :param make_secret_key_if_needed: generate a signing key if none is configured. Leave ``False``
        in real deployments (the key must be the shared one the API server validates with); only
        ``airflow standalone``, which materialises that shared key itself, passes ``True``.
    """
    audiences = [
        conf.get_mandatory_list_value("execution_api", "jwt_audience")[0],
        conf.get_mandatory_list_value("dag_processor", "jwt_audience")[0],
    ]
    generator = JWTGenerator(
        valid_for=valid_for if valid_for is not None else conf.getint("dag_processor", "jwt_expiration_time"),
        # A JWT ``aud`` may be a list; the generator's hint is single-audience, but the processor
        # presents this one token to both the Execution and DAG Processing APIs.
        audience=audiences,  # type: ignore[arg-type]
        issuer=conf.get("api_auth", "jwt_issuer", fallback=None),
        **get_signing_args(make_secret_key_if_needed=make_secret_key_if_needed),
    )
    return generator.generate({"sub": DAG_PROCESSOR_TOKEN_SUBJECT})


def provision_dag_processor_token_file(
    output: str | os.PathLike[str] | None = None,
    *,
    valid_for: int | None = None,
    make_secret_key_if_needed: bool = False,
) -> str:
    """
    Mint a DAG processor token and write it to ``output`` (or ``[dag_processor] api_token_path``).

    The token is written atomically (temp file then ``rename``) so the processor, which re-reads the
    file, never observes a half-written token. Re-run before the token expires to rotate it in place.

    :return: the path written.
    """
    target = output or conf.get("dag_processor", "api_token_path", fallback=None)
    if not target:
        raise ValueError(
            "No output path for the DAG processor token: pass output or set [dag_processor] api_token_path."
        )
    token = mint_dag_processor_token(valid_for=valid_for, make_secret_key_if_needed=make_secret_key_if_needed)
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(token)
    os.replace(tmp_path, path)
    log.info("Wrote DAG processor API token to %s", path)
    return str(path)
