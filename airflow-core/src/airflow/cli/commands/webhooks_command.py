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
"""Webhooks sub-commands."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from airflow.exceptions import AirflowConfigException, AirflowNotFoundException
from airflow.models.connection import AirflowSecretsBackendAccessDenied, Connection
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.webhooks.connections import CONN_ID_PREFIX, ConfiguredWebhook, ResolutionFailure, resolve_webhook
from airflow.webhooks.registry import WebhookRegistry, get_webhook_name_problem

DEFAULT_SIGNATURE_HEADER = "X-Signature"
MAX_ANSWER_BYTES = 1_048_576


def _check_webhook(name: str, registry: WebhookRegistry) -> ConfiguredWebhook | list[str]:
    """Return webhook ``name`` as the webhooks app would serve it, or what keeps it from being served."""
    if problem := get_webhook_name_problem(name):
        return [problem]
    problems = []
    if name not in registry.names:
        problems.append("[webhooks] names does not list it, so its deliveries are answered with 404")
    resolution = resolve_webhook(
        name, source_type_owners=registry.source_type_owners, default_max_body_bytes=registry.max_body_bytes
    )
    if isinstance(resolution, ConfiguredWebhook):
        return problems or resolution
    conn_id = f"{CONN_ID_PREFIX}{name}"
    if resolution.failure is ResolutionFailure.MISCONFIGURED:
        problems.append(f"connection {conn_id} is misconfigured: {resolution.reason}")
    elif resolution.failure is ResolutionFailure.NOT_FOUND and (
        team_name := Connection.get_team_name(conn_id)
    ):
        # resolve_webhook looks connections up without a team, so it does not find one of a team.
        problems.append(
            f"connection {conn_id} belongs to team {team_name}, and this version of Airflow serves only "
            "webhooks without a team"
        )
    else:
        problems.append(resolution.reason)
    return problems


@suppress_logs_and_warning
@providers_configuration_loaded
def check(args) -> None:
    """Check that webhooks can receive deliveries, and exit with 1 if any cannot."""
    try:
        registry = WebhookRegistry.from_config()
    except AirflowConfigException as e:
        raise SystemExit(str(e)) from e
    names = args.names or sorted(registry.names)
    if not names:
        raise SystemExit("[webhooks] names lists no webhooks to check")
    failed = False
    for name in names:
        result = _check_webhook(name, registry)
        if isinstance(result, ConfiguredWebhook):
            print(
                f"Webhook {name} can be served at /webhooks/{name} by airflow api-server --apps webhooks, "
                f"with source type {result.source_type} and bodies of up to {result.max_body_bytes} bytes"
            )
            continue
        failed = True
        for problem in result:
            print(f"Problem with webhook {name}: {problem}", file=sys.stderr)
    if failed:
        raise SystemExit(1)


class _NoRedirectHandler(HTTPRedirectHandler):
    """
    Hand a redirect back as the answer, as senders such as GitHub do, instead of following it.

    urllib would follow it as a GET that carries every header, the signature included, to whatever host
    the redirect names.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _read_body(data: str) -> bytes:
    if not data.startswith("@"):
        return data.encode()
    try:
        return Path(data[1:]).read_bytes()
    except OSError as e:
        raise SystemExit(f"Cannot read the body to send: {e}") from e


def _get_connection(conn_id: str) -> Connection | None:
    try:
        return Connection.get_connection_from_secrets(conn_id)
    except AirflowNotFoundException:
        return None
    except AirflowSecretsBackendAccessDenied as e:
        raise SystemExit(f"Cannot read connection {conn_id}: {e}") from e


def _build_auth_header(
    args, conn: Connection | None, extra: dict[str, Any], secret: str, body: bytes
) -> tuple[str, str]:
    """Return the header that proves the sender knows the secret, as a token or a signature."""
    token_header = extra.get("token_header")
    if token_header and args.signature_header is None and args.signature_prefix is None:
        return token_header, f"{extra.get('token_prefix') or ''}{secret}"
    signature_header = args.signature_header or extra.get("signature_header")
    if not signature_header:
        if conn is not None:
            raise SystemExit(
                f"Connection {conn.conn_id} sets no signature_header to send the signature in: pass "
                "--signature-header"
            )
        signature_header = DEFAULT_SIGNATURE_HEADER
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    signature = (
        base64.b64encode(digest).decode() if extra.get("signature_encoding") == "base64" else digest.hex()
    )
    signature_prefix = (
        args.signature_prefix if args.signature_prefix is not None else extra.get("signature_prefix")
    )
    return signature_header, f"{signature_prefix or ''}{signature}"


def _is_sendable_header(name: str, value: str) -> bool:
    if any(char in name + value for char in "\r\n"):
        return False
    try:
        name.encode("ascii")
        value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return True


def _send(request: Request) -> tuple[int, bytes]:
    """Send ``request`` and return the answer's status and body, whatever the status is."""
    try:
        response = build_opener(_NoRedirectHandler).open(request, timeout=30)
    except HTTPError as e:
        response = e
    with response:
        status = response.status
        answer = response.read(MAX_ANSWER_BYTES + 1)
    if len(answer) > MAX_ANSWER_BYTES:
        raise SystemExit(f"The answer, with status {status}, is longer than {MAX_ANSWER_BYTES} bytes")
    return status, answer


@suppress_logs_and_warning
@providers_configuration_loaded
def send_test(args) -> None:
    """Send a test delivery to a webhook, print the answer, and exit with 1 unless it is a 2xx."""
    # The opener also opens file: and ftp: URLs.
    if urlsplit(args.url).scheme not in ("http", "https"):
        raise SystemExit(f"--url must be an http or https URL, not {args.url!r}")
    body = _read_body(args.data)
    conn_id = f"{CONN_ID_PREFIX}{args.name}"
    conn = _get_connection(conn_id)
    secret = args.secret
    if secret is None:
        if conn is None:
            raise SystemExit(f"There is no connection {conn_id}: pass --secret")
        if not (secret := conn.password):
            raise SystemExit(f"Connection {conn_id} has no password: pass --secret")
    extra = conn.extra_dejson if conn is not None else {}

    auth_header, auth_value = _build_auth_header(args, conn, extra, secret, body)
    headers = {"Content-Type": "application/json", auth_header: auth_value}
    if args.delivery_id is not None:
        if not (delivery_id_header := extra.get("delivery_id_header")):
            raise SystemExit(
                f"Connection {conn_id} sets no delivery_id_header to send --delivery-id in: pass the header "
                "with --header"
            )
        headers[delivery_id_header] = args.delivery_id
    for header in args.header or ():
        header_name, separator, value = header.partition(":")
        if not separator or not header_name.strip():
            raise SystemExit(f"--header {header!r} is not of the form 'NAME: VALUE'")
        headers[header_name.strip()] = value.strip()
    for header_name, value in headers.items():
        if not _is_sendable_header(header_name, value):
            # The value is not shown: it can be the secret.
            raise SystemExit(
                f"Cannot send header {header_name!r}: a header name must be ASCII and a value Latin-1, "
                "without line breaks"
            )

    try:
        status, answer = _send(Request(args.url, data=body, headers=headers, method="POST"))
    except (OSError, ValueError, HTTPException) as e:
        raise SystemExit(f"Could not send the delivery to {args.url}: {e}") from e
    try:
        answer_body = json.loads(answer)
    except ValueError:
        answer_body = answer.decode(errors="replace")
    print(json.dumps({"status": status, "body": answer_body}))
    if not 200 <= status < 300:
        raise SystemExit(1)
