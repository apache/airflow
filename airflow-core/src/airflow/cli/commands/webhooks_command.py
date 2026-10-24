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
from collections import defaultdict
from http.client import HTTPException
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from sqlalchemy import select

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowNotFoundException
from airflow.models.connection import AirflowSecretsBackendAccessDenied, Connection
from airflow.models.dag import DagModel
from airflow.models.dag_cancel_condition import DAG_LEVEL_TASK_ID, DagCancelCondition
from airflow.models.inbound_delivery import InboundDelivery
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import create_session
from airflow.webhooks.connections import CONN_ID_PREFIX
from airflow.webhooks.registry import WebhookRegistry, get_webhook_name_problem
from airflow.webhooks.status import get_webhook_status
from airflow.webhooks.topics import build_dag_has_team_clause

if TYPE_CHECKING:
    from collections.abc import Collection

    from sqlalchemy.orm import Session

DEFAULT_SIGNATURE_HEADER = "X-Signature"
MAX_ANSWER_BYTES = 1_048_576
#: How many of a webhook's latest deliveries ``check`` reads to tell whether its deliveries have
#: correlation keys.
CORRELATION_KEY_SAMPLE_SIZE = 100


def _get_cancel_conditions(*, session: Session) -> dict[str, list[DagCancelCondition]]:
    conditions: defaultdict[str, list[DagCancelCondition]] = defaultdict(list)
    for condition in session.scalars(
        select(DagCancelCondition).order_by(
            DagCancelCondition.webhook,
            DagCancelCondition.dag_id,
            DagCancelCondition.task_id,
            DagCancelCondition.condition_digest,
        )
    ):
        conditions[condition.webhook].append(condition)
    return conditions


def _find_dags_with_a_team(dag_ids: Collection[str], *, session: Session) -> set[str]:
    if not dag_ids or not conf.getboolean("core", "multi_team"):
        return set()
    return set(
        session.scalars(
            select(DagModel.dag_id).where(DagModel.dag_id.in_(dag_ids), build_dag_has_team_clause())
        )
    )


def _describe_values(values: list[str]) -> str:
    return values[0] if len(values) == 1 else f"one of {', '.join(values)}"


def _describe_cancel_condition(condition: DagCancelCondition) -> str:
    """Return a line that says which deliveries to the condition's webhook cancel what."""
    events = condition.events
    if events is None:
        deliveries = "any delivery"
    else:
        deliveries = f"{' or '.join(events)} deliveries"
    if condition.match:
        deliveries += " with " + ", ".join(
            f"{key} {_describe_values(values)}" for key, values in sorted(condition.match.items())
        )
    if condition.correlation_key_conf is not None:
        deliveries += f", and manual runs by conf key {condition.correlation_key_conf}"
    if condition.task_id == DAG_LEVEL_TASK_ID:
        return f"Webhook {condition.webhook} cancels the runs of Dag {condition.dag_id} on {deliveries}"
    return (
        f"Webhook {condition.webhook} sets task {condition.task_id} of Dag {condition.dag_id} to "
        f"{condition.cancel_state} on {deliveries}"
    )


def _get_correlation_key_note(name: str, *, session: Session) -> str | None:
    """Return a note if none of the webhook's latest deliveries has a correlation key, which cancel_on needs."""
    keys = session.scalars(
        select(InboundDelivery.correlation_key)
        .where(InboundDelivery.webhook == name)
        .order_by(InboundDelivery.received_at.desc())
        .limit(CORRELATION_KEY_SAMPLE_SIZE)
    ).all()
    if not keys or any(key is not None for key in keys):
        return None
    finding = (
        f"the only delivery recorded for webhook {name} has no correlation key"
        if len(keys) == 1
        else f"none of the {len(keys)} latest deliveries recorded for webhook {name} has a correlation key"
    )
    return (
        f"Note: {finding}, and cancel_on acts only on deliveries that have one; a generic source sets one "
        "with correlation_key_field"
    )


@suppress_logs_and_warning
@providers_configuration_loaded
def check(args) -> None:
    """
    Check that webhooks can receive deliveries, and exit with 1 if any cannot.

    Also list the Dags and tasks whose ``cancel_on`` names each webhook, and check each webhook that a
    ``cancel_on`` names even when ``[webhooks] names`` does not list it.
    """
    try:
        registry = WebhookRegistry.from_config()
    except AirflowConfigException as e:
        raise SystemExit(str(e)) from e
    failed = False
    with create_session() as session:
        conditions = _get_cancel_conditions(session=session)
        names = args.names or sorted(registry.names | conditions.keys())
        if not names:
            raise SystemExit("[webhooks] names lists no webhooks to check")
        team_dag_ids = _find_dags_with_a_team(
            {condition.dag_id for name in names for condition in conditions.get(name, ())},
            session=session,
        )
        for name in names:
            webhook_conditions = conditions.get(name, [])
            served = False
            if problem := get_webhook_name_problem(name):
                problems = [problem]
            else:
                status = get_webhook_status(name, registry=registry, session=session)
                problems = list(status.problems)
                if (webhook := status.webhook) is not None and not problems:
                    served = True
                    print(
                        f"Webhook {name} can be served at /webhooks/{name} by airflow api-server --apps "
                        f"webhooks, with source type {webhook.source_type}, bodies of up to "
                        f"{webhook.max_body_bytes} bytes and store_payload {webhook.store_payload}"
                    )
            problems.extend(
                f"Dag {dag_id} belongs to a team, and this version of Airflow records no cancel requests "
                "for Dags with a team"
                for dag_id in sorted({condition.dag_id for condition in webhook_conditions} & team_dag_ids)
            )
            if problems or not served:
                failed = True
            for problem in problems:
                print(f"Problem with webhook {name}: {problem}", file=sys.stderr)
            for condition in webhook_conditions:
                print(_describe_cancel_condition(condition))
            if webhook_conditions and (note := _get_correlation_key_note(name, session=session)):
                print(note)
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
