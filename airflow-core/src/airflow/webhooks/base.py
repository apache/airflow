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
Base classes for inbound webhook source types.

A source type verifies deliveries from one kind of sender and describes them; naming, lookup and
recording are core's.

.. note:: |experimental|
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from typing import Self

    from starlette.datastructures import Headers, QueryParams


#: What is stored of a delivery's payload: all of it, what ``summarize`` keeps of it, or nothing.
StorePayload = Literal["full", "summary", "none"]


class WebhookVerificationError(Exception):
    """The request does not carry a valid signature or token for this webhook."""


@dataclass(frozen=True, kw_only=True)
class WebhookRequest:
    """
    A request to ``POST /webhooks/<name>``, as a source type sees it.

    :param method: The HTTP method.
    :param headers: The request headers; lookups ignore case.
    :param query: The query parameters.
    :param body: The body as received.
    """

    method: str
    headers: Headers
    query: QueryParams
    body: bytes


@dataclass(frozen=True, kw_only=True)
class WebhookDelivery:
    """
    A verified and parsed delivery.

    :param path: Where it happened, ``/``-separated from broad to narrow (``my-org/web``),
        or None if the sender has no such notion.
    :param kinds: What happened, each from broad to specific
        (``("pull_request", "labeled", "deploy-preview")``). Every prefix of every kind is a
        candidate name, so one delivery can be subscribed to at several levels.
    :param attributes: The values the source type declares, such as ``repository`` or ``action``,
        as a flat mapping to strings or integers. Leave out an attribute the delivery does not have;
        any other value raises ``TypeError``.
    :param delivery_id: The sender's id for the delivery, the same across its retries, or None.
    :param payload: The body as received. The connection's ``store_payload`` decides what is kept of it.
    :param correlation_key: What the delivery is about, such as a pull request or a job run, so that
        deliveries about the same thing can be found together. At most 255 characters.
    """

    path: str | None
    kinds: list[tuple[str, ...]]
    attributes: dict[str, str | int]
    delivery_id: str | None
    payload: dict[str, Any]
    correlation_key: str | None = None

    def __post_init__(self) -> None:
        for key, value in self.attributes.items():
            # bool is an int, but JSON stores it as true or false rather than as a number.
            if not isinstance(value, (str, int)) or isinstance(value, bool):
                raise TypeError(
                    f"Attribute {key!r} must be a string or an integer, not {type(value).__name__}"
                )


class WebhookSource(ABC):
    """
    Verifies and describes deliveries from one kind of sender.

    The methods run inside the API server on bytes from unauthenticated clients, and must not perform
    network I/O. Core builds one instance per connection and shares it across concurrent requests, so
    an instance must keep no per-request state.

    Any exception from ``verify`` is answered 401. A ``ValueError`` from ``parse`` or ``handshake`` is
    a malformed request, answered 400. Any other exception, including one from ``is_handshake``, is a
    bug, answered 500.

    .. note:: |experimental|
    """

    #: What is stored of a delivery's payload unless the connection's ``store_payload`` says otherwise;
    #: None leaves it to ``[webhooks] store_payload``.
    default_store_payload: ClassVar[StorePayload | None] = None

    #: Whether the sender's handshakes are unsigned. If so, ``is_handshake`` sees requests that nothing
    #: has verified, and ``handshake`` may only echo a value from the request.
    unsigned_handshake: ClassVar[bool] = False

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> Self:
        """Build from the ``webhook_<name>`` connection's extra; raise ``ValueError`` if it is invalid."""
        return cls()

    @abstractmethod
    def verify(self, request: WebhookRequest, secret: str) -> None:
        """Raise ``WebhookVerificationError`` unless the request was sent by the holder of ``secret``."""

    @abstractmethod
    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        """Describe a verified request; raise ``ValueError`` if it is malformed."""

    def is_handshake(self, request: WebhookRequest) -> bool:
        """
        Return whether the request checks the webhook's URL rather than delivering an event.

        It is asked after ``verify``, or before it if the source declares ``unsigned_handshake``, and
        then sees requests from anyone. Decide on headers or other cheap checks where the sender allows,
        and return False rather than raise for a request it cannot read.
        """
        return False

    def handshake(self, request: WebhookRequest) -> dict[str, Any]:
        """
        Return the JSON body that answers a handshake with status 200; nothing is recorded.

        Raise ``ValueError`` if the handshake is malformed.
        """
        return {}

    def summarize(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Return the parts of a payload that ``store_payload`` ``summary`` keeps; by default, all of it."""
        return payload


def _refuse_json_constant(name: str) -> Any:
    # Python's json accepts NaN and Infinity, which JSON does not, and MySQL refuses to store them.
    raise ValueError(f"JSON body contains {name}, which JSON does not allow")


def _parse_finite_float(text: str) -> float:
    # json reads a number too large for a float, such as 1e400, as inf without calling parse_constant,
    # and the JSON columns would then be given Infinity.
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("JSON body contains a number too large to store")
    return value


# Postgres stores neither U+0000 nor an unpaired surrogate in jsonb, and asset_event.extra has a jsonb
# index. json.loads joins paired surrogate escapes, so a surrogate left in a string is unpaired.
_UNSTORABLE_CHARACTER = re.compile("[\x00\ud800-\udfff]")
# MySQL refuses JSON nested more than 100 levels, and a topic event's extra holds the payload one level
# down. A fixed limit also answers the same on every Python version, whose json modules differ in how
# deep they can read and write.
_MAX_JSON_DEPTH = 64


def _refuse_unstorable_values(payload: dict[str, Any]) -> None:
    # A loop rather than recursion: the body may nest almost as deep as the recursion limit.
    pending: list[tuple[Any, int]] = [(payload, 1)]
    while pending:
        value, depth = pending.pop()
        if isinstance(value, (dict, list)):
            if depth > _MAX_JSON_DEPTH:
                raise ValueError(f"JSON body is nested too deeply: more than {_MAX_JSON_DEPTH} levels")
            children = [*value, *value.values()] if isinstance(value, dict) else value
            pending.extend((child, depth + 1) for child in children)
        elif isinstance(value, str) and (match := _UNSTORABLE_CHARACTER.search(value)):
            raise ValueError(f"JSON body contains U+{ord(match.group()):04X}, which a database cannot store")


def load_json_object(request: WebhookRequest) -> dict[str, Any]:
    """
    Return the request's body, which must be a JSON object; raise ``ValueError`` if it is not.

    Values a database cannot store are refused as well: NaN, Infinity, numbers too large for a float,
    U+0000, unpaired surrogates, and objects and arrays nested more than 64 levels deep.
    """
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type != "application/json":
        raise ValueError(
            f"Expected Content-Type application/json, got {content_type or 'none'}; "
            "configure the sender to deliver JSON"
        )
    try:
        payload = json.loads(
            request.body, parse_constant=_refuse_json_constant, parse_float=_parse_finite_float
        )
    except RecursionError:
        # json raises RecursionError rather than ValueError for nesting deeper than the interpreter
        # allows, which would otherwise answer a signed delivery with a 500.
        raise ValueError("JSON body is nested too deeply") from None
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    _refuse_unstorable_values(payload)
    return payload


def verify_hmac_sha256(
    received: str | None,
    secret: str,
    body: bytes,
    *,
    prefix: str = "",
    encoding: Literal["hex", "base64"] = "hex",
) -> None:
    """
    Raise ``WebhookVerificationError`` unless ``received`` is ``prefix`` and the body's HMAC-SHA256.

    :param received: The signature header's value, or None if it is missing.
    :param secret: The webhook's shared secret; an empty one is refused, since anyone can sign with it.
    :param body: The body as received.
    :param prefix: A scheme the sender writes before the digest, such as ``sha256=``.
    :param encoding: How the sender encodes the digest; hex digits are compared ignoring case.
    """
    if not secret:
        raise WebhookVerificationError("No shared secret is configured")
    if received is None or not received.startswith(prefix):
        raise WebhookVerificationError("Missing or malformed signature header")
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    received_digest = received[len(prefix) :]
    if encoding == "hex":
        expected, received_digest = digest.hex(), received_digest.lower()
    else:
        expected = base64.b64encode(digest).decode()
    # compare_digest raises TypeError on non-ASCII str, and header values are attacker-controlled.
    if not hmac.compare_digest(received_digest.encode(), expected.encode()):
        raise WebhookVerificationError("Signature does not match")


def verify_token(received: str | None, secret: str, *, prefix: str = "") -> None:
    """
    Raise ``WebhookVerificationError`` unless ``received`` is ``prefix`` and the shared secret.

    An empty secret is refused. A token proves who sent the request, not that the body is unmodified;
    integrity depends on TLS between the sender and the API server.
    """
    if not secret:
        raise WebhookVerificationError("No shared secret is configured")
    if received is None or not received.startswith(prefix):
        raise WebhookVerificationError("Missing or malformed token header")
    # Digests of equal length, since compare_digest's timing reveals whether the lengths differ.
    received_digest = hashlib.sha256(received[len(prefix) :].encode()).digest()
    if not hmac.compare_digest(received_digest, hashlib.sha256(secret.encode()).digest()):
        raise WebhookVerificationError("Token does not match")


def get_field(payload: dict[str, Any], dotted_path: str) -> Any:
    """Return the value at ``dotted_path`` (``issue.fields.status``), or None if there is none."""
    value: Any = payload
    for key in dotted_path.split("."):
        value = value.get(key) if isinstance(value, dict) else None
    return value


def get_text(payload: dict[str, Any], dotted_path: str) -> str | None:
    """
    Return the value at ``dotted_path`` if it is a non-empty string, and None otherwise.

    A delivery of an unexpected shape then gets fewer names, or a 400 if it lacks a field the source
    requires, rather than a 500.
    """
    value = get_field(payload, dotted_path)
    return value if isinstance(value, str) and value else None
