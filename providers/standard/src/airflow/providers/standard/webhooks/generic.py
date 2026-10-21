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
Inbound webhook source types for services configured entirely from the connection extra.

.. note:: |experimental|
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.webhooks.base import (
        WebhookDelivery,
        WebhookRequest,
        WebhookSource,
        get_field,
        get_text,
        load_json_object,
        verify_hmac_sha256,
        verify_token,
    )
except ImportError:
    raise AirflowOptionalProviderFeatureException("Inbound webhook sources need Airflow 3.5+.")

if TYPE_CHECKING:
    from typing import Self

SignatureEncoding = Literal["hex", "base64"]
SIGNATURE_ENCODINGS = ("hex", "base64")
_DELIVERY_OPTIONS = (
    "event_field",
    "event_header",
    "path_field",
    "delivery_id_field",
    "delivery_id_header",
    "correlation_key_field",
)
#: The attributes these source types set themselves, which ``attribute_paths`` cannot set.
_OWN_ATTRIBUTES = ("event", "path")
# Core has no rule for attribute names; this one is strict, so it can be relaxed without breaking a
# connection.
_ATTRIBUTE_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}")
# Attributes are stored in a JSON column, and MySQL keeps an integer outside the signed 64-bit range as
# a double, or refuses the record when the integer is too large for a double.
_MIN_INTEGER_ATTRIBUTE = -(2**63)
_MAX_INTEGER_ATTRIBUTE = 2**63 - 1
#: The longest correlation key that ``WebhookDelivery`` allows.
_MAX_CORRELATION_KEY_LENGTH = 255


def _read_text_option(extra: dict[str, Any], key: str, *, source_type: str) -> str | None:
    """
    Return the connection extra's option ``key``, or None if it is not set.

    The connection form saves every field, as null when it is left untouched, so null and an empty
    string mean the option is not set. Connections defined in JSON or environment variables skip the
    form's type checks, so any other value that is not a string is refused with a ``ValueError``.
    """
    value = extra.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"{source_type} {key} must be a string, not {type(value).__name__}")
    return value


def _read_attribute_paths(extra: dict[str, Any], *, source_type: str) -> dict[str, str]:
    """
    Return the connection extra's ``attribute_paths``, or an empty mapping if it is not set.

    Raise ``ValueError`` unless it is a JSON object, or that object as JSON text as in a connection given
    as a URI, that maps attribute names, of at most 64 lowercase letters, digits and underscores starting
    with a letter, other than ``event`` and ``path``, to non-empty dotted paths.
    """
    attribute_paths = extra.get("attribute_paths")
    if attribute_paths is None or attribute_paths == "":
        return {}
    if isinstance(attribute_paths, str):
        try:
            attribute_paths = json.loads(attribute_paths)
        except ValueError:
            raise ValueError(
                f"{source_type} attribute_paths must be a JSON object of attribute names and dotted paths"
            ) from None
    if not isinstance(attribute_paths, dict):
        raise ValueError(
            f"{source_type} attribute_paths must be a JSON object of attribute names and dotted paths, "
            f"not {type(attribute_paths).__name__}"
        )
    for name, dotted_path in attribute_paths.items():
        if name in _OWN_ATTRIBUTES:
            raise ValueError(f"{source_type} attribute_paths cannot set {name!r}, which the source type sets")
        if not _ATTRIBUTE_NAME_PATTERN.fullmatch(name):
            raise ValueError(
                f"{source_type} attribute_paths has the attribute name {name!r}, which does not match "
                f"{_ATTRIBUTE_NAME_PATTERN.pattern!r}"
            )
        if not isinstance(dotted_path, str) or not dotted_path:
            raise ValueError(f"{source_type} attribute_paths must map {name!r} to a dotted path")
    return dict(attribute_paths)


def _get_text_or_integer(payload: dict[str, Any], dotted_path: str) -> str | int | None:
    """Return the value at ``dotted_path`` if it is a non-empty string or an integer, and None otherwise."""
    value = get_field(payload, dotted_path)
    # bool is an int, but a sender writes it as true or false rather than as a number.
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return value if isinstance(value, str) and value else None


def _get_attribute_value(payload: dict[str, Any], dotted_path: str) -> str | int | None:
    """Return the value at ``dotted_path`` if it is a non-empty string or a signed 64-bit integer."""
    value = _get_text_or_integer(payload, dotted_path)
    if isinstance(value, int) and not _MIN_INTEGER_ATTRIBUTE <= value <= _MAX_INTEGER_ATTRIBUTE:
        return None
    return value


@dataclass(frozen=True, kw_only=True)
class ConfiguredJsonSource(WebhookSource):
    """
    Base for services configured entirely from the connection extra.

    The event name comes from ``event_header`` or ``event_field``, the optional path from
    ``path_field``, and the optional delivery ID from ``delivery_id_header`` or ``delivery_id_field``.
    Fields are dotted paths into the payload (``data.type``). The attributes are ``event``, ``path``
    when the delivery has one, and each attribute that ``attribute_paths`` names, from the field it maps
    the name to, when that field is a non-empty string or a signed 64-bit integer. The optional
    correlation key is the value of ``correlation_key_field`` as a string, if that field is a non-empty
    string or an integer of any size and the string has at most 255 characters.
    """

    event_field: str | None = None
    event_header: str | None = None
    path_field: str | None = None
    delivery_id_field: str | None = None
    delivery_id_header: str | None = None
    attribute_paths: Mapping[str, str] = field(default_factory=dict)
    correlation_key_field: str | None = None

    @staticmethod
    def read_delivery_options(source_type: str, extra: dict[str, Any]) -> dict[str, Any]:
        """Read the options that say where a delivery names its event, path, ID, attributes and key."""
        options: dict[str, Any] = {
            key: _read_text_option(extra, key, source_type=source_type) for key in _DELIVERY_OPTIONS
        }
        if (options["event_field"] is None) == (options["event_header"] is None):
            raise ValueError(f"{source_type} needs exactly one of event_field or event_header")
        if options["delivery_id_field"] is not None and options["delivery_id_header"] is not None:
            raise ValueError(f"{source_type} takes at most one of delivery_id_field or delivery_id_header")
        options["attribute_paths"] = _read_attribute_paths(extra, source_type=source_type)
        return options

    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        payload = load_json_object(request)
        if self.event_header:
            event = request.headers.get(self.event_header)
        else:
            event = get_text(payload, self.event_field or "")
        if not event:
            raise ValueError(f"No event name in {self.event_header or self.event_field!r}")
        path = get_field(payload, self.path_field) if self.path_field else None
        if path is not None and not isinstance(path, str):
            raise ValueError(f"{self.path_field!r} is not a string")
        attributes: dict[str, str | int] = {"event": event}
        if path:
            attributes["path"] = path
        for name, dotted_path in self.attribute_paths.items():
            if (value := _get_attribute_value(payload, dotted_path)) is not None:
                attributes[name] = value
        return WebhookDelivery(
            path=path or None,
            kinds=[(event,)],
            attributes=attributes,
            delivery_id=self.read_delivery_id(request, payload),
            payload=payload,
            correlation_key=self.find_correlation_key(payload),
        )

    def read_delivery_id(self, request: WebhookRequest, payload: dict[str, Any]) -> str | None:
        """Return the delivery ID from the configured header or field, or None if none is configured."""
        if self.delivery_id_header:
            return request.headers.get(self.delivery_id_header)
        if not self.delivery_id_field:
            return None
        delivery_id = get_field(payload, self.delivery_id_field)
        # Some services number their deliveries.
        if isinstance(delivery_id, int) and not isinstance(delivery_id, bool):
            return str(delivery_id)
        if delivery_id is not None and not isinstance(delivery_id, str):
            raise ValueError(f"{self.delivery_id_field!r} is not a string or an integer")
        return delivery_id

    def find_correlation_key(self, payload: dict[str, Any]) -> str | None:
        """Return the value of ``correlation_key_field`` as a string, or None if it cannot be a key."""
        if not self.correlation_key_field:
            return None
        value = _get_text_or_integer(payload, self.correlation_key_field)
        if value is None:
            return None
        key = str(value)
        # Core answers a longer key with a 500; a delivery is better served without a key than refused.
        return key if len(key) <= _MAX_CORRELATION_KEY_LENGTH else None


@dataclass(frozen=True, kw_only=True)
class HmacJsonSource(ConfiguredJsonSource):
    """
    Any service that signs the raw body with HMAC-SHA256.

    For example::

        {"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event"}

    ``signature_prefix`` strips a scheme such as ``v1=``; ``signature_encoding`` is ``hex``
    (default) or ``base64``.
    """

    signature_header: str
    signature_prefix: str = ""
    signature_encoding: SignatureEncoding = "hex"

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> Self:
        signature_header = _read_text_option(extra, "signature_header", source_type="hmac_json")
        if signature_header is None:
            raise ValueError("hmac_json needs signature_header in the connection extra")
        encoding = _read_text_option(extra, "signature_encoding", source_type="hmac_json") or "hex"
        if encoding not in SIGNATURE_ENCODINGS:
            raise ValueError(f"hmac_json signature_encoding must be one of {SIGNATURE_ENCODINGS}")
        return cls(
            signature_header=signature_header,
            signature_prefix=_read_text_option(extra, "signature_prefix", source_type="hmac_json") or "",
            signature_encoding=cast("SignatureEncoding", encoding),
            **cls.read_delivery_options("hmac_json", extra),
        )

    def verify(self, request: WebhookRequest, secret: str) -> None:
        verify_hmac_sha256(
            request.headers.get(self.signature_header),
            secret,
            request.body,
            prefix=self.signature_prefix,
            encoding=self.signature_encoding,
        )


@dataclass(frozen=True, kw_only=True)
class TokenHeaderSource(ConfiguredJsonSource):
    """
    Any service that can only send the shared secret in a header it lets you configure.

    For example::

        {
            "source_type": "token_header",
            "token_header": "Authorization",
            "token_prefix": "Bearer ",
            "event_field": "event_type",
        }
    """

    token_header: str
    token_prefix: str = ""

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> Self:
        token_header = _read_text_option(extra, "token_header", source_type="token_header")
        if token_header is None:
            raise ValueError("token_header needs token_header in the connection extra")
        return cls(
            token_header=token_header,
            token_prefix=_read_text_option(extra, "token_prefix", source_type="token_header") or "",
            **cls.read_delivery_options("token_header", extra),
        )

    def verify(self, request: WebhookRequest, secret: str) -> None:
        verify_token(request.headers.get(self.token_header), secret, prefix=self.token_prefix)
