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

from dataclasses import dataclass
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
_DELIVERY_OPTIONS = ("event_field", "event_header", "path_field", "delivery_id_field", "delivery_id_header")


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


@dataclass(frozen=True, kw_only=True)
class ConfiguredJsonSource(WebhookSource):
    """
    Base for services configured entirely from the connection extra.

    The event name comes from ``event_header`` or ``event_field``, the optional path from
    ``path_field``, and the optional delivery ID from ``delivery_id_header`` or ``delivery_id_field``.
    Fields are dotted paths into the payload (``data.type``). The attributes are ``event``, and
    ``path`` when the delivery has one.
    """

    event_field: str | None = None
    event_header: str | None = None
    path_field: str | None = None
    delivery_id_field: str | None = None
    delivery_id_header: str | None = None

    @staticmethod
    def read_delivery_options(source_type: str, extra: dict[str, Any]) -> dict[str, str | None]:
        """Read the options that say where a delivery names its event, path and ID."""
        options = {key: _read_text_option(extra, key, source_type=source_type) for key in _DELIVERY_OPTIONS}
        if (options["event_field"] is None) == (options["event_header"] is None):
            raise ValueError(f"{source_type} needs exactly one of event_field or event_header")
        if options["delivery_id_field"] is not None and options["delivery_id_header"] is not None:
            raise ValueError(f"{source_type} takes at most one of delivery_id_field or delivery_id_header")
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
        return WebhookDelivery(
            path=path or None,
            kinds=[(event,)],
            attributes=attributes,
            delivery_id=self.read_delivery_id(request, payload),
            payload=payload,
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
