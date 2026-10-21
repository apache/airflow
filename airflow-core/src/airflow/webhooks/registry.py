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
"""The inbound webhooks to serve, from the ``[webhooks]`` configuration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, cast, get_args

from packaging.utils import InvalidName, canonicalize_name

from airflow._shared.webhook_naming import WEBHOOK_NAME_PATTERN
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.providers_manager import WEBHOOK_SOURCE_TYPE_PATTERN
from airflow.webhooks.base import StorePayload

if TYPE_CHECKING:
    from typing import Self

#: The webhook tag that metrics give requests to names that are not listed, so no webhook can have it.
UNLISTED_WEBHOOK_TAG = "other"
# retain_until is a TIMESTAMP on MySQL, which ends in January 2038. Without a bound, a large value would
# fail the insert of every delivery rather than the start of the process.
MAX_DELIVERY_RETENTION_DAYS = 365
# An inbound wait's deadline is a TIMESTAMP on MySQL too, so a wait's timeout is bounded the same way.
MAX_WAIT_TIMEOUT = timedelta(days=365)


def get_webhook_name_problem(name: str) -> str | None:
    """Return why ``name`` cannot name a webhook, or None if it can."""
    if ":" in name:
        return "it is qualified with a team, and this version of Airflow serves only webhooks without one"
    if not WEBHOOK_NAME_PATTERN.fullmatch(name):
        return (
            "it is not a webhook name: use 1 to 64 lowercase letters, digits, - and _, starting with a "
            "letter or a digit"
        )
    return None


def parse_webhook_names(value: str) -> frozenset[str]:
    """Parse ``[webhooks] names``; raise ``AirflowConfigException`` if it is invalid."""
    names: set[str] = set()
    for name in filter(None, (item.strip() for item in value.split(","))):
        if name == "*":
            raise AirflowConfigException("[webhooks] names cannot be *: list each webhook name to serve")
        if problem := get_webhook_name_problem(name):
            raise AirflowConfigException(f"[webhooks] names lists {name!r}, but {problem}")
        if name == UNLISTED_WEBHOOK_TAG:
            raise AirflowConfigException(
                f"[webhooks] names lists {name!r}, which is reserved: metrics tag requests to names that "
                "are not listed with it"
            )
        if name in names:
            raise AirflowConfigException(f"[webhooks] names lists {name!r} more than once")
        names.add(name)
    return frozenset(names)


def parse_source_type_owners(value: str) -> dict[str, str]:
    """
    Parse ``[webhooks] source_type_owners``; raise ``AirflowConfigException`` if it is invalid.

    :return: The package that serves each source type listed, normalized as ``ProvidersManager``
        normalizes package names.
    """
    owners: dict[str, str] = {}
    for item in filter(None, (item.strip() for item in value.split(","))):
        source_type, _, package_name = (part.strip() for part in item.partition("="))
        if not source_type or not package_name:
            raise AirflowConfigException(
                f"[webhooks] source_type_owners lists {item!r}; expected source_type=package"
            )
        # A pair that can never match a declaration would silently leave its source type unpinned.
        if not WEBHOOK_SOURCE_TYPE_PATTERN.fullmatch(source_type):
            raise AirflowConfigException(
                f"[webhooks] source_type_owners lists {source_type!r}, which is not a source type: use "
                "1 to 64 lowercase letters, digits and _, starting with a letter"
            )
        try:
            package_name = canonicalize_name(package_name, validate=True)
        except InvalidName:
            raise AirflowConfigException(
                f"[webhooks] source_type_owners lists {package_name!r}, which is not a package name"
            ) from None
        if source_type in owners:
            raise AirflowConfigException(
                f"[webhooks] source_type_owners lists source type {source_type!r} more than once"
            )
        owners[source_type] = package_name
    return owners


def _get_int_at_least(option: str, minimum: int) -> int:
    value = conf.getint("webhooks", option)
    if value < minimum:
        raise AirflowConfigException(f"[webhooks] {option} must be {minimum} or more, not {value}")
    return value


def _get_delivery_retention() -> timedelta:
    days = _get_int_at_least("delivery_retention", 1)
    if days > MAX_DELIVERY_RETENTION_DAYS:
        raise AirflowConfigException(
            f"[webhooks] delivery_retention must be {MAX_DELIVERY_RETENTION_DAYS} or less, not {days}"
        )
    return timedelta(days=days)


def get_default_wait_timeout() -> timedelta | None:
    """
    Return ``[webhooks] default_wait_timeout``, or None if it is not set.

    :raises AirflowConfigException: if it is set to less than 1 second, or to more than
        ``MAX_WAIT_TIMEOUT``.
    """
    if not conf.get("webhooks", "default_wait_timeout"):
        return None
    timeout = timedelta(seconds=_get_int_at_least("default_wait_timeout", 1))
    if timeout > MAX_WAIT_TIMEOUT:
        raise AirflowConfigException(
            f"[webhooks] default_wait_timeout must be {int(MAX_WAIT_TIMEOUT.total_seconds())} or less, "
            f"not {int(timeout.total_seconds())}"
        )
    return timeout


def get_early_delivery_ttl() -> timedelta:
    """Return ``[webhooks] early_delivery_ttl``; raise ``AirflowConfigException`` if it is below 1 second."""
    return timedelta(seconds=_get_int_at_least("early_delivery_ttl", 1))


def _get_store_payload() -> StorePayload:
    value = conf.get("webhooks", "store_payload")
    if value not in get_args(StorePayload):
        raise AirflowConfigException(
            f"[webhooks] store_payload must be one of {', '.join(get_args(StorePayload))}, not {value!r}"
        )
    return cast("StorePayload", value)


@dataclass(frozen=True, kw_only=True)
class WebhookRegistry:
    """
    The ``[webhooks]`` configuration, parsed and validated once rather than on every request.

    :param names: The names that ``POST /webhooks/<name>`` serves. Any other name is refused without
        looking up a connection.
    :param source_type_owners: The package that serves each source type listed, even if other packages
        declare it too.
    :param connection_cache_ttl: Seconds a lookup of a webhook's connection is reused for.
    :param max_body_bytes: The largest body a webhook accepts unless its connection sets its own limit.
    :param delivery_retention: How long a delivery is recorded for.
    :param rate_limit_per_minute: Verified deliveries per minute that this process takes for each
        webhook, or 0 for no limit.
    :param store_payload: What is kept of a delivery's payload when neither the webhook's connection nor
        its source type says.
    :param max_event_payload_bytes: The largest payload, as compact JSON, copied into a topic event, or 0
        for no limit.
    :param early_delivery_ttl: How long after it is received an early delivery can be claimed by a wait.
    :param early_delivery_max_per_webhook: The most early deliveries a webhook keeps claimable.
    """

    names: frozenset[str]
    source_type_owners: Mapping[str, str]
    connection_cache_ttl: int
    max_body_bytes: int
    delivery_retention: timedelta
    rate_limit_per_minute: int
    store_payload: StorePayload
    max_event_payload_bytes: int
    early_delivery_ttl: timedelta
    early_delivery_max_per_webhook: int

    @classmethod
    def from_config(cls) -> Self:
        """Read ``[webhooks]``; raise ``AirflowConfigException`` if it is invalid."""
        return cls(
            names=parse_webhook_names(conf.get("webhooks", "names")),
            source_type_owners=parse_source_type_owners(conf.get("webhooks", "source_type_owners")),
            connection_cache_ttl=_get_int_at_least("connection_cache_ttl", 0),
            max_body_bytes=_get_int_at_least("max_body_bytes", 1),
            delivery_retention=_get_delivery_retention(),
            rate_limit_per_minute=_get_int_at_least("rate_limit_per_minute", 0),
            store_payload=_get_store_payload(),
            max_event_payload_bytes=_get_int_at_least("max_event_payload_bytes", 0),
            early_delivery_ttl=get_early_delivery_ttl(),
            early_delivery_max_per_webhook=_get_int_at_least("early_delivery_max_per_webhook", 1),
        )
