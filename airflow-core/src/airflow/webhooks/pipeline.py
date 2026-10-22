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
"""The steps an inbound webhook delivery passes through, from its name to the answer it gets."""

from __future__ import annotations

import hashlib
import json
import math
import threading
from collections.abc import AsyncIterable, Callable, Hashable
from dataclasses import dataclass, replace
from datetime import timedelta
from http import HTTPStatus
from time import monotonic
from typing import TYPE_CHECKING, Any, cast

import structlog
import uuid6
from sqlalchemy import inspect, select, update
from sqlalchemy.exc import (
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    TimeoutError as PoolTimeoutError,
)
from starlette.requests import ClientDisconnect

from airflow._shared.observability.metrics import stats
from airflow._shared.timezones import timezone
from airflow._shared.webhook_naming import build_candidate_names
from airflow.models.inbound_delivery import (
    MAX_CORRELATION_KEY_LENGTH,
    MAX_DELIVERY_ID_LENGTH,
    DeliveryOutcome,
    InboundDelivery,
)
from airflow.models.log import Log
from airflow.utils.session import create_session
from airflow.webhooks.base import WebhookVerificationError
from airflow.webhooks.connections import ConfiguredWebhook, ResolutionFailure, WebhookConnectionCache
from airflow.webhooks.registry import UNLISTED_WEBHOOK_TAG
from airflow.webhooks.topics import record_topic_events

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy import CursorResult, Row
    from sqlalchemy.orm import Session

    from airflow.webhooks.base import WebhookDelivery, WebhookRequest
    from airflow.webhooks.registry import WebhookRegistry

log = structlog.get_logger(__name__)

#: The outcome answered to a copy of a delivery that another copy recorded; it is never stored.
DUPLICATE = "duplicate"
#: The outcomes of deliveries that recorded nothing, which are processed again when redelivered.
_REPROCESSED_OUTCOMES = (DeliveryOutcome.NO_SUBSCRIBERS, DeliveryOutcome.DAG_PAUSED)
RETRY_AFTER_SECONDS = 30
# The database is down, unreachable or locked, or the pool has no connection to give. SQLite reports a
# missing table as an OperationalError too.
_UNAVAILABLE_STORAGE_ERRORS = (OperationalError, InterfaceError, PoolTimeoutError)


class _RecordChangedMeanwhile(Exception):
    """
    The record of a redelivered delivery changed between this copy's lookup and its update.

    Another copy acted on it, or ``airflow db clean`` removed it.
    """


class DeliveryRejected(Exception):
    """
    The request is refused, and answered with ``status`` and ``detail``.

    :param status: The status to answer with.
    :param detail: What the sender is told: what is wrong with its request, never with the deployment.
    :param retry_after: Seconds after which the sender may send the request again, if it should.
    """

    def __init__(self, status: HTTPStatus, detail: str, *, retry_after: int | None = None) -> None:
        super().__init__(f"{status.value} {detail}")
        self.status = status
        self.detail = detail
        self.retry_after = retry_after


class WebhookUnavailable(DeliveryRejected):
    """
    The delivery cannot be served for now, because of the deployment rather than the request.

    Its connection is misconfigured, or a secrets backend or the metadata database fails. The answer is
    the same whatever the cause, so that the sender learns only that it may retry.
    """

    def __init__(self) -> None:
        super().__init__(
            HTTPStatus.SERVICE_UNAVAILABLE, "Temporarily unavailable", retry_after=RETRY_AFTER_SECONDS
        )


@dataclass(frozen=True)
class DeliveryAnswer:
    """What a request that is not refused is answered with."""

    status: HTTPStatus
    body: dict[str, Any]


class LogThrottle:
    """
    Lets a message through at most once per ``interval`` seconds for each key.

    Anyone can send requests that are refused, and logging each one would let them flood the log. Keys
    must come from a bounded set, such as the listed webhook names, never from the request.
    """

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._logged_at: dict[Hashable, float] = {}
        self._lock = threading.Lock()

    def should_log(self, key: Hashable) -> bool:
        """Return whether a message with this key may be logged now, and if so, count it as logged."""
        now = monotonic()
        with self._lock:
            if now < self._logged_at.get(key, -math.inf) + self._interval:
                return False
            self._logged_at[key] = now
            return True


@dataclass
class TokenBucket:
    """The deliveries one webhook can still make in this process, refilled continuously to the limit."""

    tokens: float
    updated_at: float


def find_delivery(
    webhook_name: str, delivery_id: str | None, body_digest: str, *, session: Session
) -> Row[tuple[UUID, str]] | None:
    """
    Return the id and outcome of the recorded delivery that a delivery repeats, if any.

    A repeat has the same id and body: senders send the id in a header that no signature covers, so a
    captured body sent again under another delivery's id is not taken for that delivery.
    """
    if delivery_id is None:
        return None
    return session.execute(
        select(InboundDelivery.id, InboundDelivery.outcome).where(
            InboundDelivery.webhook == webhook_name,
            InboundDelivery.delivery_id == delivery_id,
            InboundDelivery.body_digest == body_digest,
        )
    ).one_or_none()


def _build_audit_log(webhook_name: str, delivery_id: str | None, names: list[str]) -> Log:
    """Return the audit log entry of a delivery that recorded topic events on ``names``."""
    return Log(
        event="webhook_delivery",
        owner=f"webhook:{webhook_name}",
        owner_display_name=f"Webhook {webhook_name}",
        extra=json.dumps({"delivery_id": delivery_id, "names": names, "outcome": DeliveryOutcome.ACCEPTED}),
        team_name=None,
    )


def record_delivery(
    webhook: ConfiguredWebhook,
    delivery: WebhookDelivery,
    *,
    body_digest: str,
    retention: timedelta,
    max_event_payload_bytes: int,
) -> str:
    """
    Record a verified delivery and its topic events in one transaction, and return its outcome.

    A delivery that repeats one that acted is a duplicate. One that repeats a delivery that recorded
    nothing is processed again, without a second row, so that a Dag that subscribed or was unpaused
    since receives it. A copy that another request recorded or acted on between this one's lookup and
    its commit is a duplicate, unless no copy has acted though this one could: the record was removed,
    or the copy that got there first found no Dag to act for. Then this copy is processed again, once,
    and is answered with 503 if that loses a race too, so that the sender retries.
    """
    candidates = build_candidate_names(webhook.name, delivery.path, delivery.kinds)
    for _ in range(2):
        callbacks: list[Callable[[], None]] = []
        acted = False
        try:
            with create_session(scoped=False) as session:
                original = find_delivery(webhook.name, delivery.delivery_id, body_digest, session=session)
                if original is not None and original.outcome not in _REPROCESSED_OUTCOMES:
                    return DUPLICATE
                # Chosen before the row is written, since every topic event carries it.
                record_id = uuid6.uuid7() if original is None else original.id
                topic_events = record_topic_events(
                    webhook,
                    delivery,
                    candidates=candidates,
                    inbound_delivery_id=record_id,
                    max_event_payload_bytes=max_event_payload_bytes,
                    callback_sink=callbacks,
                    session=session,
                )
                acted = topic_events.outcome is DeliveryOutcome.ACCEPTED
                recorded: dict[str, Any] = {
                    "candidates": candidates,
                    "payload": topic_events.payload,
                    "outcome": topic_events.outcome,
                    "actions": {"asset_events": topic_events.asset_events} if acted else None,
                }
                if original is None:
                    received_at = timezone.utcnow()
                    session.add(
                        InboundDelivery(
                            id=record_id,
                            webhook=webhook.name,
                            delivery_id=delivery.delivery_id,
                            body_digest=body_digest,
                            received_at=received_at,
                            retain_until=received_at + retention,
                            source_type=webhook.source_type,
                            attributes=delivery.attributes,
                            correlation_key=delivery.correlation_key,
                            **recorded,
                        )
                    )
                elif topic_events.outcome != original.outcome:
                    # Matches nothing if another copy has acted on the row, once that copy has committed.
                    result = session.execute(
                        update(InboundDelivery)
                        .where(
                            InboundDelivery.id == original.id,
                            InboundDelivery.outcome.in_(_REPROCESSED_OUTCOMES),
                        )
                        .values(**recorded)
                    )
                    if cast("CursorResult", result).rowcount != 1:
                        raise _RecordChangedMeanwhile
                if acted:
                    names = [asset_event["asset"] for asset_event in topic_events.asset_events]
                    session.add(_build_audit_log(webhook.name, delivery.delivery_id, names))
        except (IntegrityError, _RecordChangedMeanwhile) as e:
            # Rolling back undid what this copy recorded.
            with create_session(scoped=False) as session:
                original = find_delivery(webhook.name, delivery.delivery_id, body_digest, session=session)
            if original is None and isinstance(e, IntegrityError):
                raise
            if original is not None and (original.outcome not in _REPROCESSED_OUTCOMES or not acted):
                return DUPLICATE
            # No copy has acted on the delivery: its record was removed, or the copy that got there
            # first recorded nothing where this one found a Dag to act for.
            continue
        # Only once committed, so that no listener hears of an event that was rolled back.
        for callback in callbacks:
            callback()
        return topic_events.outcome
    raise WebhookUnavailable


def _has_delivery_table() -> bool:
    with create_session(scoped=False) as session:
        return inspect(session.connection()).has_table(
            InboundDelivery.__tablename__, schema=InboundDelivery.metadata.schema
        )


def _is_storage_unavailable(error: Exception) -> bool:
    if isinstance(error, ProgrammingError):
        # PostgreSQL and MySQL report a missing table this way, which retrying once airflow db migrate
        # has run fixes; any other ProgrammingError is a bug.
        try:
            return not _has_delivery_table()
        except _UNAVAILABLE_STORAGE_ERRORS:
            return True
    return isinstance(error, _UNAVAILABLE_STORAGE_ERRORS)


def _is_declared_over_limit(content_length: str | None, limit: int) -> bool:
    if not content_length or not content_length.isdecimal():
        return False
    digits = content_length.lstrip("0")
    # int() refuses a string of more than 4300 digits, so a value with more digits than the limit is
    # compared by its length alone.
    return len(digits) > len(str(limit)) or int(digits or "0") > limit


def _find_verification_error(webhook: ConfiguredWebhook, request: WebhookRequest) -> Exception | None:
    try:
        webhook.source.verify(request, webhook.secret)
    except Exception as e:
        return e
    return None


class DeliveryPipeline:
    """
    Serves deliveries to the webhooks that a registry lists, and keeps this process's state for them.

    A request passes, in order: its name, the webhook's connection, the body limit, verification, the
    handshake, the rate limit, parsing, and one transaction that records the delivery and its topic
    events. Verification is skipped only for a handshake that the source type declares unsigned.
    """

    def __init__(self, registry: WebhookRegistry) -> None:
        self._registry = registry
        self._connections = WebhookConnectionCache(registry)
        self._log_throttle = LogThrottle(interval=60)
        # One per webhook that has had a delivery verified, so only listed names ever get one.
        self._rate_limit_buckets: dict[str, TokenBucket] = {}
        self._rate_limit_lock = threading.Lock()

    def get_webhook(self, name: str) -> ConfiguredWebhook:
        """
        Return webhook ``name`` as its connection configures it.

        Raise ``DeliveryRejected`` if the webhook is not served. This blocks while the connection is read,
        so call it from a worker thread.
        """
        resolution = self._connections.get(name)
        if isinstance(resolution, ConfiguredWebhook):
            return resolution
        if resolution.failure is ResolutionFailure.NOT_FOUND:
            if name in self._registry.names and self._log_throttle.should_log((name, "no_connection")):
                log.warning(
                    "Refused a webhook delivery: the webhook has no connection",
                    webhook=name,
                    reason=resolution.reason,
                )
            raise DeliveryRejected(HTTPStatus.NOT_FOUND, "Unknown webhook")
        # The cache logs a misconfigured connection itself, once until it changes.
        if resolution.failure is ResolutionFailure.UNAVAILABLE and self._log_throttle.should_log(
            (name, "secrets_backend")
        ):
            log.error(
                "Refused a webhook delivery: its connection could not be read",
                webhook=name,
                reason=resolution.reason,
            )
        raise WebhookUnavailable

    async def read_body(
        self, webhook: ConfiguredWebhook, content_length: str | None, chunks: AsyncIterable[bytes]
    ) -> bytes:
        """
        Return the request's body.

        Raise ``DeliveryRejected`` once the body is known to be over the limit, or if the client
        disconnects before sending all of it.
        """
        if _is_declared_over_limit(content_length, webhook.max_body_bytes):
            raise self._reject_too_large(webhook)
        body = bytearray()
        try:
            async for chunk in chunks:
                body += chunk
                if len(body) > webhook.max_body_bytes:
                    raise self._reject_too_large(webhook)
        except ClientDisconnect:
            # Not logged: no one is left to read the answer, and anyone can make a request this way.
            raise DeliveryRejected(HTTPStatus.BAD_REQUEST, "Incomplete body") from None
        return bytes(body)

    def process(self, webhook: ConfiguredWebhook, request: WebhookRequest) -> DeliveryAnswer:
        """
        Verify, parse and record a delivery whose body is within the webhook's limit.

        This blocks while the delivery is recorded, so call it from a worker thread.
        """
        if webhook.source.unsigned_handshake and webhook.source.is_handshake(request):
            return self._answer_handshake(webhook, request)
        webhook = self._verify(webhook, request)
        if not webhook.source.unsigned_handshake and webhook.source.is_handshake(request):
            return self._answer_handshake(webhook, request)
        self._limit_rate(webhook)
        delivery = self._parse(webhook, request)
        body_digest = hashlib.sha256(request.body).hexdigest()
        timer = stats.timer("webhooks.delivery_duration", tags={"webhook": webhook.name})
        timer.start()
        try:
            outcome = record_delivery(
                webhook,
                delivery,
                body_digest=body_digest,
                retention=self._registry.delivery_retention,
                max_event_payload_bytes=self._registry.max_event_payload_bytes,
            )
        except Exception as e:
            # Not timed: during an outage, recordings fail after as long as the pool or database takes.
            timer.stop(send=False)
            if not _is_storage_unavailable(e):
                raise
            if self._log_throttle.should_log((webhook.name, "storage")):
                log.error(
                    "Could not record a webhook delivery; check that the metadata database is reachable "
                    "and that airflow db migrate has run",
                    webhook=webhook.name,
                    exc_info=e,
                )
            raise WebhookUnavailable from e
        timer.stop()
        stats.incr("webhooks.deliveries", tags={"webhook": webhook.name, "outcome": outcome})
        status = HTTPStatus.OK if outcome == DUPLICATE else HTTPStatus.ACCEPTED
        return DeliveryAnswer(status, {"delivery_id": delivery.delivery_id, "outcome": outcome})

    def count_rejection(self, name: str | None, status: int) -> None:
        """Count a request that is refused with ``status``, to webhook ``name`` or to no webhook's path."""
        webhook = self._get_listed_name(name) or UNLISTED_WEBHOOK_TAG
        stats.incr("webhooks.rejections", tags={"webhook": webhook, "status": str(status)})

    def log_unhandled_error(self, name: str | None, error: Exception) -> None:
        """Log an error that a request to webhook ``name`` is answered 500 for, at most once a minute."""
        # Requests that nothing has verified can reach a bug too, such as one in the is_handshake of a
        # source type with unsigned handshakes.
        webhook = self._get_listed_name(name)
        if self._log_throttle.should_log((webhook, "error")):
            log.error("Unhandled error in the webhooks app", webhook=webhook, exc_info=error)

    def _get_listed_name(self, name: str | None) -> str | None:
        """
        Return ``name`` if ``[webhooks] names`` lists it, or else None.

        Names that are not listed are chosen by whoever sends the request, so they must share one log key
        and one metric tag value rather than let anyone add keys or series.
        """
        return name if name in self._registry.names else None

    def _verify(self, webhook: ConfiguredWebhook, request: WebhookRequest) -> ConfiguredWebhook:
        """Return the webhook that the request verifies against; raise ``DeliveryRejected`` if none does."""
        error = _find_verification_error(webhook, request)
        # The secret may have been changed in the connection since it was cached.
        if error is not None and (reread := self._connections.reread(webhook)) is not None:
            webhook = reread
            error = _find_verification_error(webhook, request)
        if error is None:
            return webhook
        if self._log_throttle.should_log((webhook.name, "verification")):
            if isinstance(error, WebhookVerificationError):
                log.warning(
                    "Refused a webhook delivery that failed verification",
                    webhook=webhook.name,
                    reason=str(error),
                )
            else:
                log.error(
                    "Refused a webhook delivery: verifying it raised", webhook=webhook.name, exc_info=error
                )
        # Answered like a failed verification even when the verifier raised, so that what made it raise
        # cannot be told from outside.
        raise DeliveryRejected(HTTPStatus.UNAUTHORIZED, "Invalid webhook credentials") from error

    def _reject_too_large(self, webhook: ConfiguredWebhook) -> DeliveryRejected:
        if self._log_throttle.should_log((webhook.name, "too_large")):
            log.warning(
                "Refused a webhook delivery: its body is too large",
                webhook=webhook.name,
                max_body_bytes=webhook.max_body_bytes,
            )
        return DeliveryRejected(
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE, f"Body exceeds {webhook.max_body_bytes} bytes"
        )

    def _reject_malformed(self, webhook: ConfiguredWebhook, reason: str) -> DeliveryRejected:
        if self._log_throttle.should_log((webhook.name, "malformed")):
            log.warning("Refused a malformed webhook delivery", webhook=webhook.name, reason=reason)
        return DeliveryRejected(HTTPStatus.BAD_REQUEST, reason)

    def _answer_handshake(self, webhook: ConfiguredWebhook, request: WebhookRequest) -> DeliveryAnswer:
        try:
            body = webhook.source.handshake(request)
        except ValueError as e:
            raise self._reject_malformed(webhook, str(e)) from e
        return DeliveryAnswer(HTTPStatus.OK, body)

    def _limit_rate(self, webhook: ConfiguredWebhook) -> None:
        """
        Refuse a verified delivery over ``rate_limit_per_minute``, before it is parsed or recorded.

        Only verified deliveries count: a limit that unsigned requests could use up would let anyone have
        the sender's own deliveries refused, and senders such as GitLab disable a webhook whose
        deliveries keep failing.
        """
        if (limit := self._registry.rate_limit_per_minute) <= 0:
            return
        now = monotonic()
        with self._rate_limit_lock:
            bucket = self._rate_limit_buckets.setdefault(
                webhook.name, TokenBucket(tokens=limit, updated_at=now)
            )
            bucket.tokens = min(limit, bucket.tokens + (now - bucket.updated_at) * limit / 60)
            bucket.updated_at = now
            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return
            retry_after = math.ceil((1 - bucket.tokens) * 60 / limit)
        if self._log_throttle.should_log((webhook.name, "rate_limit")):
            log.warning(
                "Refused a webhook delivery: the webhook is over its rate limit",
                webhook=webhook.name,
                rate_limit_per_minute=limit,
            )
        raise DeliveryRejected(HTTPStatus.TOO_MANY_REQUESTS, "Too many deliveries", retry_after=retry_after)

    def _parse(self, webhook: ConfiguredWebhook, request: WebhookRequest) -> WebhookDelivery:
        try:
            delivery = webhook.source.parse(request)
        except ValueError as e:
            raise self._reject_malformed(webhook, str(e)) from e
        # An empty id is no id: as an id, it would make every later delivery without one a retry.
        delivery_id = delivery.delivery_id or None
        if delivery_id is not None and (
            len(delivery_id) > MAX_DELIVERY_ID_LENGTH
            or not delivery_id.isascii()
            or not delivery_id.isprintable()
        ):
            raise self._reject_malformed(
                webhook, f"Delivery ID must be at most {MAX_DELIVERY_ID_LENGTH} printable ASCII characters"
            )
        if (
            delivery.correlation_key is not None
            and len(delivery.correlation_key) > MAX_CORRELATION_KEY_LENGTH
        ):
            # Not DeliveryRejected: the source type broke its contract, which is a bug answered 500.
            raise ValueError(
                f"{type(webhook.source).__name__}.parse returned a correlation key longer than "
                f"{MAX_CORRELATION_KEY_LENGTH} characters"
            )
        return replace(delivery, delivery_id=delivery_id)
