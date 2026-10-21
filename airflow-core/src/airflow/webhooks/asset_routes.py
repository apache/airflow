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
Asset routes: the events a delivery records on data assets that its webhook's connection names.

A route in ``asset_routes`` of the connection's extra has each delivery offered its topic name also
record an event on its asset, as topic events are recorded: only for a Dag that the event can queue, in
the delivery's transaction, and from a webhook without a team.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypedDict

from sqlalchemy import and_, or_, select

from airflow.assets.manager import asset_manager
from airflow.configuration import conf
from airflow.models.asset import AssetActive, AssetModel
from airflow.models.base import ID_LEN
from airflow.models.inbound_delivery import DeliveryOutcome
from airflow.serialization.definitions.assets import SerializedAssetUniqueKey
from airflow.utils.sqlalchemy import get_dialect_name
from airflow.webhooks.base import get_field
from airflow.webhooks.topics import build_dag_has_team_clause, build_event_extra, select_scheduling_dags

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence
    from uuid import UUID

    from sqlalchemy import Row
    from sqlalchemy.orm import Session

    from airflow.models.asset import AssetEvent
    from airflow.webhooks.base import WebhookDelivery
    from airflow.webhooks.connections import AssetRoute, ConfiguredWebhook

# PostgreSQL stores neither U+0000 nor an unpaired surrogate as text, and MySQL stores partition keys as
# utf8mb3, which has no character outside the Basic Multilingual Plane, such as an emoji.
_UNSTORABLE_KEY_CHARACTER = re.compile("[\x00\ud800-\udfff\U00010000-\U0010ffff]")
# The group of every WebhookTopic, which the Task SDK sets as WebhookTopic.asset_type.
_TOPIC_GROUP = "webhook"


class TransactionRolledBack(Exception):
    """
    The asset manager rolled back the delivery's transaction while recording an event on an asset.

    It does so on SQLite while another process holds the database's write lock, and then goes on in a
    new transaction, which has lost what the delivery recorded before.
    """


class SkipReason(str, enum.Enum):
    """Why an asset route whose topic a delivery offered recorded no event."""

    #: No active asset has the route's asset as its URI or its name: no Dag declares it.
    ASSET_NOT_ACTIVE = "asset_not_active"
    #: The asset is a webhook topic, which receives events only from its webhook.
    ASSET_IS_TOPIC = "asset_is_topic"
    #: Under multi-team, a Dag of a team schedules on the asset, and webhooks have no team.
    DAG_HAS_TEAM = "dag_has_team"
    #: The payload has nothing, or null, at the route's ``partition_key_field``.
    PARTITION_KEY_MISSING = "partition_key_missing"
    #: The value at ``partition_key_field`` is not a non-blank string of at most ``ID_LEN`` characters,
    #: all of which every database can store.
    PARTITION_KEY_INVALID = "partition_key_invalid"
    #: Only paused or draining Dags that the event could queue schedule on the asset.
    DAG_PAUSED = "dag_paused"
    #: No Dag that the event could queue schedules on the asset.
    NO_SUBSCRIBERS = "no_subscribers"

    def __str__(self) -> str:
        return self.value


class RecordedRouteEvent(TypedDict):
    """An event that a delivery recorded through a route, as listed in ``actions["asset_routes"]``."""

    topic: str
    #: The route's asset, as the route names it.
    asset: str
    asset_id: int
    #: The id of the AssetEvent row.
    event_id: int
    partition_key: str | None


class SkippedAssetRoute(TypedDict):
    """A route that recorded no event, as listed in ``actions["skipped_asset_routes"]``."""

    topic: str
    asset: str
    partition_key_field: str | None
    reason: SkipReason


@dataclass(frozen=True)
class RouteEvents:
    """
    What a delivery's asset routes did.

    :param outcome: The delivery's outcome, were the routes all it acted on.
    :param asset_routes: The event that each route recorded, in the routes' order.
    :param skipped_asset_routes: Each route whose topic the delivery offered but that recorded nothing.
    :param payload: What ``store_payload`` keeps of the payload, if any route recorded an event.
    """

    outcome: DeliveryOutcome
    asset_routes: list[RecordedRouteEvent] = field(default_factory=list)
    skipped_asset_routes: list[SkippedAssetRoute] = field(default_factory=list)
    payload: dict[str, Any] | None = None


def merge_outcomes(*outcomes: DeliveryOutcome) -> DeliveryOutcome:
    """Return the outcome of a delivery whose actions had ``outcomes``: whichever says most was done."""
    for outcome in (DeliveryOutcome.ACCEPTED, DeliveryOutcome.DAG_PAUSED):
        if outcome in outcomes:
            return outcome
    return DeliveryOutcome.NO_SUBSCRIBERS


def find_route_asset(key: str, *, session: Session) -> Row[Any] | None:
    """Return the ``name``, ``uri`` and ``group`` of the active asset whose URI, or else name, is ``key``."""
    if not key.isascii() and get_dialect_name(session) == "mysql":
        # MySQL stores them as latin1 and fails a comparison with text that latin1 cannot hold. No asset
        # has such a name or URI there, as the Task SDK refuses any that is not ASCII.
        return None
    assets = session.execute(
        select(AssetActive.name, AssetActive.uri, AssetModel.group)
        .join(AssetModel, and_(AssetModel.name == AssetActive.name, AssetModel.uri == AssetActive.uri))
        .where(or_(AssetActive.uri == key, AssetActive.name == key))
    ).all()
    for asset in assets:
        if asset.uri == key:
            return asset
    return assets[0] if assets else None


def find_route_consumers(asset: Row[Any], *, multi_team: bool, session: Session) -> Sequence[Row[Any]]:
    """
    Return each Dag that is not stale and schedules on ``asset``, as ``select_scheduling_dags`` does.

    Under multi-team, each row also says whether the Dag has a team, in ``has_team``.
    """
    assets = (
        select(AssetActive.name, AssetActive.uri)
        .where(AssetActive.name == asset.name, AssetActive.uri == asset.uri)
        .cte("route_asset")
    )
    query = select_scheduling_dags(assets)
    if multi_team:
        query = query.add_columns(build_dag_has_team_clause().label("has_team"))
    return session.execute(query).all()


def _read_partition_key(payload: dict[str, Any], dotted_path: str) -> str | SkipReason:
    value = get_field(payload, dotted_path)
    if value is None:
        return SkipReason.PARTITION_KEY_MISSING
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > ID_LEN
        or _UNSTORABLE_KEY_CHARACTER.search(value)
    ):
        return SkipReason.PARTITION_KEY_INVALID
    return value


def _check_route(
    route: AssetRoute, delivery: WebhookDelivery, *, multi_team: bool, session: Session
) -> tuple[Row[Any], str | None] | SkipReason:
    """Return the asset and partition key of the event that ``route`` records, or why it records none."""
    if (asset := find_route_asset(route.asset, session=session)) is None:
        return SkipReason.ASSET_NOT_ACTIVE
    if asset.group == _TOPIC_GROUP:
        return SkipReason.ASSET_IS_TOPIC
    consumers = find_route_consumers(asset, multi_team=multi_team, session=session)
    if multi_team and any(consumer.has_team for consumer in consumers):
        return SkipReason.DAG_HAS_TEAM
    partition_key = None
    if route.partition_key_field is not None:
        partition_key = _read_partition_key(delivery.payload, route.partition_key_field)
        if isinstance(partition_key, SkipReason):
            return partition_key
    # register_asset_change queues partition-aware Dags only for an event with a partition key, and the
    # other Dags only for one without.
    queued = [
        consumer
        for consumer in consumers
        if bool(consumer.timetable_partitioned) == (partition_key is not None)
    ]
    if any(not consumer.is_paused and not consumer.is_draining for consumer in queued):
        return asset, partition_key
    return SkipReason.DAG_PAUSED if queued else SkipReason.NO_SUBSCRIBERS


def _build_skipped(route: AssetRoute, reason: SkipReason) -> SkippedAssetRoute:
    return {
        "topic": route.topic,
        "asset": route.asset,
        "partition_key_field": route.partition_key_field,
        "reason": reason,
    }


def _register_route_event(
    asset: Row[Any],
    *,
    extra: dict[str, Any],
    partition_key: str | None,
    callback_sink: list[Callable[[], None]],
    session: Session,
) -> AssetEvent | None:
    transaction = session.get_transaction()
    try:
        asset_event = asset_manager.register_asset_change(
            asset=SerializedAssetUniqueKey(name=asset.name, uri=asset.uri),
            extra=extra,
            partition_key=partition_key,
            source_is_api=True,
            api_user_teams=set(),
            callback_sink=callback_sink,
            session=session,
        )
    except RuntimeError as e:
        # On SQLite, after rolling back for the last time, when the write lock never came free.
        if session.get_transaction() is transaction:
            raise
        raise TransactionRolledBack from e
    if session.get_transaction() is not transaction:
        raise TransactionRolledBack
    return asset_event


def record_route_events(
    webhook: ConfiguredWebhook,
    delivery: WebhookDelivery,
    *,
    candidates: Collection[str],
    inbound_delivery_id: UUID,
    max_event_payload_bytes: int,
    callback_sink: list[Callable[[], None]],
    session: Session,
) -> RouteEvents:
    """
    Record an event through each of the webhook's asset routes whose topic is in ``candidates``.

    A route records an event on its asset, looked up by URI and then by name among active assets, only
    if a Dag that is neither paused nor draining schedules on it and could be queued for the event: a
    partition-aware Dag for a route with a ``partition_key_field``, and any other Dag for one without.
    A route to a webhook topic, an asset in the ``webhook`` group, records nothing, so that a topic
    receives events only from its webhook. The event's partition key is the string at the route's
    ``partition_key_field`` in the payload, read without a template engine. Under multi-team, a route
    whose asset a Dag of a team schedules on, paused or not, records nothing, since webhooks have no
    team. The events carry the extra that topic events do.

    :param callback_sink: Collects the asset listener calls, to make once the events are committed.
    :raises TransactionRolledBack: if the asset manager rolled back the session's transaction.
    """
    offered = set(candidates)
    multi_team = conf.getboolean("core", "multi_team")
    to_record: list[tuple[AssetRoute, Row[Any], str | None]] = []
    skipped: list[SkippedAssetRoute] = []
    for route in webhook.asset_routes:
        if route.topic not in offered:
            continue
        checked = _check_route(route, delivery, multi_team=multi_team, session=session)
        if isinstance(checked, SkipReason):
            skipped.append(_build_skipped(route, checked))
        else:
            to_record.append((route, *checked))
    recorded: list[RecordedRouteEvent] = []
    payload = None
    if to_record:
        payload = webhook.select_payload(delivery.payload)
        extra = build_event_extra(
            webhook,
            delivery,
            inbound_delivery_id=inbound_delivery_id,
            payload=payload,
            max_event_payload_bytes=max_event_payload_bytes,
        )
        for route, asset, partition_key in to_record:
            asset_event = _register_route_event(
                asset, extra=extra, partition_key=partition_key, callback_sink=callback_sink, session=session
            )
            if asset_event is None:
                skipped.append(_build_skipped(route, SkipReason.ASSET_NOT_ACTIVE))
                continue
            recorded.append(
                {
                    "topic": route.topic,
                    "asset": route.asset,
                    "asset_id": asset_event.asset_id,
                    "event_id": asset_event.id,
                    "partition_key": partition_key,
                }
            )
    if recorded:
        return RouteEvents(DeliveryOutcome.ACCEPTED, recorded, skipped, payload)
    paused = any(skipped_route["reason"] is SkipReason.DAG_PAUSED for skipped_route in skipped)
    return RouteEvents(
        DeliveryOutcome.DAG_PAUSED if paused else DeliveryOutcome.NO_SUBSCRIBERS, skipped_asset_routes=skipped
    )
