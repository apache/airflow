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
Topic events: the asset events a delivery records for the Dags that subscribe to it.

A Dag subscribes to a delivery by scheduling on an asset named like one of the candidate names that
``build_candidate_names`` builds for the delivery.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypedDict

from sqlalchemy import and_, exists, select, union_all

from airflow.assets.manager import asset_manager
from airflow.configuration import conf
from airflow.models.asset import (
    AssetActive,
    AssetModel,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
)
from airflow.models.dag import DagModel
from airflow.models.inbound_delivery import DeliveryOutcome
from airflow.models.team import dag_bundle_team_association_table
from airflow.serialization.definitions.assets import SerializedAssetUniqueKey

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence
    from uuid import UUID

    from sqlalchemy import CTE, Exists, Row, Select
    from sqlalchemy.orm import Session

    from airflow.webhooks.base import WebhookDelivery
    from airflow.webhooks.connections import ConfiguredWebhook


def build_dag_has_team_clause() -> Exists:
    """Return a clause that holds for a Dag whose bundle belongs to a team."""
    bundle_team = dag_bundle_team_association_table.c
    return exists().where(bundle_team.dag_bundle_name == DagModel.bundle_name)


def select_scheduling_dags(assets: CTE) -> Select[Any]:
    """
    Select each Dag that is not stale and schedules on one of ``assets``.

    A Dag schedules on an asset directly, or through ``Asset.ref(name=...)`` or ``Asset.ref(uri=...)``,
    which ``register_asset_change`` queues as well. Stale Dags are left out, since the scheduler never
    runs them, though ``register_asset_change`` would queue them.

    :param assets: The assets, as rows with a ``name`` and a ``uri``.
    :return: A query for one row per asset and Dag, with the asset's ``name`` and ``uri``, and the Dag's
        ``dag_id``, ``is_paused``, ``is_draining`` and ``timetable_partitioned``.
    """
    references = union_all(
        select(assets.c.name, assets.c.uri, DagScheduleAssetReference.dag_id)
        .join(AssetModel, and_(AssetModel.name == assets.c.name, AssetModel.uri == assets.c.uri))
        .join(DagScheduleAssetReference, DagScheduleAssetReference.asset_id == AssetModel.id),
        select(assets.c.name, assets.c.uri, DagScheduleAssetNameReference.dag_id).join(
            DagScheduleAssetNameReference, DagScheduleAssetNameReference.name == assets.c.name
        ),
        select(assets.c.name, assets.c.uri, DagScheduleAssetUriReference.dag_id).join(
            DagScheduleAssetUriReference, DagScheduleAssetUriReference.uri == assets.c.uri
        ),
    ).subquery()
    return (
        select(
            references.c.name,
            references.c.uri,
            DagModel.dag_id,
            DagModel.is_paused,
            DagModel.is_draining,
            DagModel.timetable_partitioned,
        )
        .join(DagModel, DagModel.dag_id == references.c.dag_id)
        .where(~DagModel.is_stale)
    )


def find_subscriptions(
    candidates: Collection[str], *, multi_team: bool, session: Session
) -> Sequence[Row[Any]]:
    """
    Return each Dag that is not stale and schedules on an active asset named in ``candidates``.

    Only an asset whose URI is its name counts, as that of a ``WebhookTopic`` or of an ``Asset`` given
    only a name, so that a delivery records no event on an asset that names some data, such as
    ``Asset(name="github/my-org/web/issues", uri="s3://exports/issues")``.

    A Dag schedules on an asset as ``select_scheduling_dags`` finds it. A reference subscribes only once
    some Dag declares the asset, since an event can only be recorded on an active asset. Under
    multi-team, Dags of a team are left out: topic events come from a webhook without a team, and such
    events queue only Dags without one.

    :return: One row per asset and Dag, as ``select_scheduling_dags`` returns them.
    """
    candidate_assets = (
        select(AssetActive.name, AssetActive.uri)
        .where(AssetActive.name.in_(candidates), AssetActive.uri == AssetActive.name)
        .cte("candidate_asset")
    )
    query = select_scheduling_dags(candidate_assets)
    if multi_team:
        query = query.where(~build_dag_has_team_clause())
    return session.execute(query).all()


def _compute_json_size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode())


def build_event_extra(
    webhook: ConfiguredWebhook,
    delivery: WebhookDelivery,
    *,
    inbound_delivery_id: UUID,
    payload: dict[str, Any] | None,
    max_event_payload_bytes: int,
) -> dict[str, Any]:
    """
    Return the extra of a delivery's topic events.

    :param inbound_delivery_id: The id of the delivery's record.
    :param payload: What ``store_payload`` keeps of the delivery's payload. It is left out, and
        ``payload_truncated`` is set, if it is larger than ``max_event_payload_bytes`` as compact JSON
        encoded as UTF-8.
    :param max_event_payload_bytes: As in ``WebhookRegistry``; 0 is no limit.
    """
    extra = {
        # Anyone who may create asset events can set it too, so it tells Dags where an event says it
        # comes from, not where it does.
        "from_webhook": True,
        "webhook": webhook.name,
        "source_type": webhook.source_type,
        "delivery_id": delivery.delivery_id,
        "inbound_delivery_id": str(inbound_delivery_id),
        "correlation_key": delivery.correlation_key,
        "payload": payload,
    }
    if payload is not None and 0 < max_event_payload_bytes < _compute_json_size(payload):
        extra.update(payload=None, payload_truncated=True)
    return extra


class RecordedAssetEvent(TypedDict):
    """A topic event that a delivery recorded, as listed in ``actions["asset_events"]`` of its record."""

    #: The name of the asset the event is recorded on, which is one of the delivery's candidate names.
    asset: str
    asset_id: int
    #: The id of the AssetEvent row, not the sender's id of the delivery or of its event.
    event_id: int


@dataclass(frozen=True)
class TopicEvents:
    """
    What recording a delivery's topic events did.

    :param outcome: The delivery's outcome.
    :param asset_events: The event recorded on each candidate name, sorted by name.
    :param payload: What ``store_payload`` keeps of the payload, if any event was recorded.
    """

    outcome: DeliveryOutcome
    asset_events: list[RecordedAssetEvent] = field(default_factory=list)
    payload: dict[str, Any] | None = None


def record_topic_events(
    webhook: ConfiguredWebhook,
    delivery: WebhookDelivery,
    *,
    candidates: Collection[str],
    inbound_delivery_id: UUID,
    max_event_payload_bytes: int,
    callback_sink: list[Callable[[], None]],
    session: Session,
) -> TopicEvents:
    """
    Record a topic event on each of a delivery's candidate names that a live Dag subscribes to.

    A live Dag is neither paused nor draining, and is one that a topic event can queue: topic events
    have no partition key, which a partition-aware Dag needs. So, as of the lookup, every event
    recorded has a Dag to run; a Dag paused after the lookup still gets the event, but is not queued.

    :param callback_sink: Collects the asset listener calls, to make once the events are committed.
    """
    subscribers = [
        row
        for row in find_subscriptions(
            candidates, multi_team=conf.getboolean("core", "multi_team"), session=session
        )
        if not row.timetable_partitioned
    ]
    live_assets = sorted(
        {(row.name, row.uri) for row in subscribers if not row.is_paused and not row.is_draining}
    )
    if live_assets:
        payload = webhook.select_payload(delivery.payload)
        extra = build_event_extra(
            webhook,
            delivery,
            inbound_delivery_id=inbound_delivery_id,
            payload=payload,
            max_event_payload_bytes=max_event_payload_bytes,
        )
        asset_events: list[RecordedAssetEvent] = []
        for name, uri in live_assets:
            asset_event = asset_manager.register_asset_change(
                asset=SerializedAssetUniqueKey(name=name, uri=uri),
                extra=extra,
                partition_key=None,
                source_is_api=True,
                api_user_teams=set(),
                callback_sink=callback_sink,
                session=session,
            )
            if asset_event is not None:
                asset_events.append(
                    {"asset": name, "asset_id": asset_event.asset_id, "event_id": asset_event.id}
                )
        if asset_events:
            return TopicEvents(DeliveryOutcome.ACCEPTED, asset_events, payload)
    # Nothing is recorded for a paused Dag, so unpausing it never replays the delivery.
    if any(row.is_paused or row.is_draining for row in subscribers):
        return TopicEvents(DeliveryOutcome.DAG_PAUSED)
    return TopicEvents(DeliveryOutcome.NO_SUBSCRIBERS)
