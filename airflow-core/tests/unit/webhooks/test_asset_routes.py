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
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any
from unittest import mock
from uuid import UUID

import pytest
from sqlalchemy import delete, event, select
from sqlalchemy.exc import OperationalError

from airflow import settings
from airflow.assets.manager import AssetManager, asset_manager
from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
)
from airflow.models.dagbundle import DagBundleModel
from airflow.models.inbound_delivery import DeliveryOutcome
from airflow.models.team import Team
from airflow.sdk import Asset, PartitionedAssetTimetable, WebhookTopic
from airflow.webhooks.asset_routes import (
    RouteEvents,
    SkipReason,
    TransactionRolledBack,
    merge_outcomes,
    record_route_events,
)
from airflow.webhooks.base import WebhookDelivery
from airflow.webhooks.connections import AssetRoute, ConfiguredWebhook
from airflow.webhooks.naming import build_candidate_names
from airflow.webhooks.topics import build_event_extra

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_apdr, clear_db_assets, clear_db_pakl, clear_db_teams
from tests_common.test_utils.webhooks import FakeHmacSource

ORDERS = Asset(name="orders", uri="s3://warehouse/orders")
TOPIC_NAME = "etl/orders/load.completed"
DELIVERY = WebhookDelivery(
    path="orders",
    kinds=[("load.completed",)],
    attributes={"event": "load.completed", "path": "orders"},
    delivery_id="delivery-1",
    payload={"table": "orders", "data": {"date": "2026-10-19"}},
    correlation_key="run-7",
)
CANDIDATES = build_candidate_names("etl", DELIVERY.path, DELIVERY.kinds)
RECORD_ID = UUID("01a14ee2-0e00-7366-b2e2-38ec29a5a228")
ROUTE = AssetRoute(topic=TOPIC_NAME, asset=ORDERS.uri)
KEYED_ROUTE = AssetRoute(topic=TOPIC_NAME, asset=ORDERS.uri, partition_key_field="data.date")
PARTITIONED = PartitionedAssetTimetable(assets=ORDERS)


def build_webhook(*routes: AssetRoute, store_payload: Any = "full") -> ConfiguredWebhook:
    return ConfiguredWebhook(
        name="etl",
        source_type="fake",
        source=FakeHmacSource(),
        secret="s3cret",
        max_body_bytes=1024,
        store_payload=store_payload,
        asset_routes=routes,
    )


def record_events(
    session,
    *routes: AssetRoute,
    webhook: ConfiguredWebhook | None = None,
    payload: dict[str, Any] | None = None,
    max_event_payload_bytes: int = 65536,
    callback_sink: list | None = None,
) -> RouteEvents:
    delivery = DELIVERY if payload is None else replace(DELIVERY, payload=payload)
    return record_route_events(
        webhook or build_webhook(*routes),
        delivery,
        candidates=CANDIDATES,
        inbound_delivery_id=RECORD_ID,
        max_event_payload_bytes=max_event_payload_bytes,
        callback_sink=[] if callback_sink is None else callback_sink,
        session=session,
    )


def get_asset_id(session, asset: Asset) -> int:
    return session.execute(select(AssetModel.id).where(AssetModel.uri == asset.uri)).scalar_one()


def build_recorded(session, route: AssetRoute, partition_key: str | None = None) -> dict[str, Any]:
    """Return how ``route``'s one event on ORDERS is listed."""
    (asset_event,) = session.scalars(select(AssetEvent))
    return {
        "topic": route.topic,
        "asset": route.asset,
        "asset_id": get_asset_id(session, ORDERS),
        "event_id": asset_event.id,
        "partition_key": partition_key,
    }


def build_skipped(route: AssetRoute, reason: SkipReason) -> dict[str, Any]:
    return {
        "topic": route.topic,
        "asset": route.asset,
        "partition_key_field": route.partition_key_field,
        "reason": reason,
    }


@contextmanager
def lock_the_database(times: int | None = None) -> Iterator[None]:
    """Fail the asset manager's SQLite writer-lock statement as a locked database does, ``times`` times."""
    failures = 0

    def fail_while_locked(connection, cursor, statement, parameters, context, executemany) -> None:
        nonlocal failures
        if statement.startswith("UPDATE asset SET id=asset.id") and (times is None or failures < times):
            failures += 1
            raise OperationalError(statement, parameters, Exception("database is locked"))

    event.listen(settings.get_engine(), "before_cursor_execute", fail_while_locked)
    try:
        yield
    finally:
        event.remove(settings.get_engine(), "before_cursor_execute", fail_while_locked)


@pytest.fixture
def team_bundle(session):
    """Bundle team_a_bundle, of team team_a; request it before dag_maker, which must remove its Dags first."""
    clear_db_teams()
    team = Team(name="team_a")
    bundle = DagBundleModel(name="team_a_bundle")
    bundle.teams.append(team)
    session.add_all([team, bundle])
    session.commit()
    yield bundle.name
    clear_db_teams()
    session.execute(delete(DagBundleModel).where(DagBundleModel.name == "team_a_bundle"))
    session.commit()


@pytest.mark.parametrize(
    ("outcomes", "expected"),
    [
        pytest.param(
            (DeliveryOutcome.NO_SUBSCRIBERS, DeliveryOutcome.ACCEPTED),
            DeliveryOutcome.ACCEPTED,
            id="accepted",
        ),
        pytest.param(
            (DeliveryOutcome.DAG_PAUSED, DeliveryOutcome.ACCEPTED), DeliveryOutcome.ACCEPTED, id="over-paused"
        ),
        pytest.param(
            (DeliveryOutcome.NO_SUBSCRIBERS, DeliveryOutcome.DAG_PAUSED),
            DeliveryOutcome.DAG_PAUSED,
            id="paused",
        ),
        pytest.param(
            (DeliveryOutcome.NO_SUBSCRIBERS, DeliveryOutcome.NO_SUBSCRIBERS),
            DeliveryOutcome.NO_SUBSCRIBERS,
            id="nothing",
        ),
    ],
)
def test_merged_outcome_says_the_most_that_was_done(outcomes, expected):
    assert merge_outcomes(*outcomes) == expected


@pytest.mark.db_test
class TestRecordRouteEvents:
    @pytest.fixture(autouse=True)
    def clean_db(self):
        clear_db_assets()
        clear_db_apdr()
        clear_db_pakl()
        yield
        clear_db_assets()
        clear_db_apdr()
        clear_db_pakl()

    @pytest.mark.parametrize(
        "schedule",
        [
            pytest.param(ORDERS, id="direct"),
            pytest.param([ORDERS, Asset.ref(name=ORDERS.name)], id="name-ref"),
            pytest.param([ORDERS, Asset.ref(uri=ORDERS.uri)], id="uri-ref"),
        ],
    )
    @pytest.mark.parametrize(
        "key", [pytest.param(ORDERS.uri, id="by-uri"), pytest.param(ORDERS.name, id="by-name")]
    )
    def test_event_is_recorded_on_the_asset_the_route_names(self, dag_maker, session, schedule, key):
        if isinstance(schedule, list):
            # The asset must be active for a reference to find it, so another Dag declares it.
            with dag_maker("declares", schedule=schedule[0], session=session):
                pass
            dag_maker.dag_model.is_paused = True
            schedule = schedule[1]
        with dag_maker("consumer", schedule=schedule, session=session):
            pass
        session.commit()
        route = AssetRoute(topic=TOPIC_NAME, asset=key)

        route_events = record_events(session, route)

        (asset_event,) = session.scalars(select(AssetEvent))
        assert route_events == RouteEvents(
            DeliveryOutcome.ACCEPTED, [build_recorded(session, route)], [], DELIVERY.payload
        )
        assert asset_event.asset_id == get_asset_id(session, ORDERS)
        assert (asset_event.extra, asset_event.partition_key) == (
            build_event_extra(
                build_webhook(),
                DELIVERY,
                inbound_delivery_id=RECORD_ID,
                payload=DELIVERY.payload,
                max_event_payload_bytes=65536,
            ),
            None,
        )
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["consumer"]

    def test_asset_is_looked_up_by_uri_before_name(self, dag_maker, session):
        # This asset's name is the URI of ORDERS.
        legacy = Asset(name=ORDERS.uri, uri="s3://warehouse/legacy-orders")
        with dag_maker("orders_consumer", schedule=ORDERS, session=session):
            pass
        with dag_maker("legacy_consumer", schedule=legacy, session=session):
            pass

        assert record_events(session, ROUTE).outcome == DeliveryOutcome.ACCEPTED
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["orders_consumer"]

    def test_route_whose_topic_the_delivery_does_not_offer_records_nothing(self, dag_maker, session):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass

        route_events = record_events(session, AssetRoute(topic="etl/orders/load.failed", asset=ORDERS.uri))

        assert route_events == RouteEvents(DeliveryOutcome.NO_SUBSCRIBERS)
        assert session.scalars(select(AssetEvent)).all() == []

    def test_route_to_a_wildcard_topic_records_an_event(self, dag_maker, session):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass

        route_events = record_events(session, AssetRoute(topic="etl/*/load.completed", asset=ORDERS.uri))

        assert route_events.outcome == DeliveryOutcome.ACCEPTED

    @pytest.mark.parametrize(
        "declared", [pytest.param(False, id="unknown"), pytest.param(True, id="orphaned")]
    )
    def test_route_to_an_asset_that_is_not_active_is_skipped(self, dag_maker, session, declared):
        if declared:
            with dag_maker("consumer", schedule=ORDERS, session=session):
                pass
            session.execute(delete(AssetActive))
            session.commit()

        assert record_events(session, ROUTE) == RouteEvents(
            DeliveryOutcome.NO_SUBSCRIBERS,
            skipped_asset_routes=[build_skipped(ROUTE, SkipReason.ASSET_NOT_ACTIVE)],
        )
        assert session.scalars(select(AssetEvent)).all() == []

    @pytest.mark.backend("mysql")
    def test_route_to_an_asset_that_is_not_ascii_is_skipped_on_mysql(self, dag_maker, session):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass
        route = AssetRoute(topic=TOPIC_NAME, asset="s3://warehouse/注文")

        assert record_events(session, route) == RouteEvents(
            DeliveryOutcome.NO_SUBSCRIBERS,
            skipped_asset_routes=[build_skipped(route, SkipReason.ASSET_NOT_ACTIVE)],
        )

    def test_route_to_a_webhook_topic_is_skipped(self, dag_maker, session):
        topic = WebhookTopic(webhook="github", path="my-org/web", event="push")
        with dag_maker("subscriber", schedule=topic, session=session):
            pass
        route = AssetRoute(topic=TOPIC_NAME, asset=topic.name)

        assert record_events(session, route) == RouteEvents(
            DeliveryOutcome.NO_SUBSCRIBERS,
            skipped_asset_routes=[build_skipped(route, SkipReason.ASSET_IS_TOPIC)],
        )
        assert session.scalars(select(AssetEvent)).all() == []

    @mock.patch.object(asset_manager, "register_asset_change", autospec=True, return_value=None)
    def test_route_whose_asset_is_removed_before_its_event_is_recorded_is_skipped(
        self, mock_register, dag_maker, session
    ):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass

        route_events = record_events(session, ROUTE)

        assert route_events == RouteEvents(
            DeliveryOutcome.NO_SUBSCRIBERS,
            skipped_asset_routes=[build_skipped(ROUTE, SkipReason.ASSET_NOT_ACTIVE)],
        )

    @pytest.mark.parametrize(
        ("route", "schedule", "dag_state", "expected_reason"),
        [
            pytest.param(ROUTE, ORDERS, {"is_paused": True}, SkipReason.DAG_PAUSED, id="paused"),
            pytest.param(ROUTE, ORDERS, {"is_draining": True}, SkipReason.DAG_PAUSED, id="draining"),
            pytest.param(ROUTE, ORDERS, {"is_stale": True}, SkipReason.NO_SUBSCRIBERS, id="stale"),
            # An event without a partition key never queues a partition-aware Dag, paused or not.
            pytest.param(ROUTE, PARTITIONED, {}, SkipReason.NO_SUBSCRIBERS, id="unkeyed-partitioned"),
            pytest.param(
                ROUTE,
                PARTITIONED,
                {"is_paused": True},
                SkipReason.NO_SUBSCRIBERS,
                id="unkeyed-paused-partitioned",
            ),
            # And an event with one never queues any other Dag.
            pytest.param(KEYED_ROUTE, ORDERS, {}, SkipReason.NO_SUBSCRIBERS, id="keyed-unpartitioned"),
            pytest.param(
                KEYED_ROUTE,
                PARTITIONED,
                {"is_paused": True},
                SkipReason.DAG_PAUSED,
                id="keyed-paused-partitioned",
            ),
        ],
    )
    def test_no_event_is_recorded_for_a_dag_that_would_not_run(
        self, dag_maker, session, route, schedule, dag_state, expected_reason
    ):
        with dag_maker("consumer", schedule=schedule, session=session):
            pass
        for column, value in dag_state.items():
            setattr(dag_maker.dag_model, column, value)
        session.commit()

        route_events = record_events(session, route)

        assert route_events == RouteEvents(
            DeliveryOutcome(expected_reason.value),
            skipped_asset_routes=[build_skipped(route, expected_reason)],
        )
        assert session.scalars(select(AssetEvent)).all() == []

    @pytest.mark.parametrize(
        "partition_key",
        [
            pytest.param("2026-10-19", id="date"),
            pytest.param("k" * 250, id="longest"),
            pytest.param("日付-2026", id="not-ascii"),
        ],
    )
    def test_keyed_event_queues_only_partition_aware_dags(self, dag_maker, session, partition_key):
        with dag_maker("by_day", schedule=PARTITIONED, session=session):
            pass
        with dag_maker("whole_table", schedule=ORDERS, session=session):
            pass

        route_events = record_events(session, KEYED_ROUTE, payload={"data": {"date": partition_key}})

        (asset_event,) = session.scalars(select(AssetEvent))
        assert route_events.asset_routes == [build_recorded(session, KEYED_ROUTE, partition_key)]
        assert asset_event.partition_key == partition_key
        assert session.execute(
            select(AssetPartitionDagRun.target_dag_id, AssetPartitionDagRun.partition_key)
        ).all() == [("by_day", partition_key)]
        assert session.scalars(select(AssetDagRunQueue)).all() == []

    @pytest.mark.parametrize(
        ("payload", "expected_reason"),
        [
            pytest.param({}, SkipReason.PARTITION_KEY_MISSING, id="missing"),
            pytest.param({"data": None}, SkipReason.PARTITION_KEY_MISSING, id="missing-parent"),
            pytest.param({"data": {"date": None}}, SkipReason.PARTITION_KEY_MISSING, id="null"),
            pytest.param({"data": {"date": 20261019}}, SkipReason.PARTITION_KEY_INVALID, id="integer"),
            pytest.param({"data": {"date": True}}, SkipReason.PARTITION_KEY_INVALID, id="boolean"),
            pytest.param({"data": {"date": ["2026-10-19"]}}, SkipReason.PARTITION_KEY_INVALID, id="list"),
            pytest.param({"data": {"date": {}}}, SkipReason.PARTITION_KEY_INVALID, id="object"),
            pytest.param({"data": {"date": ""}}, SkipReason.PARTITION_KEY_INVALID, id="empty"),
            pytest.param({"data": {"date": " \t"}}, SkipReason.PARTITION_KEY_INVALID, id="blank"),
            pytest.param({"data": {"date": "k" * 251}}, SkipReason.PARTITION_KEY_INVALID, id="too-long"),
            # MySQL cannot store it.
            pytest.param({"data": {"date": "📦-2026"}}, SkipReason.PARTITION_KEY_INVALID, id="emoji"),
            # PostgreSQL cannot store it, and source types that do not read JSON can return it.
            pytest.param({"data": {"date": "2026\x00"}}, SkipReason.PARTITION_KEY_INVALID, id="nul"),
            # PostgreSQL cannot store it, and JSON text such as "\ud800" parses to it.
            pytest.param(
                {"data": {"date": "2026\ud800"}}, SkipReason.PARTITION_KEY_INVALID, id="lone-surrogate"
            ),
        ],
    )
    def test_route_without_a_usable_partition_key_is_skipped(
        self, dag_maker, session, payload, expected_reason
    ):
        with dag_maker("by_day", schedule=PARTITIONED, session=session):
            pass

        route_events = record_events(session, KEYED_ROUTE, payload=payload)

        assert route_events == RouteEvents(
            DeliveryOutcome.NO_SUBSCRIBERS, skipped_asset_routes=[build_skipped(KEYED_ROUTE, expected_reason)]
        )
        assert session.scalars(select(AssetEvent)).all() == []

    @pytest.mark.parametrize(
        ("multi_team", "is_paused", "expected_queued"),
        [
            # Webhooks have no team, so under multi-team a route to an asset that a Dag of a team schedules
            # on, paused or not, records nothing for the Dags without a team either.
            pytest.param("true", False, [], id="multi-team"),
            pytest.param("true", True, [], id="multi-team-paused"),
            pytest.param("false", False, ["of_team_a", "teamless"], id="single-team"),
        ],
    )
    def test_route_to_an_asset_a_dag_of_a_team_schedules_on_is_skipped_under_multi_team(
        self, team_bundle, dag_maker, session, multi_team, is_paused, expected_queued
    ):
        with dag_maker("teamless", schedule=ORDERS, session=session):
            pass
        with dag_maker("of_team_a", schedule=ORDERS, bundle_name=team_bundle, session=session):
            pass
        dag_maker.dag_model.is_paused = is_paused
        session.commit()

        with conf_vars({("core", "multi_team"): multi_team}):
            route_events = record_events(session, ROUTE)

        if expected_queued:
            assert route_events.outcome == DeliveryOutcome.ACCEPTED
        else:
            assert route_events == RouteEvents(
                DeliveryOutcome.NO_SUBSCRIBERS,
                skipped_asset_routes=[build_skipped(ROUTE, SkipReason.DAG_HAS_TEAM)],
            )
        assert sorted(session.scalars(select(AssetDagRunQueue.target_dag_id))) == expected_queued

    @conf_vars({("core", "multi_team"): "true"})
    def test_route_to_an_asset_only_dags_without_a_team_schedule_on_records_under_multi_team(
        self, team_bundle, dag_maker, session
    ):
        with dag_maker("teamless", schedule=ORDERS, session=session):
            pass

        assert record_events(session, ROUTE).outcome == DeliveryOutcome.ACCEPTED

    def test_each_route_is_listed_in_order(self, dag_maker, session):
        invoices = Asset(name="invoices", uri="s3://warehouse/invoices")
        with dag_maker("orders_consumer", schedule=ORDERS, session=session):
            pass
        with dag_maker("invoices_consumer", schedule=invoices, session=session):
            pass
        routes = [
            AssetRoute(topic=TOPIC_NAME, asset=invoices.uri),
            AssetRoute(topic=TOPIC_NAME, asset="s3://warehouse/refunds"),
            ROUTE,
        ]

        route_events = record_events(session, *routes)

        event_ids = {
            asset_event.asset_id: asset_event.id for asset_event in session.scalars(select(AssetEvent))
        }
        assert route_events.outcome == DeliveryOutcome.ACCEPTED
        assert route_events.asset_routes == [
            {
                "topic": TOPIC_NAME,
                "asset": asset.uri,
                "asset_id": get_asset_id(session, asset),
                "event_id": event_ids[get_asset_id(session, asset)],
                "partition_key": None,
            }
            for asset in (invoices, ORDERS)
        ]
        assert route_events.skipped_asset_routes == [build_skipped(routes[1], SkipReason.ASSET_NOT_ACTIVE)]

    def test_paused_dag_behind_one_route_makes_a_delivery_that_records_nothing_dag_paused(
        self, dag_maker, session
    ):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass
        dag_maker.dag_model.is_paused = True
        session.commit()
        unknown = AssetRoute(topic=TOPIC_NAME, asset="s3://warehouse/refunds")

        assert record_events(session, unknown, ROUTE).outcome == DeliveryOutcome.DAG_PAUSED

    @pytest.mark.parametrize(
        ("store_payload", "max_event_payload_bytes", "expected_payload"),
        [
            pytest.param("full", 65536, DELIVERY.payload, id="full"),
            pytest.param("full", 1, DELIVERY.payload, id="over-the-event-limit"),
            pytest.param("none", 65536, None, id="none"),
        ],
    )
    def test_events_carry_the_payload_as_topic_events_do(
        self, dag_maker, session, store_payload, max_event_payload_bytes, expected_payload
    ):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass
        webhook = build_webhook(ROUTE, store_payload=store_payload)

        route_events = record_events(
            session, webhook=webhook, max_event_payload_bytes=max_event_payload_bytes
        )

        (asset_event,) = session.scalars(select(AssetEvent))
        assert route_events.payload == expected_payload
        assert asset_event.extra == build_event_extra(
            webhook,
            DELIVERY,
            inbound_delivery_id=RECORD_ID,
            payload=expected_payload,
            max_event_payload_bytes=max_event_payload_bytes,
        )

    @mock.patch.object(FakeHmacSource, "summarize", autospec=True)
    def test_payload_is_not_summarized_for_routes_that_record_nothing(
        self, mock_summarize, dag_maker, session
    ):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass
        dag_maker.dag_model.is_paused = True
        session.commit()

        record_events(session, webhook=build_webhook(ROUTE, store_payload="summary"))

        mock_summarize.assert_not_called()

    @mock.patch.object(AssetManager, "nofity_asset_event_emitted", autospec=True)
    def test_listeners_are_left_to_the_callback_sink(self, mock_notify_event_emitted, dag_maker, session):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass
        callback_sink: list = []

        record_events(session, ROUTE, callback_sink=callback_sink)

        mock_notify_event_emitted.assert_not_called()
        for callback in callback_sink:
            callback()
        mock_notify_event_emitted.assert_called_once()

    @pytest.mark.backend("sqlite")
    @pytest.mark.parametrize(
        "times",
        [
            # The asset manager rolls back, takes the lock and records the event in a new transaction.
            pytest.param(1, id="once"),
            # It rolls back each time and then raises RuntimeError.
            pytest.param(None, id="always"),
        ],
    )
    @mock.patch("time.sleep", autospec=True)
    def test_transaction_rolled_back_by_the_asset_manager_is_raised(
        self, mock_sleep, dag_maker, session, times
    ):
        with dag_maker("by_day", schedule=PARTITIONED, session=session):
            pass

        with lock_the_database(times), pytest.raises(TransactionRolledBack):
            record_events(session, KEYED_ROUTE)

    @mock.patch.object(asset_manager, "register_asset_change", autospec=True, side_effect=RuntimeError("bug"))
    def test_other_runtime_errors_are_raised_as_they_are(self, mock_register, dag_maker, session):
        with dag_maker("consumer", schedule=ORDERS, session=session):
            pass

        with pytest.raises(RuntimeError, match="bug"):
            record_events(session, ROUTE)
