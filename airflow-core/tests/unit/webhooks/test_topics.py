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

from dataclasses import replace
from unittest import mock
from uuid import UUID

import pytest
from sqlalchemy import delete, select

from airflow.models.asset import AssetActive, AssetDagRunQueue, AssetEvent, AssetModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.inbound_delivery import DeliveryOutcome
from airflow.models.team import Team
from airflow.sdk import Asset
from airflow.webhooks.base import WebhookDelivery
from airflow.webhooks.connections import ConfiguredWebhook
from airflow.webhooks.naming import build_candidate_names, build_topic_name
from airflow.webhooks.topics import TopicEvents, build_event_extra, record_topic_events

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_assets, clear_db_teams
from tests_common.test_utils.webhooks import FakeHmacSource

WEBHOOK = ConfiguredWebhook(
    name="fake",
    source_type="fake",
    source=FakeHmacSource(),
    secret="s3cret",
    max_body_bytes=1024,
    store_payload="full",
)
DELIVERY = WebhookDelivery(
    path="my-org/web",
    kinds=[("push",)],
    attributes={"event": "push"},
    delivery_id="delivery-1",
    payload={"ref": "main"},
    correlation_key="pr-7",
)
CANDIDATES = build_candidate_names("fake", DELIVERY.path, DELIVERY.kinds)
TOPIC = Asset(name=build_topic_name("fake", "my-org/web", "push"))
RECORD_ID = UUID("01a14ee2-0e00-7366-b2e2-38ec29a5a228")


@pytest.fixture
def topic_asset(session):
    """The asset of TOPIC, active, as if a Dag declared it, so that Dags can subscribe through Asset.ref."""
    session.add_all([AssetModel(name=TOPIC.name, uri=TOPIC.uri), AssetActive(name=TOPIC.name, uri=TOPIC.uri)])
    session.commit()


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


def record_events(session, webhook: ConfiguredWebhook = WEBHOOK) -> TopicEvents:
    return record_topic_events(
        webhook,
        DELIVERY,
        candidates=CANDIDATES,
        inbound_delivery_id=RECORD_ID,
        max_event_payload_bytes=65536,
        callback_sink=[],
        session=session,
    )


@pytest.mark.db_test
class TestRecordTopicEvents:
    @pytest.fixture(autouse=True)
    def clean_db(self):
        clear_db_assets()
        yield
        clear_db_assets()

    @pytest.mark.usefixtures("topic_asset")
    @pytest.mark.parametrize(
        "schedule",
        [
            pytest.param(TOPIC, id="direct"),
            pytest.param(Asset.ref(name=TOPIC.name), id="name-ref"),
            pytest.param(Asset.ref(uri=TOPIC.uri), id="uri-ref"),
        ],
    )
    def test_event_is_recorded_for_each_kind_of_subscriber(self, dag_maker, session, schedule):
        with dag_maker("subscriber", schedule=schedule, session=session):
            pass

        topic_events = record_events(session)

        (event,) = session.scalars(select(AssetEvent))
        assert topic_events == TopicEvents(DeliveryOutcome.ACCEPTED, {TOPIC.name: event.id}, {"ref": "main"})
        assert (event.extra, event.partition_key) == (
            {
                "from_webhook": True,
                "webhook": "fake",
                "source_type": "fake",
                "delivery_id": "delivery-1",
                "inbound_delivery_id": str(RECORD_ID),
                "correlation_key": "pr-7",
                "payload": {"ref": "main"},
            },
            None,
        )
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["subscriber"]

    def test_reference_subscribes_only_once_a_dag_declares_the_asset(self, dag_maker, session):
        with dag_maker("subscriber", schedule=Asset.ref(name=TOPIC.name), session=session):
            pass

        assert record_events(session) == TopicEvents(DeliveryOutcome.NO_SUBSCRIBERS)

    def test_event_is_recorded_once_on_each_name_however_many_dags_subscribe(self, dag_maker, session):
        wildcard_topic = Asset(name=build_topic_name("fake", "my-org/*", "push"))
        for dag_id, schedule in [
            ("direct", TOPIC),
            ("by_name", Asset.ref(name=TOPIC.name)),
            ("any", wildcard_topic),
        ]:
            with dag_maker(dag_id, schedule=schedule, session=session):
                pass

        topic_events = record_events(session)

        assert (topic_events.outcome, sorted(topic_events.asset_events)) == (
            DeliveryOutcome.ACCEPTED,
            [wildcard_topic.name, TOPIC.name],
        )
        assert sorted(session.scalars(select(AssetDagRunQueue.target_dag_id))) == ["any", "by_name", "direct"]

    @pytest.mark.parametrize(
        ("dag_state", "expected_outcome"),
        [
            pytest.param({"is_paused": True}, DeliveryOutcome.DAG_PAUSED, id="paused"),
            pytest.param({"is_draining": True}, DeliveryOutcome.DAG_PAUSED, id="draining"),
            pytest.param({"is_stale": True}, DeliveryOutcome.NO_SUBSCRIBERS, id="stale"),
            # Topic events have no partition key, so a partition-aware Dag is never queued for one,
            # paused or not.
            pytest.param({"timetable_partitioned": True}, DeliveryOutcome.NO_SUBSCRIBERS, id="partitioned"),
            pytest.param(
                {"is_paused": True, "timetable_partitioned": True},
                DeliveryOutcome.NO_SUBSCRIBERS,
                id="paused-partitioned",
            ),
        ],
    )
    def test_no_event_is_recorded_for_a_dag_that_would_not_run(
        self, dag_maker, session, dag_state, expected_outcome
    ):
        with dag_maker("subscriber", schedule=TOPIC, session=session):
            pass
        for column, value in dag_state.items():
            setattr(dag_maker.dag_model, column, value)
        session.commit()

        assert record_events(session) == TopicEvents(expected_outcome)
        assert session.scalars(select(AssetEvent)).all() == []

    @mock.patch.object(FakeHmacSource, "summarize", autospec=True)
    def test_payload_is_not_summarized_for_a_delivery_that_records_nothing(
        self, mock_summarize, dag_maker, session
    ):
        with dag_maker("subscriber", schedule=TOPIC, session=session):
            pass
        dag_maker.dag_model.is_paused = True
        session.commit()

        assert record_events(session, replace(WEBHOOK, store_payload="summary")) == TopicEvents(
            DeliveryOutcome.DAG_PAUSED
        )
        mock_summarize.assert_not_called()

    def test_paused_dag_does_not_keep_a_live_dag_from_receiving_the_event(self, dag_maker, session):
        with dag_maker("paused", schedule=TOPIC, session=session):
            pass
        dag_maker.dag_model.is_paused = True
        with dag_maker("live", schedule=TOPIC, session=session):
            pass
        session.commit()

        assert record_events(session).outcome == DeliveryOutcome.ACCEPTED
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["live"]

    def test_asset_with_a_uri_of_its_own_is_not_subscribed_to(self, dag_maker, session):
        with dag_maker(
            "subscriber", schedule=Asset(name=TOPIC.name, uri="s3://exports/issues"), session=session
        ):
            pass

        assert record_events(session) == TopicEvents(DeliveryOutcome.NO_SUBSCRIBERS)
        assert session.scalars(select(AssetEvent)).all() == []

    def test_inactive_asset_is_not_subscribed_to(self, dag_maker, session):
        with dag_maker("subscriber", schedule=TOPIC, session=session):
            pass
        session.execute(delete(AssetActive))
        session.commit()

        assert record_events(session) == TopicEvents(DeliveryOutcome.NO_SUBSCRIBERS)

    @pytest.mark.parametrize(
        ("multi_team", "is_paused", "expected_outcome"),
        [
            # A webhook without a team queues only Dags without one under multi-team, so a Dag of a
            # team, paused or not, does not subscribe.
            pytest.param("true", False, DeliveryOutcome.NO_SUBSCRIBERS, id="multi-team"),
            pytest.param("true", True, DeliveryOutcome.NO_SUBSCRIBERS, id="multi-team-paused"),
            pytest.param("false", False, DeliveryOutcome.ACCEPTED, id="single-team"),
        ],
    )
    def test_dag_of_a_team_subscribes_only_without_multi_team(
        self, team_bundle, dag_maker, session, multi_team, is_paused, expected_outcome
    ):
        with dag_maker("subscriber", schedule=TOPIC, bundle_name=team_bundle, session=session):
            pass
        dag_maker.dag_model.is_paused = is_paused
        session.commit()

        with conf_vars({("core", "multi_team"): multi_team}):
            assert record_events(session).outcome == expected_outcome

    def test_event_queues_no_dag_of_a_team_under_multi_team(self, team_bundle, dag_maker, session):
        with dag_maker("teamless", schedule=TOPIC, session=session):
            pass
        with dag_maker("of_team_a", schedule=TOPIC, bundle_name=team_bundle, session=session):
            pass

        with conf_vars({("core", "multi_team"): "true"}):
            assert record_events(session).outcome == DeliveryOutcome.ACCEPTED

        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["teamless"]


class TestBuildEventExtra:
    @pytest.mark.parametrize(
        ("payload", "max_event_payload_bytes", "expected_payload"),
        [
            # '{"ref":"é"}' is 11 characters and 12 bytes.
            pytest.param({"ref": "é"}, 12, {"payload": {"ref": "é"}}, id="at-the-limit"),
            pytest.param(
                {"ref": "é"}, 11, {"payload": None, "payload_truncated": True}, id="over-the-limit-in-bytes"
            ),
            pytest.param({"ref": "é"}, 0, {"payload": {"ref": "é"}}, id="no-limit"),
            pytest.param(None, 1, {"payload": None}, id="no-payload"),
        ],
    )
    def test_payload_over_the_limit_is_left_out(self, payload, max_event_payload_bytes, expected_payload):
        extra = build_event_extra(
            WEBHOOK,
            DELIVERY,
            inbound_delivery_id=RECORD_ID,
            payload=payload,
            max_event_payload_bytes=max_event_payload_bytes,
        )

        assert extra == {
            "from_webhook": True,
            "webhook": "fake",
            "source_type": "fake",
            "delivery_id": "delivery-1",
            "inbound_delivery_id": str(RECORD_ID),
            "correlation_key": "pr-7",
            **expected_payload,
        }
