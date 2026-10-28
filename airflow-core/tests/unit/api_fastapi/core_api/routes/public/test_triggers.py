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

from datetime import timedelta
from unittest import mock

import pytest
import time_machine

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.models.asset import AssetActive, AssetModel, AssetWatcherModel
from airflow.models.deadline import Deadline
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_callbacks,
    clear_db_dags,
    clear_db_deadline,
    clear_db_jobs,
    clear_db_runs,
    clear_db_teams,
    clear_db_triggers,
)

pytestmark = pytest.mark.db_test

NOW = timezone.datetime(2026, 10, 21, 12, 0, 0)
SECRET_KWARG = "kwargs-secret-7f3a9c"
SIMPLE_AUTH = "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"


async def notify(**kwargs):
    """A triggerer callback."""


def _clear():
    clear_db_assets()
    clear_db_deadline()
    clear_db_callbacks()
    clear_db_triggers()
    clear_db_runs()
    clear_db_jobs()
    clear_db_dags()
    clear_db_teams()


@pytest.fixture(autouse=True)
def _clean_db():
    _clear()
    yield
    _clear()


@pytest.fixture(autouse=True)
def _frozen():
    with time_machine.travel(NOW, tick=False):
        yield


def _job(session, *, hostname, heartbeat_age):
    job = Job(job_type="TriggererJob", heartrate=5)
    job.hostname = hostname
    job.latest_heartbeat = NOW - timedelta(seconds=heartbeat_age)
    session.add(job)
    session.flush()
    return job


def _trigger(session, name, *, job=None, created_date=NOW):
    trigger = Trigger(
        classpath=f"tests.{name}.Trigger", kwargs={"token": SECRET_KWARG}, created_date=created_date
    )
    trigger.triggerer_id = job.id if job else None
    session.add(trigger)
    session.flush()
    return trigger


def _watcher(session, name, trigger):
    asset = AssetModel(name=f"to_{name}_asset", uri=f"s3://bucket/{name}", group="asset")
    session.add(asset)
    session.flush()
    session.add(AssetWatcherModel(name=f"{name}_watcher", asset_id=asset.id, trigger_id=trigger.id))
    session.add(AssetActive.for_asset(asset))
    session.flush()
    return asset


def _deferred_task(session, create_task_instance, name, trigger, *, resumed=False):
    state = TaskInstanceState.SCHEDULED if resumed else TaskInstanceState.DEFERRED
    ti = create_task_instance(dag_id=f"dag_{name}", task_id=f"{name}_task", state=state, session=session)
    if not resumed:
        ti.trigger_id = trigger.id
    session.flush()
    return ti


def _name(trigger_json):
    return "callback" if trigger_json["kind"] == "callback" else trigger_json["classpath"].split(".")[1]


@pytest.fixture
def triggers(session, create_task_instance):
    """Triggers of each kind and status, on a live and a dead triggerer, and two whose owner let go."""
    live = _job(session, hostname="trig-a", heartbeat_age=1)
    dead = _job(session, hostname="trig-b", heartbeat_age=600)

    sensor = _trigger(session, "sensor", job=live)
    sensor_ti = _deferred_task(session, create_task_instance, "sensor", sensor)
    queued = _trigger(session, "queued")
    _deferred_task(session, create_task_instance, "queued", queued)
    # The task resumed: the trigger is deleted on the triggerer's next cleanup.
    fired = _trigger(session, "fired", job=live)
    _deferred_task(session, create_task_instance, "fired", fired, resumed=True)

    # A deadline of the sensor's Dag run, missed: its callback runs on a triggerer.
    deadline = Deadline(
        deadline_time=NOW,
        callback=AsyncCallback(notify),
        dagrun_id=sensor_ti.dag_run.id,
        deadline_alert_id=None,
        dag_id="dag_sensor",
    )
    session.add(deadline)
    session.flush()
    callback = deadline.callback
    callback.queue(session=session)
    session.flush()
    callback.trigger.triggerer_id = dead.id

    watched = _trigger(session, "watched", job=live, created_date=NOW - timedelta(minutes=5))
    watched_asset = _watcher(session, "watched", watched)
    pending = _trigger(session, "pending")
    pending_asset = _watcher(session, "pending", pending)
    # Its watcher was removed: deleted on the next cleanup too.
    removed = _trigger(session, "removed", job=live)
    session.commit()
    return {
        "live": live,
        "sensor": sensor,
        "sensor_ti": sensor_ti,
        "queued": queued,
        "fired": fired,
        "callback": callback,
        "watched": watched,
        "watched_asset": watched_asset,
        "pending": pending,
        "pending_asset": pending_asset,
        "removed": removed,
    }


def _by_id(body):
    return {t["id"]: t for t in body["triggers"]}


class TestGetTriggers:
    def test_lists_every_kind_with_its_status(self, test_client, triggers):
        response = test_client.get("/triggers")

        assert response.status_code == 200
        assert sorted((_name(t), t["kind"], t["status"]) for t in response.json()["triggers"]) == [
            ("callback", "callback", "orphaned"),
            ("pending", "asset_watcher", "pending"),
            ("queued", "task", "pending"),
            ("sensor", "task", "running"),
            ("watched", "asset_watcher", "running"),
        ]
        assert response.json()["total_entries"] == 5

    def test_names_the_owner_of_each_kind(self, test_client, triggers):
        body = _by_id(test_client.get("/triggers").json())
        sensor_ti, callback = triggers["sensor_ti"], triggers["callback"]

        assert body[triggers["sensor"].id]["task_instance"] == {
            "dag_id": "dag_sensor",
            "dag_run_id": sensor_ti.run_id,
            "task_id": "sensor_task",
            "map_index": -1,
            "try_number": sensor_ti.try_number,
            "state": "deferred",
        }
        assert body[callback.trigger_id]["callback"] == {
            "id": str(callback.id),
            "dag_id": "dag_sensor",
            "dag_run_id": sensor_ti.run_id,
            "state": "queued",
        }
        assert (body[triggers["sensor"].id]["dag_id"], body[callback.trigger_id]["dag_id"]) == (
            "dag_sensor",
            "dag_sensor",
        )

    def test_a_watcher_trigger_in_full(self, test_client, triggers):
        watched = _by_id(test_client.get("/triggers").json())[triggers["watched"].id]

        assert watched == {
            "id": triggers["watched"].id,
            "kind": "asset_watcher",
            "classpath": "tests.watched.Trigger",
            "created_date": "2026-10-21T11:55:00Z",
            "queue": None,
            "team_name": None,
            "status": "running",
            "triggerer_hostname": "trig-a",
            "dag_id": None,
            "task_instance": None,
            "callback": None,
            "asset_watchers": [
                {
                    "asset_id": triggers["watched_asset"].id,
                    "asset_name": "to_watched_asset",
                    "asset_uri": "s3://bucket/watched",
                    "watcher_name": "watched_watcher",
                }
            ],
        }

    def test_a_trigger_turns_orphaned_when_its_triggerer_stops(self, test_client, session, triggers):
        session.get(Job, triggers["live"].id).latest_heartbeat = NOW - timedelta(seconds=600)
        session.commit()

        body = test_client.get("/triggers", params={"status": "orphaned"}).json()

        assert sorted(_name(t) for t in body["triggers"]) == ["callback", "sensor", "watched"]

    def test_never_exposes_trigger_kwargs(self, test_client, triggers):
        response = test_client.get("/triggers")

        assert SECRET_KWARG not in response.text
        assert all("kwargs" not in t for t in response.json()["triggers"])

    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            pytest.param([("kind", "callback")], ["callback"], id="kind"),
            pytest.param([("kind", "task"), ("status", "running")], ["sensor"], id="kind-and-status"),
            pytest.param(
                [("status", "orphaned"), ("status", "pending")],
                ["callback", "pending", "queued"],
                id="statuses",
            ),
            pytest.param([("triggerer_hostname", "trig-a")], ["sensor", "watched"], id="triggerer"),
            pytest.param(
                [("triggerer_hostname", "trig-a"), ("kind", "task")], ["sensor"], id="triggerer-and-kind"
            ),
            pytest.param([("dag_id", "dag_queued")], ["queued"], id="dag"),
            pytest.param([("dag_id", "dag_sensor")], ["callback", "sensor"], id="dag-of-task-and-callback"),
            pytest.param(
                [("kind", "asset_watcher"), ("status", "running")], ["watched"], id="running-watchers"
            ),
        ],
    )
    def test_filters(self, test_client, triggers, params, expected):
        response = test_client.get("/triggers", params=params)

        assert response.status_code == 200
        assert sorted(_name(t) for t in response.json()["triggers"]) == expected

    def test_filter_by_asset(self, test_client, triggers):
        response = test_client.get("/triggers", params={"asset_id": triggers["pending_asset"].id})

        assert [t["id"] for t in response.json()["triggers"]] == [triggers["pending"].id]

    @pytest.mark.parametrize(
        ("order_by", "expected"),
        [
            pytest.param("created_date", ["watched", "sensor"], id="created"),
            pytest.param("-created_date", ["sensor", "watched"], id="created-descending"),
            pytest.param("-id", ["watched", "sensor"], id="id-descending"),
        ],
    )
    def test_sorts(self, test_client, triggers, order_by, expected):
        response = test_client.get("/triggers", params={"order_by": order_by, "triggerer_hostname": "trig-a"})

        assert [_name(t) for t in response.json()["triggers"]] == expected

    @pytest.mark.parametrize("order_by", ["consecutive_failures", "kind"])
    def test_rejects_sorting_on_anything_else(self, test_client, triggers, order_by):
        assert test_client.get("/triggers", params={"order_by": order_by}).status_code == 400

    @pytest.mark.parametrize("params", [{"status": "crashed"}, {"kind": "sensor"}])
    def test_rejects_an_unknown_status_or_kind(self, test_client, triggers, params):
        assert test_client.get("/triggers", params=params).status_code == 422

    @pytest.mark.parametrize(
        ("dag_ids", "asset_ids", "expected"),
        [
            pytest.param({"dag_sensor"}, set(), {"sensor", "callback"}, id="one-dag"),
            pytest.param(set(), {"watched"}, {"watched"}, id="one-asset"),
            pytest.param(set(), set(), set(), id="nothing"),
        ],
    )
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets")
    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_shows_a_trigger_to_whoever_can_see_its_owner(
        self, mock_dag_ids, mock_assets, test_client, triggers, dag_ids, asset_ids, expected
    ):
        mock_dag_ids.return_value = dag_ids
        mock_assets.return_value = {triggers[f"{name}_asset"].id for name in asset_ids}

        body = test_client.get("/triggers").json()

        assert {_name(t) for t in body["triggers"]} == expected
        assert body["total_entries"] == len(expected)

    @mock.patch(f"{SIMPLE_AUTH}.is_authorized_asset", return_value=False)
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets")
    def test_assets_listed_without_asset_read_show_no_watchers(self, mock_assets, _, test_client, triggers):
        # The FAB auth manager lists every asset id and leaves the asset read check to the route, as GET /assets does.
        mock_assets.return_value = {triggers["watched_asset"].id, triggers["pending_asset"].id}

        body = test_client.get("/triggers").json()

        assert [t for t in body["triggers"] if t["asset_watchers"]] == []

    @pytest.mark.parametrize(
        ("readable", "expected"),
        [
            pytest.param({"watched"}, ["watched_watcher"], id="one-of-its-assets"),
            pytest.param({"watched", "shared"}, ["watched_watcher", "shared_watcher"], id="both"),
        ],
    )
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets")
    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_a_shared_trigger_lists_only_the_watchers_the_caller_can_read(
        self, mock_dag_ids, mock_assets, test_client, session, triggers, readable, expected
    ):
        assets = {
            "watched": triggers["watched_asset"],
            "shared": _watcher(session, "shared", triggers["watched"]),
        }
        session.commit()
        mock_dag_ids.return_value = set()
        mock_assets.return_value = {assets[name].id for name in readable}
        watched_id = triggers["watched"].id

        listed = test_client.get("/triggers").json()["triggers"]
        one = test_client.get(f"/triggers/{watched_id}").json()

        assert [t["id"] for t in listed] == [watched_id]
        assert [w["watcher_name"] for w in listed[0]["asset_watchers"]] == expected
        assert [w["watcher_name"] for w in one["asset_watchers"]] == expected

    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets")
    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_filter_by_an_asset_the_caller_cannot_read_finds_nothing(
        self, mock_dag_ids, mock_assets, test_client, session, triggers
    ):
        shared_asset = _watcher(session, "shared", triggers["watched"])
        session.commit()
        mock_dag_ids.return_value = set()
        mock_assets.return_value = {triggers["watched_asset"].id}

        response = test_client.get("/triggers", params={"asset_id": shared_asset.id})

        assert response.json() == {"triggers": [], "total_entries": 0}

    @pytest.mark.parametrize(("multi_team", "total"), [("True", 4), ("False", 5)])
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_teams", return_value={"team_x"})
    def test_multi_team_hides_the_triggers_of_other_teams(
        self, _teams, test_client, session, triggers, multi_team, total
    ):
        session.add_all([Team(name="team_x"), Team(name="team_y")])
        session.flush()
        session.get(Trigger, triggers["watched"].id).team_name = "team_y"
        session.get(Trigger, triggers["sensor"].id).team_name = "team_x"
        session.commit()

        with conf_vars({("core", "multi_team"): multi_team}):
            body = test_client.get("/triggers").json()

        ids = {t["id"] for t in body["triggers"]}
        assert (triggers["watched"].id in ids, triggers["sensor"].id in ids) == (multi_team == "False", True)
        assert body["total_entries"] == total

    @pytest.mark.parametrize("extra", [0, 6])
    def test_query_count_does_not_grow_with_triggers(self, test_client, session, triggers, extra):
        # Watchers: syncing a Dag for each extra deferred task would orphan the fixture's unreferenced assets.
        for i in range(extra):
            _watcher(session, f"extra{i}", _trigger(session, f"extra{i}"))
        session.commit()

        # readable Dags and assets, the count, the page and its eager loads of watchers, assets, callbacks
        # and task instances, and the callbacks' Dag runs
        with assert_queries_count(9):
            response = test_client.get("/triggers")

        assert response.json()["total_entries"] == 5 + extra

    def test_should_respond_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get("/triggers").status_code == 401

    def test_a_user_who_can_see_no_owner_sees_no_triggers(self, unauthorized_test_client, triggers):
        response = unauthorized_test_client.get("/triggers")

        assert response.status_code == 200
        assert response.json() == {"triggers": [], "total_entries": 0}


class TestGetTrigger:
    def test_get_one(self, test_client, triggers):
        response = test_client.get(f"/triggers/{triggers['sensor'].id}")

        assert response.status_code == 200
        body = response.json()
        assert (body["kind"], body["status"], body["triggerer_hostname"]) == ("task", "running", "trig-a")
        assert body["task_instance"]["state"] == "deferred"

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_a_trigger_the_caller_cannot_see_is_not_found(self, mock_dag_ids, test_client, triggers):
        mock_dag_ids.return_value = {"dag_sensor"}

        assert test_client.get(f"/triggers/{triggers['queued'].id}").status_code == 404
        assert test_client.get(f"/triggers/{triggers['sensor'].id}").status_code == 200

    @pytest.mark.parametrize("name", ["fired", "removed"])
    def test_a_trigger_its_owner_let_go_is_not_found(self, test_client, triggers, name):
        response = test_client.get(f"/triggers/{triggers[name].id}")

        assert (response.status_code, response.json()["detail"]) == (
            404,
            f"The trigger with ID `{triggers[name].id}` was not found",
        )

    def test_should_respond_404(self, test_client, triggers):
        response = test_client.get("/triggers/999999")

        assert (response.status_code, response.json()["detail"]) == (
            404,
            "The trigger with ID `999999` was not found",
        )

    def test_should_respond_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get("/triggers/1").status_code == 401


def test_get_asset_lists_its_watchers_with_their_status_and_trigger(test_client, triggers):
    response = test_client.get(f"/assets/{triggers['watched_asset'].id}")

    assert response.json()["watchers"] == [
        {
            "name": "watched_watcher",
            "trigger_id": triggers["watched"].id,
            "created_date": "2026-10-21T11:55:00Z",
            "status": "running",
        }
    ]


@pytest.mark.parametrize(
    ("multi_team", "team_name", "expected"),
    [
        pytest.param("True", "team_y", None, id="other-team"),
        pytest.param("True", "team_x", "running", id="own-team"),
        pytest.param("True", None, "running", id="global"),
        pytest.param("False", "team_y", "running", id="multi-team-off"),
    ],
)
@mock.patch(f"{SIMPLE_AUTH}.get_authorized_teams", return_value={"team_x"})
def test_asset_watcher_status_is_null_unless_the_caller_can_see_the_trigger(
    _teams, test_client, session, triggers, multi_team, team_name, expected
):
    session.add_all([Team(name="team_x"), Team(name="team_y")])
    session.flush()
    session.get(Trigger, triggers["watched"].id).team_name = team_name
    session.commit()
    asset_id = triggers["watched_asset"].id

    with conf_vars({("core", "multi_team"): multi_team}):
        one = test_client.get(f"/assets/{asset_id}").json()["watchers"]
        listed = next(a for a in test_client.get("/assets").json()["assets"] if a["id"] == asset_id)
        ui = next(
            a for a in test_client.get("http://testserver/ui/assets").json()["assets"] if a["id"] == asset_id
        )
        seen = test_client.get(f"/triggers/{triggers['watched'].id}").status_code

    assert [w["status"] for w in one] == [w["status"] for w in listed["watchers"]] == [expected]
    assert [w["status"] for w in ui["watchers"]] == [expected]
    # The same rule as the triggers endpoint
    assert seen == (404 if expected is None else 200)
