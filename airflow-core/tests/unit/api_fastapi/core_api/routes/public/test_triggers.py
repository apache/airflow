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
from sqlalchemy import delete

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.core_api.routes.public.triggers import _get_public_map_indexes
from airflow.jobs.job import Job
from airflow.models.asset import AssetActive, AssetModel, AssetWatcherModel
from airflow.models.deadline import Deadline
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.sdk import task
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count, count_queries
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
BASE_AUTH = "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager"


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


def _trigger(session, name, *, job=None, **state):
    trigger = Trigger(classpath=f"tests.{name}.Trigger", kwargs={"token": SECRET_KWARG}, created_date=NOW)
    trigger.triggerer_id = job.id if job else None
    for column, value in state.items():
        setattr(trigger, column, value)
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
    trigger.dag_id = ti.dag_id
    trigger.owner_ref = {"ti_id": str(ti.id)}
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
    fired = _trigger(session, "fired", job=live, last_event_at=NOW - timedelta(seconds=1))
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

    failing = _trigger(
        session,
        "failing",
        job=live,
        consecutive_failures=3,
        first_failed_at=NOW - timedelta(minutes=14),
        last_failed_at=NOW - timedelta(seconds=20),
        last_error="ConnectionError: token expired for user:***",
    )
    failing_asset = _watcher(session, "failing", failing)
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
        "failing": failing,
        "failing_asset": failing_asset,
        "pending": pending,
        "pending_asset": pending_asset,
        "removed": removed,
    }


def _by_id(body):
    return {t["id"]: t for t in body["triggers"]}


def _mapped_task_triggers(session, dag_maker, job, mapped):
    """Deferred task instances of one mapped task, each with its trigger on ``job``."""
    with dag_maker("dag_mapped_wait", serialized=True, session=session) as dag:

        @task
        def values():
            return [0]

        @task
        def wait(value):
            return value

        wait.expand(value=values())
    dr = dag_maker.create_dagrun(session=session)
    region = DynamicRegion.get_or_create(dag_id=dr.dag_id, run_id=dr.run_id, node_id="wait", session=session)
    session.add(region)
    session.flush()
    for i in range(mapped):
        trigger = _trigger(session, f"mapped{i}", job=job)
        ti = TaskInstance(
            task=dag.get_task("wait"),
            run_id=dr.run_id,
            dag_version_id=dr.created_dag_version_id,
            region_id=region.id,
            map_index=i,
        )
        ti.try_number, ti.state, ti.trigger_id = 1, TaskInstanceState.DEFERRED, trigger.id
        session.add(ti)
        session.flush()
        trigger.dag_id, trigger.owner_ref = ti.dag_id, {"ti_id": str(ti.id)}
    session.commit()


def _delete_mapped_task_instances_then_get_public_map_indexes(page, *, session):
    """Another session deletes the task instances after the page and its task instances were read."""
    with create_session() as other:
        other.execute(delete(TaskInstance).where(TaskInstance.dag_id == "dag_mapped_wait"))
    return _get_public_map_indexes(page, session=session)


class TestGetTriggers:
    def test_lists_every_kind_with_its_status(self, test_client, triggers):
        response = test_client.get("/triggers")

        assert response.status_code == 200
        assert sorted((_name(t), t["kind"], t["status"]) for t in response.json()["triggers"]) == [
            ("callback", "callback", "orphaned"),
            ("failing", "asset_watcher", "failing"),
            ("pending", "asset_watcher", "pending"),
            ("queued", "task", "pending"),
            ("sensor", "task", "running"),
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
        assert body[triggers["failing"].id]["asset_watchers"] == [
            {
                "asset_id": triggers["failing_asset"].id,
                "asset_name": "to_failing_asset",
                "asset_uri": "s3://bucket/failing",
                "watcher_name": "failing_watcher",
            }
        ]

    def test_failing_trigger_says_why_and_since_when(self, test_client, triggers):
        failing = _by_id(test_client.get("/triggers").json())[triggers["failing"].id]

        assert failing == {
            "id": triggers["failing"].id,
            "kind": "asset_watcher",
            "classpath": "tests.failing.Trigger",
            "created_date": "2026-10-21T12:00:00Z",
            "queue": None,
            "team_name": None,
            "status": "failing",
            "triggerer_hostname": "trig-a",
            "consecutive_failures": 3,
            "first_failed_at": "2026-10-21T11:46:00Z",
            "last_failed_at": "2026-10-21T11:59:40Z",
            "last_error": "ConnectionError: token expired for user:***",
            "last_event_at": None,
            "dag_id": None,
            "task_instance": None,
            "callback": None,
            "asset_watchers": failing["asset_watchers"],
        }

    @pytest.mark.parametrize(
        ("recovered", "expected", "failures"),
        [pytest.param(False, "failing", 3, id="failing"), pytest.param(True, "running", 0, id="recovered")],
    )
    def test_failing_until_the_trigger_recovers(self, test_client, triggers, recovered, expected, failures):
        failing_id = triggers["failing"].id
        if recovered:
            # What the triggerer records once the trigger fires an event or stays up long enough.
            Trigger.clear_recorded_failures({failing_id}, triggerer_id=triggers["live"].id)

        listed = _by_id(test_client.get("/triggers").json())[failing_id]
        filtered = {
            status: [
                t["id"] for t in test_client.get("/triggers", params={"status": status}).json()["triggers"]
            ]
            for status in ("failing", "running")
        }

        assert (listed["status"], listed["consecutive_failures"]) == (expected, failures)
        # The last error is kept after a recovery.
        assert listed["last_error"] == "ConnectionError: token expired for user:***"
        assert (failing_id in filtered["failing"], failing_id in filtered["running"]) == (
            expected == "failing",
            expected == "running",
        )

    def test_a_trigger_turns_orphaned_when_its_triggerer_stops(self, test_client, session, triggers):
        session.get(Job, triggers["live"].id).latest_heartbeat = NOW - timedelta(seconds=600)
        session.commit()

        body = test_client.get("/triggers", params={"status": "orphaned"}).json()

        assert sorted(_name(t) for t in body["triggers"]) == ["callback", "failing", "sensor"]

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
            pytest.param([("triggerer_hostname", "trig-a")], ["failing", "sensor"], id="triggerer"),
            pytest.param(
                [("triggerer_hostname", "trig-a"), ("kind", "task")], ["sensor"], id="triggerer-and-kind"
            ),
            pytest.param([("dag_id", "dag_queued")], ["queued"], id="dag"),
            pytest.param([("dag_id", "dag_sensor")], ["callback", "sensor"], id="dag-of-task-and-callback"),
            pytest.param(
                [("kind", "asset_watcher"), ("status", "failing")], ["failing"], id="failing-watchers"
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
        ("column", "low", "high"),
        [
            pytest.param("consecutive_failures", 1, 3, id="failures"),
            pytest.param(
                "last_failed_at", NOW - timedelta(minutes=5), NOW - timedelta(seconds=20), id="failed"
            ),
            pytest.param("last_event_at", NOW - timedelta(hours=1), NOW - timedelta(seconds=1), id="event"),
        ],
    )
    def test_sorts_a_value_never_set_as_the_smallest_on_every_database(
        self, test_client, session, triggers, column, low, high
    ):
        setattr(session.get(Trigger, triggers["sensor"].id), column, low)
        setattr(session.get(Trigger, triggers["failing"].id), column, high)
        session.commit()
        unset = sorted(t.id for t in (triggers["queued"], triggers["callback"].trigger, triggers["pending"]))

        def ids(order_by):
            response = test_client.get("/triggers", params={"order_by": order_by})
            return [t["id"] for t in response.json()["triggers"]]

        assert ids(f"-{column}") == [triggers["failing"].id, triggers["sensor"].id, *reversed(unset)]
        assert ids(column) == [*unset, triggers["sensor"].id, triggers["failing"].id]

    @pytest.mark.parametrize("order_by", ["kind", "status", "classpath"])
    def test_rejects_sorting_on_anything_else(self, test_client, triggers, order_by):
        assert test_client.get("/triggers", params={"order_by": order_by}).status_code == 400

    @pytest.mark.parametrize("params", [{"status": "crashed"}, {"kind": "sensor"}])
    def test_rejects_an_unknown_status_or_kind(self, test_client, triggers, params):
        assert test_client.get("/triggers", params=params).status_code == 422

    @pytest.mark.parametrize(
        ("dag_ids", "asset_ids", "expected"),
        [
            pytest.param({"dag_sensor"}, set(), {"sensor", "callback"}, id="one-dag"),
            pytest.param(set(), {"failing"}, {"failing"}, id="one-asset"),
            pytest.param(set(), set(), set(), id="nothing"),
        ],
    )
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets", autospec=True)
    @mock.patch(f"{BASE_AUTH}.get_authorized_dag_ids", autospec=True)
    def test_shows_a_trigger_to_whoever_can_see_its_owner(
        self, mock_dag_ids, mock_assets, test_client, triggers, dag_ids, asset_ids, expected
    ):
        mock_dag_ids.return_value = dag_ids
        mock_assets.return_value = {triggers[f"{name}_asset"].id for name in asset_ids}

        body = test_client.get("/triggers").json()

        assert {_name(t) for t in body["triggers"]} == expected
        assert body["total_entries"] == len(expected)

    @pytest.mark.parametrize(
        ("unreadable", "expected"),
        [
            pytest.param(
                {DagAccessEntity.TASK_INSTANCE},
                {"callback", "failing", "pending"},
                id="no-task-instance-read",
            ),
            pytest.param(
                {DagAccessEntity.RUN}, {"sensor", "queued", "failing", "pending"}, id="no-dag-run-read"
            ),
            pytest.param(
                {DagAccessEntity.TASK_INSTANCE, DagAccessEntity.RUN}, {"failing", "pending"}, id="assets-only"
            ),
        ],
    )
    @mock.patch(f"{SIMPLE_AUTH}.is_authorized_dag", autospec=True)
    def test_a_trigger_needs_read_on_its_owner_as_the_owner_routes_check_it(
        self, mock_is_authorized_dag, test_client, triggers, unreadable, expected
    ):
        mock_is_authorized_dag.side_effect = lambda _self, *, method, user, access_entity=None, details=None: (
            access_entity not in unreadable
        )

        body = test_client.get("/triggers").json()
        detail_codes = {
            name: test_client.get(f"/triggers/{triggers[name].id}").status_code
            for name in ("sensor", "queued", "failing", "pending")
        }

        assert {_name(t) for t in body["triggers"]} == expected
        assert body["total_entries"] == len(expected)
        assert detail_codes == {name: 200 if name in expected else 404 for name in detail_codes}

    @mock.patch(f"{SIMPLE_AUTH}.is_authorized_dag", autospec=True)
    def test_a_task_trigger_is_not_shown_through_an_asset_it_also_watches(
        self, mock_is_authorized_dag, test_client, session, triggers
    ):
        _watcher(session, "sensor", triggers["sensor"])
        session.commit()
        mock_is_authorized_dag.side_effect = lambda _self, *, method, user, access_entity=None, details=None: (
            access_entity != DagAccessEntity.TASK_INSTANCE
        )

        body = test_client.get("/triggers", params={"kind": "task"}).json()

        assert body == {"triggers": [], "total_entries": 0}

    @mock.patch(f"{SIMPLE_AUTH}.is_authorized_asset", autospec=True, return_value=False)
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets", autospec=True)
    def test_assets_listed_without_asset_read_show_no_watchers(self, mock_assets, _, test_client, triggers):
        # The FAB auth manager lists every asset id and leaves the asset read check to the route, as GET /assets does.
        mock_assets.return_value = {triggers["failing_asset"].id, triggers["pending_asset"].id}

        body = test_client.get("/triggers").json()

        assert [t for t in body["triggers"] if t["asset_watchers"]] == []

    @pytest.mark.parametrize(
        ("readable", "expected"),
        [
            pytest.param({"failing"}, ["failing_watcher"], id="one-of-its-assets"),
            pytest.param({"failing", "shared"}, ["failing_watcher", "shared_watcher"], id="both"),
        ],
    )
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets", autospec=True)
    @mock.patch(f"{BASE_AUTH}.get_authorized_dag_ids", autospec=True)
    def test_a_shared_trigger_lists_only_the_watchers_the_caller_can_read(
        self, mock_dag_ids, mock_assets, test_client, session, triggers, readable, expected
    ):
        assets = {
            "failing": triggers["failing_asset"],
            "shared": _watcher(session, "shared", triggers["failing"]),
        }
        session.commit()
        mock_dag_ids.return_value = set()
        mock_assets.return_value = {assets[name].id for name in readable}
        failing_id = triggers["failing"].id

        listed = test_client.get("/triggers").json()["triggers"]
        one = test_client.get(f"/triggers/{failing_id}").json()

        assert [t["id"] for t in listed] == [failing_id]
        assert [w["watcher_name"] for w in listed[0]["asset_watchers"]] == expected
        assert [w["watcher_name"] for w in one["asset_watchers"]] == expected

    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets", autospec=True)
    @mock.patch(f"{BASE_AUTH}.get_authorized_dag_ids", autospec=True)
    def test_filter_by_an_asset_the_caller_cannot_read_finds_nothing(
        self, mock_dag_ids, mock_assets, test_client, session, triggers
    ):
        shared_asset = _watcher(session, "shared", triggers["failing"])
        session.commit()
        mock_dag_ids.return_value = set()
        mock_assets.return_value = {triggers["failing_asset"].id}

        response = test_client.get("/triggers", params={"asset_id": shared_asset.id})

        assert response.json() == {"triggers": [], "total_entries": 0}

    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_teams", autospec=True, return_value={"team_x"})
    @mock.patch(f"{SIMPLE_AUTH}.get_authorized_assets", autospec=True, return_value=set())
    @mock.patch(f"{BASE_AUTH}.get_authorized_dag_ids", autospec=True)
    def test_a_user_who_can_read_a_dag_sees_its_triggers_whatever_their_team(
        self, mock_dag_ids, _assets, _teams, test_client, session, triggers
    ):
        session.add(Team(name="team_y"))
        session.flush()
        session.get(Trigger, triggers["sensor"].id).team_name = "team_y"
        session.get(Trigger, triggers["callback"].trigger_id).team_name = "team_y"
        session.commit()
        mock_dag_ids.return_value = {"dag_sensor"}

        with conf_vars({("core", "multi_team"): "True"}):
            body = test_client.get("/triggers").json()
            one = test_client.get(f"/triggers/{triggers['sensor'].id}")

        assert sorted((_name(t), t["team_name"]) for t in body["triggers"]) == [
            ("callback", "team_y"),
            ("sensor", "team_y"),
        ]
        assert one.status_code == 200

    @pytest.mark.parametrize(
        ("teams", "expected"),
        [
            pytest.param(["team_x"], ["failing"], id="team-x"),
            pytest.param(["team_y"], ["callback", "sensor"], id="team-y"),
            pytest.param(["team_x", "team_y"], ["callback", "failing", "sensor"], id="both-teams"),
            pytest.param(["team_z"], [], id="no-trigger-of-that-team"),
        ],
    )
    def test_filter_by_teams(self, test_client, session, triggers, teams, expected):
        session.add_all([Team(name="team_x"), Team(name="team_y")])
        session.flush()
        session.get(Trigger, triggers["failing"].id).team_name = "team_x"
        session.get(Trigger, triggers["sensor"].id).team_name = "team_y"
        session.get(Trigger, triggers["callback"].trigger_id).team_name = "team_y"
        session.commit()

        response = test_client.get("/triggers", params={"teams": teams})

        assert sorted(_name(t) for t in response.json()["triggers"]) == expected
        assert response.json()["total_entries"] == len(expected)

    @mock.patch(f"{BASE_AUTH}.get_authorized_dag_ids", autospec=True)
    def test_filter_by_teams_shows_only_what_the_caller_can_see(
        self, mock_dag_ids, test_client, session, triggers
    ):
        session.add(Team(name="team_y"))
        session.flush()
        session.get(Trigger, triggers["sensor"].id).team_name = "team_y"
        session.get(Trigger, triggers["queued"].id).team_name = "team_y"
        session.commit()
        mock_dag_ids.return_value = {"dag_queued"}

        response = test_client.get("/triggers", params={"teams": ["team_y"]})

        assert [_name(t) for t in response.json()["triggers"]] == ["queued"]

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

    def test_query_count_does_not_grow_with_mapped_task_triggers(
        self, test_client, session, triggers, dag_maker
    ):
        mapped = 4
        _mapped_task_triggers(session, dag_maker, triggers["live"], mapped)

        with count_queries() as one:
            test_client.get("/triggers", params={"dag_id": "dag_mapped_wait", "limit": 1})
        with count_queries() as page:
            response = test_client.get("/triggers", params={"dag_id": "dag_mapped_wait", "limit": mapped})

        assert sum(page.values()) == sum(one.values())

        assert response.status_code == 200
        map_indexes = sorted(t["task_instance"]["map_index"] for t in response.json()["triggers"])
        assert map_indexes == list(range(mapped))

    @mock.patch(
        "airflow.api_fastapi.core_api.routes.public.triggers._get_public_map_indexes",
        autospec=True,
        side_effect=_delete_mapped_task_instances_then_get_public_map_indexes,
    )
    def test_a_task_instance_deleted_while_the_page_is_read_is_left_out(
        self, _, test_client, session, triggers, dag_maker
    ):
        _mapped_task_triggers(session, dag_maker, triggers["live"], 2)

        response = test_client.get("/triggers", params={"dag_id": "dag_mapped_wait"})

        assert response.status_code == 200
        assert response.json()["triggers"] == []

    def test_should_respond_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get("/triggers").status_code == 401

    def test_should_respond_403_to_a_user_who_can_read_no_owner(self, unauthorized_test_client, triggers):
        assert unauthorized_test_client.get("/triggers").status_code == 403
        assert unauthorized_test_client.get(f"/triggers/{triggers['sensor'].id}").status_code == 403


class TestGetTrigger:
    def test_get_one(self, test_client, triggers):
        response = test_client.get(f"/triggers/{triggers['sensor'].id}")

        assert response.status_code == 200
        body = response.json()
        assert (body["kind"], body["status"], body["triggerer_hostname"]) == ("task", "running", "trig-a")
        assert body["task_instance"]["state"] == "deferred"

    @mock.patch(f"{BASE_AUTH}.get_authorized_dag_ids", autospec=True)
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
    response = test_client.get(f"/assets/{triggers['failing_asset'].id}")

    assert response.json()["watchers"] == [
        {
            "name": "failing_watcher",
            "trigger_id": triggers["failing"].id,
            "created_date": "2026-10-21T12:00:00Z",
            "status": "failing",
        }
    ]
