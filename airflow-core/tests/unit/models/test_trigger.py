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

import datetime
import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import patch

import pendulum
import pytest
import pytz
from cryptography.fernet import Fernet
from sqlalchemy import delete, func, select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import TaskInstance, Trigger
from airflow.models.asset import AssetEvent, AssetModel, AssetWatcherModel
from airflow.models.callback import Callback, TriggererCallback
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.triggers.base import (
    BaseTrigger,
    TaskFailedEvent,
    TaskSkippedEvent,
    TaskSuccessEvent,
    TriggerEvent,
)
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


@pytest.fixture(autouse=True)
def clear_db(session):
    session.execute(delete(TaskInstance))
    session.execute(delete(AssetWatcherModel))
    session.execute(delete(Callback))
    session.execute(delete(Trigger))
    session.execute(delete(AssetModel))
    session.execute(delete(AssetEvent))
    session.execute(delete(Job))
    yield session
    session.execute(delete(TaskInstance))
    session.execute(delete(AssetWatcherModel))
    session.execute(delete(Callback))
    session.execute(delete(Trigger))
    session.execute(delete(AssetModel))
    session.execute(delete(AssetEvent))
    session.execute(delete(Job))
    session.commit()


def test_fetch_trigger_ids_with_non_task_associations(session):
    # Create triggers
    asset_trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger1", kwargs={})
    callback_trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger2", kwargs={})
    other_trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger3", kwargs={})
    session.add_all([asset_trigger, callback_trigger, other_trigger])
    session.commit()

    # Create callback association
    callback = TriggererCallback(callback_def=AsyncCallback("classpath.log.error"))
    callback.trigger = callback_trigger
    session.add(callback)

    # Create asset association
    asset = AssetModel("test")
    asset.add_trigger(asset_trigger, "test_asset_watcher")
    session.add(asset)

    session.commit()
    results = Trigger.fetch_trigger_ids_with_non_task_associations()
    assert results == {asset_trigger.id, callback_trigger.id}


def test_clean_unused(session, dag_maker):
    """
    Tests that unused triggers (those with no task instances referencing them)
    are cleaned out automatically.
    """
    # Create triggers
    trigger1 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger1", kwargs={})
    trigger2 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger2", kwargs={})
    trigger3 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger3", kwargs={})
    trigger4 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger4", kwargs={})
    trigger5 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger5", kwargs={})
    trigger6 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger6", kwargs={})
    session.add(trigger1)
    session.add(trigger2)
    session.add(trigger3)
    session.add(trigger4)
    session.add(trigger5)
    session.add(trigger6)
    session.flush()
    assert session.scalar(select(func.count()).select_from(Trigger)) == 6

    # Tie one to a fake TaskInstance that is not deferred, and one to one that is
    with dag_maker(session=session):
        EmptyOperator(task_id="fake0")
        EmptyOperator(task_id="fake1")
        EmptyOperator(task_id="fake2")

    dr = dag_maker.create_dagrun(logical_date=timezone.utcnow())
    tis = {ti.task_id: ti for ti in dr.task_instances}
    tis["fake0"].state = State.DEFERRED
    tis["fake0"].trigger_id = trigger1.id
    tis["fake1"].state = State.SUCCESS
    tis["fake1"].trigger_id = trigger2.id
    tis["fake2"].state = State.SUCCESS
    tis["fake2"].trigger_id = trigger4.id
    session.flush()

    # Create assets
    asset = AssetModel("test")
    asset.add_trigger(trigger4, "test_asset_watcher1")
    asset.add_trigger(trigger5, "test_asset_watcher2")
    session.add(asset)
    session.flush()
    assert session.scalar(select(func.count()).select_from(AssetModel)) == 1

    # Create callback with trigger
    callback = TriggererCallback(callback_def=AsyncCallback("classpath.callback"))
    callback.trigger = trigger6
    session.add(callback)
    session.flush()

    # Run clear operation
    Trigger.clean_unused(session=session)
    results = session.scalars(select(Trigger)).all()
    assert len(results) == 4
    assert {result.id for result in results} == {trigger1.id, trigger4.id, trigger5.id, trigger6.id}


@patch.object(TriggererCallback, "handle_event")
def test_submit_event(mock_callback_handle_event, session, create_task_instance):
    """
    Tests that events submitted to a trigger re-wake their dependent
    task instances and notify associated assets and callbacks.
    """
    # Make a trigger
    trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    session.add(trigger)
    # Make a TaskInstance that's deferred and waiting on it
    task_instance = create_task_instance(
        session=session, logical_date=timezone.utcnow(), state=State.DEFERRED
    )
    task_instance.trigger_id = trigger.id
    task_instance.next_kwargs = {"cheesecake": True}
    # Create assets
    asset = AssetModel("test")
    asset.add_trigger(trigger, "test_asset_watcher")
    session.add(asset)

    # Create a callback with the same trigger
    callback = TriggererCallback(
        callback_def=AsyncCallback("classpath.callback"),
    )
    callback.trigger = trigger
    session.add(callback)
    session.commit()

    # Check that the asset has 0 event prior to sending an event to the trigger
    assert (
        session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asset.id))
        == 0
    )

    # Create event
    payload = "payload"
    event = TriggerEvent(payload)
    # Call submit_event
    Trigger.submit_event(trigger.id, event, session=session)
    # commit changes made by submit event and expire all cache to read from db.
    session.flush()
    # Check that the task instance is now scheduled
    session.refresh(task_instance)
    assert task_instance.state == State.SCHEDULED
    assert task_instance.next_kwargs == {"event": payload, "cheesecake": True}
    # Check that the asset has received an event
    assert (
        session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asset.id))
        == 1
    )
    asset_event = session.scalar(select(AssetEvent).where(AssetEvent.asset_id == asset.id))
    assert asset_event.extra == {"from_trigger": True, "payload": payload}

    # Check that the callback's handle_event was called
    mock_callback_handle_event.assert_called_once_with(event, session)


def test_submit_failure(session, create_task_instance):
    """
    Tests that failures submitted to a trigger fail their dependent
    task instances if not using a TaskEndEvent.
    """
    # Make a trigger
    trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    session.add(trigger)
    # Make a TaskInstance that's deferred and waiting on it
    task_instance = create_task_instance(task_id="fake", logical_date=timezone.utcnow(), state=State.DEFERRED)
    task_instance.trigger_id = trigger.id
    session.commit()
    # Call submit_event
    Trigger.submit_failure(trigger.id, session=session)
    # Check that the task instance is now scheduled to fail
    updated_task_instance = session.scalar(select(TaskInstance))
    assert updated_task_instance.state == State.SCHEDULED
    assert updated_task_instance.next_method == "__fail__"


@pytest.mark.parametrize(
    ("event_cls", "expected"),
    [
        (TaskSuccessEvent, "success"),
        (TaskFailedEvent, "failed"),
        (TaskSkippedEvent, "skipped"),
    ],
)
@patch("airflow._shared.timezones.timezone.utcnow")
def test_submit_event_task_end(mock_utcnow, session, create_task_instance, event_cls, expected):
    """
    Tests that events inheriting BaseTaskEndEvent *don't* re-wake their dependent
    but mark them in the appropriate terminal state and send xcom
    """
    now = pendulum.now("UTC")
    mock_utcnow.return_value = now

    # Make a trigger
    trigger = Trigger(classpath="does.not.matter", kwargs={})
    session.add(trigger)
    # Make a TaskInstance that's deferred and waiting on it
    task_instance = create_task_instance(
        session=session, logical_date=timezone.utcnow(), state=State.DEFERRED
    )
    task_instance.trigger_id = trigger.id
    session.commit()

    def get_xcoms(ti):
        return session.scalars(
            XComModel.get_many(dag_ids=[ti.dag_id], task_ids=[ti.task_id], run_id=ti.run_id)
        ).all()

    # now for the real test
    # first check initial state
    ti: TaskInstance = session.scalar(select(TaskInstance))
    assert ti.state == "deferred"
    assert get_xcoms(ti) == []

    session.flush()
    # now, for each type, submit event
    # verify that (1) task ends in right state and (2) xcom is pushed
    Trigger.submit_event(
        trigger.id, event_cls(xcoms={"return_value": "xcomret", "a": "b", "c": "d"}), session=session
    )
    # commit changes made by submit event and expire all cache to read from db.
    session.flush()
    # Check that the task instance is now correct
    ti = session.scalar(select(TaskInstance))
    assert ti.state == expected
    assert ti.next_kwargs is None
    assert ti.end_date == now
    assert ti.duration is not None
    actual_xcoms = {x.key: x.value for x in get_xcoms(ti)}
    expected_xcoms = {}
    for k, v in {"return_value": "xcomret", "a": "b", "c": "d"}.items():
        expected_xcoms[k] = json.dumps(v)
    assert actual_xcoms == expected_xcoms


@pytest.mark.need_serialized_dag
def test_assign_unassigned(session, create_task_instance):
    """
    Tests that unassigned triggers of all appropriate states are assigned.
    """
    time_now = timezone.utcnow()
    triggerer_heartrate = 10
    finished_triggerer = Job(heartrate=triggerer_heartrate, state=State.SUCCESS)
    TriggererJobRunner(finished_triggerer)
    finished_triggerer.end_date = time_now - datetime.timedelta(hours=1)
    session.add(finished_triggerer)
    assert not finished_triggerer.is_alive()
    healthy_triggerer = Job(heartrate=triggerer_heartrate, state=State.RUNNING)
    valid_trigger_queues = {"default", "custom_queue_name_1"}
    TriggererJobRunner(healthy_triggerer, trigger_queues=valid_trigger_queues)
    session.add(healthy_triggerer)
    assert healthy_triggerer.is_alive()
    new_triggerer = Job(heartrate=triggerer_heartrate, state=State.RUNNING)
    TriggererJobRunner(new_triggerer, trigger_queues=valid_trigger_queues)
    session.add(new_triggerer)
    assert new_triggerer.is_alive()
    # This trigger's last heartbeat is older than the check threshold, expect
    # its triggers to be taken by other healthy triggerers below
    unhealthy_triggerer = Job(
        heartrate=triggerer_heartrate,
        state=State.RUNNING,
        latest_heartbeat=time_now - datetime.timedelta(seconds=100),
    )
    TriggererJobRunner(unhealthy_triggerer)
    session.add(unhealthy_triggerer)
    # Triggerer is not healtht, its last heartbeat was too long ago
    assert not unhealthy_triggerer.is_alive()
    session.commit()

    def _create_test_trigger(
        name: str,
        logical_date: datetime,
        classpath: str,
        trigger_kwargs: dict[str, Any],
        triggerer_id: int | None = None,
        trigger_queue: str | None = None,
    ) -> Trigger:
        """Creates individual test trigger instances."""
        trig = Trigger(classpath=classpath, kwargs=trigger_kwargs, trigger_queue=trigger_queue)
        trig.triggerer_id = triggerer_id
        session.add(trig)
        ti = create_task_instance(task_id=f"ti_{name}", logical_date=logical_date, run_id=f"{name}_run_id")
        ti.trigger_id = trig.id
        session.add(ti)
        return trig

    trigger_on_healthy_triggerer = _create_test_trigger(
        name="trigger_on_healthy_triggerer",
        logical_date=time_now,
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=healthy_triggerer.id,
    )

    trigger_explicit_queue_on_healthy_triggerer = _create_test_trigger(
        name="trigger_explicit_queue_on_healthy_triggerer",
        logical_date=time_now + datetime.timedelta(minutes=30),
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=healthy_triggerer.id,
        trigger_queue="custom_queue_name_1",
    )

    trigger_on_unhealthy_triggerer = _create_test_trigger(
        name="trigger_on_unhealthy_triggerer",
        logical_date=time_now + datetime.timedelta(hours=1),
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=unhealthy_triggerer.id,
    )

    trigger_on_killed_triggerer = _create_test_trigger(
        name="trigger_on_killed_triggerer",
        logical_date=time_now + datetime.timedelta(hours=2),
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=finished_triggerer.id,
    )

    trigger_unassigned_to_triggerer = _create_test_trigger(
        name="trigger_unassigned_to_triggerer",
        logical_date=time_now + datetime.timedelta(hours=3),
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=None,
    )

    trigger_explicit_valid_queue_unassigned_to_triggerer = _create_test_trigger(
        name="trigger_explicit_valid_queue_unassigned_to_triggerer",
        logical_date=time_now + datetime.timedelta(hours=4),
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=None,
        trigger_queue="custom_queue_name_1",
    )

    trigger_explicit_bad_queue_unassigned_to_triggerer = _create_test_trigger(
        name="trigger_explicit_bad_queue_unassigned_to_triggerer",
        logical_date=time_now + datetime.timedelta(hours=5),
        classpath="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        triggerer_id=None,
        trigger_queue="bad_queue_name",
    )

    assert trigger_unassigned_to_triggerer.triggerer_id is None
    assert trigger_explicit_valid_queue_unassigned_to_triggerer.triggerer_id is None
    assert trigger_explicit_bad_queue_unassigned_to_triggerer.triggerer_id is None
    session.commit()
    assert session.scalar(select(func.count()).select_from(Trigger)) == 7
    Trigger.assign_unassigned(
        new_triggerer.id, trigger_queues=valid_trigger_queues, capacity=100, health_check_threshold=30
    )
    session.expire_all()
    # Check that trigger on killed triggerer and unassigned trigger are assigned to new triggerer
    assert (
        session.scalar(select(Trigger).where(Trigger.id == trigger_on_killed_triggerer.id)).triggerer_id
        == new_triggerer.id
    )
    assert (
        session.scalar(select(Trigger).where(Trigger.id == trigger_unassigned_to_triggerer.id)).triggerer_id
        == new_triggerer.id
    )
    # Check that unassigned trigger with a valid queue value is assigned to new triggerer
    assert (
        session.query(Trigger)
        .filter(Trigger.id == trigger_explicit_valid_queue_unassigned_to_triggerer.id)
        .one()
        .triggerer_id
        == new_triggerer.id
    )
    # Check that unassigned trigger with a queue value which has no consuming triggerers remains unassigned
    assert (
        session.query(Trigger)
        .filter(Trigger.id == trigger_explicit_bad_queue_unassigned_to_triggerer.id)
        .one()
        .triggerer_id
        is None
    )
    # Check that trigger on healthy triggerer still assigned to existing triggerer
    assert (
        session.scalar(select(Trigger).where(Trigger.id == trigger_on_healthy_triggerer.id)).triggerer_id
        == healthy_triggerer.id
    )
    # Check that trigger with explicit, non-default queue value on healthy triggerer still assigned to
    # existing triggerer
    assert (
        session.query(Trigger)
        .filter(Trigger.id == trigger_explicit_queue_on_healthy_triggerer.id)
        .one()
        .triggerer_id
        == healthy_triggerer.id
    )
    # Check that trigger on unhealthy triggerer is assigned to new triggerer
    assert (
        session.scalar(select(Trigger).where(Trigger.id == trigger_on_unhealthy_triggerer.id)).triggerer_id
        == new_triggerer.id
    )


@pytest.mark.need_serialized_dag
def test_get_sorted_triggers_same_priority_weight(session, create_task_instance):
    """
    Tests that triggers are sorted by the creation_date if they have the same priority.
    """
    old_logical_date = datetime.datetime(
        2023, 5, 9, 12, 16, 14, 474415, tzinfo=pytz.timezone("Africa/Abidjan")
    )
    trigger_old = Trigger(
        classpath="airflow.triggers.testing.SuccessTrigger",
        kwargs={},
        created_date=old_logical_date + datetime.timedelta(seconds=30),
    )
    session.add(trigger_old)
    TI_old = create_task_instance(
        task_id="old",
        logical_date=old_logical_date,
        run_id="old_run_id",
    )
    TI_old.priority_weight = 1
    TI_old.trigger_id = trigger_old.id
    session.add(TI_old)

    new_logical_date = datetime.datetime(
        2023, 5, 9, 12, 17, 14, 474415, tzinfo=pytz.timezone("Africa/Abidjan")
    )
    trigger_new = Trigger(
        classpath="airflow.triggers.testing.SuccessTrigger",
        kwargs={},
        created_date=new_logical_date + datetime.timedelta(seconds=30),
    )
    session.add(trigger_new)
    TI_new = create_task_instance(
        task_id="new",
        logical_date=new_logical_date,
        run_id="new_run_id",
    )
    TI_new.priority_weight = 1
    TI_new.trigger_id = trigger_new.id
    session.add(TI_new)
    trigger_orphan = Trigger(
        classpath="airflow.triggers.testing.TriggerOrphan",
        kwargs={},
        created_date=new_logical_date,
    )
    session.add(trigger_orphan)
    trigger_asset = Trigger(
        classpath="airflow.triggers.testing.TriggerAsset",
        kwargs={},
        created_date=new_logical_date,
    )
    session.add(trigger_asset)
    trigger_callback = Trigger(
        classpath="airflow.triggers.testing.TriggerCallback",
        kwargs={},
        created_date=new_logical_date,
    )
    session.add(trigger_callback)
    session.commit()
    assert session.scalar(select(func.count()).select_from(Trigger)) == 5
    # Create assets
    asset = AssetModel("test")
    asset.add_trigger(trigger_asset, "test_asset_watcher")
    session.add(asset)
    # Create callback with trigger
    callback = TriggererCallback(callback_def=AsyncCallback("classpath.callback"))
    callback.trigger = trigger_callback
    session.add(callback)
    session.commit()

    trigger_ids_query = Trigger.get_sorted_triggers(
        capacity=100, trigger_queues={"default"}, alive_triggerer_ids=[], session=session
    )

    # Callback triggers should be first, followed by task triggers, then asset triggers
    assert trigger_ids_query == [
        (trigger_callback.id,),
        (trigger_old.id,),
        (trigger_new.id,),
        (trigger_asset.id,),
    ]


@pytest.mark.need_serialized_dag
def test_get_sorted_triggers_different_priority_weights(session, create_task_instance):
    """
    Tests that triggers are sorted by the priority_weight.
    """
    callback_triggers = [
        Trigger(classpath="airflow.triggers.testing.CallbackTrigger", kwargs={}),
        Trigger(classpath="airflow.triggers.testing.CallbackTrigger", kwargs={}),
        Trigger(classpath="airflow.triggers.testing.CallbackTrigger", kwargs={}),
    ]
    session.add_all(callback_triggers)
    session.flush()

    callbacks = [
        TriggererCallback(callback_def=AsyncCallback("classpath.low"), priority_weight=1),
        TriggererCallback(callback_def=AsyncCallback("classpath.mid"), priority_weight=5),
        TriggererCallback(callback_def=AsyncCallback("classpath.high"), priority_weight=10),
    ]
    for callback, trigger in zip(callbacks, callback_triggers):
        callback.trigger = trigger
    session.add_all(callbacks)

    old_logical_date = datetime.datetime(
        2023, 5, 9, 12, 16, 14, 474415, tzinfo=pytz.timezone("Africa/Abidjan")
    )
    trigger_old = Trigger(
        classpath="airflow.triggers.testing.SuccessTrigger",
        kwargs={},
        created_date=old_logical_date + datetime.timedelta(seconds=30),
    )
    session.add(trigger_old)
    TI_old = create_task_instance(
        task_id="old",
        logical_date=old_logical_date,
        run_id="old_run_id",
    )
    TI_old.priority_weight = 1
    TI_old.trigger_id = trigger_old.id
    session.add(TI_old)

    new_logical_date = datetime.datetime(
        2023, 5, 9, 12, 17, 14, 474415, tzinfo=pytz.timezone("Africa/Abidjan")
    )
    trigger_new = Trigger(
        classpath="airflow.triggers.testing.SuccessTrigger",
        kwargs={},
        created_date=new_logical_date + datetime.timedelta(seconds=30),
    )
    session.add(trigger_new)
    TI_new = create_task_instance(
        task_id="new",
        logical_date=new_logical_date,
        run_id="new_run_id",
    )
    TI_new.priority_weight = 2
    TI_new.trigger_id = trigger_new.id
    session.add(TI_new)

    session.commit()
    assert session.scalar(select(func.count()).select_from(Trigger)) == 5

    trigger_ids_query = Trigger.get_sorted_triggers(
        capacity=100, trigger_queues={"default"}, alive_triggerer_ids=[], session=session
    )

    assert trigger_ids_query == [
        (callback_triggers[2].id,),
        (callback_triggers[1].id,),
        (callback_triggers[0].id,),
        (trigger_new.id,),
        (trigger_old.id,),
    ]


@pytest.mark.need_serialized_dag
def test_get_sorted_triggers_dont_starve_for_ha(session, create_task_instance):
    """
    Tests that get_sorted_triggers respects max_trigger_to_select_per_loop to prevent
    starvation in HA setups. When capacity is large, it should limit triggers per loop
    to avoid one triggerer picking up too many triggers.
    """
    # Create 20 callback triggers with different priorities
    callback_triggers = []
    for i in range(20):
        trigger = Trigger(classpath="airflow.triggers.testing.CallbackTrigger", kwargs={})
        session.add(trigger)
        session.flush()
        callback = TriggererCallback(
            callback_def=AsyncCallback(f"classpath.callback_{i}"), priority_weight=20 - i
        )
        callback.trigger = trigger
        session.add(callback)
        callback_triggers.append(trigger)

    # Create 20 task instance triggers with different priorities
    task_triggers = []
    for i in range(20):
        logical_date = datetime.datetime(2023, 5, 9, 12, i, 0, tzinfo=pytz.timezone("UTC"))
        trigger = Trigger(
            classpath="airflow.triggers.testing.SuccessTrigger",
            kwargs={},
            created_date=logical_date,
        )
        session.add(trigger)
        session.flush()
        ti = create_task_instance(
            task_id=f"task_{i}",
            logical_date=logical_date,
            run_id=f"run_{i}",
        )
        ti.priority_weight = 20 - i
        ti.trigger_id = trigger.id
        session.add(ti)
        task_triggers.append(trigger)

    # Create 20 asset triggers
    asset_triggers = []
    for i in range(20):
        logical_date = datetime.datetime(2023, 5, 9, 13, i, 0, tzinfo=pytz.timezone("UTC"))
        trigger = Trigger(
            classpath="airflow.triggers.testing.AssetTrigger",
            kwargs={},
            created_date=logical_date,
        )
        session.add(trigger)
        session.flush()
        asset = AssetModel(f"test_asset_{i}")
        asset.add_trigger(trigger, f"test_asset_watcher_{i}")
        session.add(asset)
        asset_triggers.append(trigger)

    session.commit()
    assert session.scalar(select(func.count()).select_from(Trigger)) == 60

    # Mock max_trigger_to_select_per_loop to 5 for testing
    with patch.object(Trigger, "max_trigger_to_select_per_loop", 5):
        # Test with large capacity (100) - should respect max_trigger_to_select_per_loop (5)
        # and return only 5 triggers from each category (callback, task, asset)
        trigger_ids_query = Trigger.get_sorted_triggers(
            capacity=100, trigger_queues={"default"}, alive_triggerer_ids=[], session=session
        )

        # Should get 5 callbacks (max_trigger_to_select_per_loop), then 5 tasks, then 5 assets
        # Total: 15 triggers instead of all 60
        assert len(trigger_ids_query) == 15

        # First 5 should be callback triggers (highest priority first)
        callback_ids = [t.id for t in callback_triggers[:5]]
        assert [row[0] for row in trigger_ids_query[:5]] == callback_ids

        # Next 5 should be task triggers (highest priority first)
        task_ids = [t.id for t in task_triggers[:5]]
        assert [row[0] for row in trigger_ids_query[5:10]] == task_ids

        # Last 5 should be asset triggers (earliest created_date first)
        asset_ids = [t.id for t in asset_triggers[:5]]
        assert [row[0] for row in trigger_ids_query[10:15]] == asset_ids

        # Test with capacity smaller than max_trigger_to_select_per_loop
        # Should respect capacity instead
        trigger_ids_query = Trigger.get_sorted_triggers(
            capacity=3, trigger_queues={"default"}, alive_triggerer_ids=[], session=session
        )

        # Should get only 3 callback triggers (capacity limit)
        assert len(trigger_ids_query) == 3
        assert [row[0] for row in trigger_ids_query] == callback_ids[:3]


class SensitiveKwargsTrigger(BaseTrigger):
    """
    A trigger that has sensitive kwargs.
    """

    def __init__(self, param1: str, param2: str):
        super().__init__()
        self.param1 = param1
        self.param2 = param2

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "unit.models.test_trigger.SensitiveKwargsTrigger",
            {
                "param1": self.param1,
                "param2": self.param2,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        yield TriggerEvent({})


@conf_vars({("core", "fernet_key"): Fernet.generate_key().decode()})
def test_serialize_sensitive_kwargs():
    """
    Tests that sensitive kwargs are encrypted.
    """
    trigger_instance = SensitiveKwargsTrigger(param1="value1", param2="value2")
    trigger_row: Trigger = Trigger.from_object(trigger_instance)

    assert trigger_row.kwargs["param1"] == "value1"
    assert trigger_row.kwargs["param2"] == "value2"
    assert isinstance(trigger_row.encrypted_kwargs, str)
    assert "value1" not in trigger_row.encrypted_kwargs
    assert "value2" not in trigger_row.encrypted_kwargs


def test_kwargs_not_encrypted():
    """
    Tests that we don't decrypt kwargs if they aren't encrypted.
    We weren't able to encrypt the kwargs in all migration paths.
    """
    trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    # force the `encrypted_kwargs` to be unencrypted, like they would be after an offline upgrade
    trigger.encrypted_kwargs = json.dumps(
        BaseSerialization.serialize({"param1": "value1", "param2": "value2"})
    )

    assert trigger.kwargs["param1"] == "value1"
    assert trigger.kwargs["param2"] == "value2"
