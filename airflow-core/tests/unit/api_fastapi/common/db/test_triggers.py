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

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.triggers import (
    build_has_owner_expression,
    build_trigger_dag_id_expression,
    build_trigger_kind_expression,
    build_trigger_status_expression,
    get_trigger_kind,
    get_trigger_status,
)
from airflow.jobs.job import Job
from airflow.models.asset import AssetModel
from airflow.models.deadline import Deadline
from airflow.models.trigger import Trigger
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_callbacks,
    clear_db_deadline,
    clear_db_jobs,
    clear_db_runs,
    clear_db_triggers,
)

pytestmark = pytest.mark.db_test


async def notify(**kwargs):
    """A triggerer callback."""


@pytest.fixture(autouse=True)
def _clean_db():
    def clear():
        clear_db_deadline()
        clear_db_callbacks()
        clear_db_assets()
        clear_db_triggers()
        clear_db_runs()
        clear_db_jobs()

    clear()
    yield
    clear()


def _trigger(session) -> Trigger:
    trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    session.add(trigger)
    session.flush()
    return trigger


@pytest.mark.parametrize(
    ("owner", "expected"),
    [
        pytest.param(None, "pending", id="unassigned"),
        pytest.param("alive", "running", id="running"),
        pytest.param("stale", "orphaned", id="stale-heartbeat"),
        pytest.param("ended", "orphaned", id="ended"),
        pytest.param("gone", "orphaned", id="job-row-gone"),
        pytest.param("no-heartbeat", "orphaned", id="never-heartbeated"),
    ],
)
def test_status_is_the_same_in_sql_and_python(session, owner, expected):
    now = timezone.utcnow()
    trigger = _trigger(session)
    if owner is not None:
        job = Job(job_type="TriggererJob")
        job.latest_heartbeat = (
            None
            if owner == "no-heartbeat"
            else now - datetime.timedelta(seconds=600 if owner == "stale" else 1)
        )
        if owner == "ended":
            job.end_date = now
        session.add(job)
        session.flush()
        trigger.triggerer_id = job.id + 1000 if owner == "gone" else job.id
    session.commit()

    sql_status = session.scalar(
        select(build_trigger_status_expression())
        .select_from(Trigger)
        .outerjoin(Trigger.triggerer_job)
        .where(Trigger.id == trigger.id)
    )
    session.expire_all()

    assert sql_status == expected
    assert get_trigger_status(session.get(Trigger, trigger.id)).value == expected


@pytest.mark.parametrize(
    ("owner", "expected_kind", "expected_dag_id"),
    [
        pytest.param("task", "task", "dag_owner", id="task"),
        pytest.param("resumed-task", None, None, id="task-resumed"),
        pytest.param("callback", "callback", "dag_owner", id="callback"),
        pytest.param("watcher", "asset_watcher", None, id="asset-watcher"),
        pytest.param(None, None, None, id="no-owner"),
    ],
)
def test_kind_and_dag_are_the_same_in_sql_and_python(
    session, create_task_instance, owner, expected_kind, expected_dag_id
):
    trigger = _trigger(session)
    if owner in ("task", "resumed-task", "callback"):
        ti = create_task_instance(dag_id="dag_owner", state=TaskInstanceState.DEFERRED, session=session)
        if owner == "task":
            ti.trigger_id = trigger.id
        elif owner == "callback":
            deadline = Deadline(
                deadline_time=timezone.utcnow(),
                callback=AsyncCallback(notify),
                dagrun_id=ti.dag_run.id,
                deadline_alert_id=None,
            )
            deadline.callback.trigger = trigger
            session.add(deadline)
    elif owner == "watcher":
        asset = AssetModel("watched")
        asset.add_trigger(trigger, "watcher")
        session.add(asset)
    session.commit()

    sql_kind, sql_dag_id, sql_has_owner = session.execute(
        select(
            build_trigger_kind_expression(), build_trigger_dag_id_expression(), build_has_owner_expression()
        )
        .select_from(Trigger)
        .where(Trigger.id == trigger.id)
    ).one()
    session.expire_all()
    kind = get_trigger_kind(session.get(Trigger, trigger.id))

    assert (sql_kind, sql_dag_id, bool(sql_has_owner)) == (
        expected_kind,
        expected_dag_id,
        expected_kind is not None,
    )
    assert (kind.value if kind else None) == expected_kind
