#
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

import pendulum
import pytest

from airflow.exceptions import AirflowException
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

from tests_common.test_utils import db
from tests_common.test_utils.db import clear_rendered_ti_fields

pytestmark = pytest.mark.db_test

EXTERNAL_LOGICAL_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")


@pytest.fixture(autouse=True)
def reset_db():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_serialized_dags()
    clear_rendered_ti_fields()


def test_clear_does_not_follow_external_marker_by_default(dag_maker, session):
    """Without include_dependent_dags, ExternalTaskMarker links are not followed."""
    with dag_maker("parent_dag", session=session, schedule=None):
        ExternalTaskMarker(
            task_id="trigger_child",
            external_dag_id="child_dag",
            external_task_id="wait_for_parent",
            recursion_depth=3,
        )

    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    serialized_parent = dag_maker.serialized_dag

    with dag_maker("child_dag", session=session, schedule=None):
        ExternalTaskSensor(
            task_id="wait_for_parent",
            external_dag_id="parent_dag",
            external_task_id="trigger_child",
            poke_interval=5,
        )
    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    session.flush()

    result = serialized_parent.clear(dry_run=True, only_failed=False, session=session)

    dag_ids = {ti.dag_id for ti in result}
    assert "parent_dag" in dag_ids
    assert "child_dag" not in dag_ids


def test_clear_follows_external_marker_when_include_dependent_dags_enabled(dag_maker, session):
    """With include_dependent_dags=True, clear() follows ExternalTaskMarker links into child DAGs."""
    with dag_maker("parent_dag", session=session, schedule=None):
        ExternalTaskMarker(
            task_id="trigger_child",
            external_dag_id="child_dag",
            external_task_id="wait_for_parent",
            recursion_depth=3,
        )

    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    serialized_parent = dag_maker.serialized_dag

    with dag_maker("child_dag", session=session, schedule=None):
        ExternalTaskSensor(
            task_id="wait_for_parent",
            external_dag_id="parent_dag",
            external_task_id="trigger_child",
            poke_interval=5,
        )

    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    session.flush()

    result = serialized_parent.clear(
        dry_run=True, only_failed=False, include_dependent_dags=True, session=session
    )

    dag_ids = {ti.dag_id for ti in result}
    task_ids = {ti.task_id for ti in result}

    assert "parent_dag" in dag_ids
    assert "child_dag" in dag_ids
    assert "wait_for_parent" in task_ids


def test_clear_raises_when_recursion_depth_exceeded(dag_maker, session):
    """AirflowException is raised when the dependency chain depth exceeds recursion_depth."""
    with dag_maker("parent_dag", session=session, schedule=None):
        ExternalTaskMarker(
            task_id="trigger_child",
            external_dag_id="child_dag",
            external_task_id="wait_for_parent",
            recursion_depth=1,
        )

    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    serialized_parent = dag_maker.serialized_dag

    with dag_maker("child_dag", session=session, schedule=None):
        wait_for_parent = ExternalTaskSensor(
            task_id="wait_for_parent",
            external_dag_id="parent_dag",
            external_task_id="trigger_child",
            poke_interval=5,
        )

        trigger_grandchild = ExternalTaskMarker(
            task_id="trigger_grandchild",
            external_dag_id="grandchild_dag",
            external_task_id="wait_for_child",
            recursion_depth=1,
        )

        wait_for_parent >> trigger_grandchild

    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)

    with dag_maker("grandchild_dag", session=session, schedule=None):
        ExternalTaskSensor(
            task_id="wait_for_child",
            external_dag_id="child_dag",
            external_task_id="trigger_grandchild",
            poke_interval=5,
        )

    dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    session.flush()

    with pytest.raises(AirflowException, match="Maximum recursion depth"):
        serialized_parent.clear(dry_run=True, only_failed=False, include_dependent_dags=True, session=session)


def test_clear_uses_rendered_fields_for_custom_logical_date_template(dag_maker, session):
    """When ExternalTaskMarker has a non-default logical_date template, the rendered value from
    RenderedTaskInstanceFields is used to locate the child DagRun."""
    child_logical_date = pendulum.datetime(2024, 1, 2, tz="UTC")
    custom_template = "{{ (logical_date + macros.timedelta(days=1)).isoformat() }}"

    with dag_maker("parent_dag", session=session, schedule=None):
        ExternalTaskMarker(
            task_id="trigger_child",
            external_dag_id="child_dag",
            external_task_id="wait_for_parent",
            logical_date=custom_template,
            recursion_depth=3,
        )

    parent_run = dag_maker.create_dagrun(logical_date=EXTERNAL_LOGICAL_DATE)
    serialized_parent = dag_maker.serialized_dag

    # Store the rendered logical_date so the code can resolve the child's DagRun date.
    parent_ti = next(ti for ti in parent_run.task_instances if ti.task_id == "trigger_child")
    parent_ti.refresh_from_task(serialized_parent.get_task("trigger_child"))
    rtif = RenderedTaskInstanceFields(
        ti=parent_ti,
        render_templates=False,
        rendered_fields={"logical_date": child_logical_date.isoformat()},
    )
    session.add(rtif)
    session.flush()

    with dag_maker("child_dag", session=session, schedule=None):
        ExternalTaskSensor(
            task_id="wait_for_parent",
            external_dag_id="parent_dag",
            external_task_id="trigger_child",
            poke_interval=5,
        )

    dag_maker.create_dagrun(logical_date=child_logical_date)
    session.flush()

    result = serialized_parent.clear(
        dry_run=True, only_failed=False, include_dependent_dags=True, session=session
    )
    dag_ids = {ti.dag_id for ti in result}

    assert "child_dag" in dag_ids
