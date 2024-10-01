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

import datetime

import pytest
from dateutil import relativedelta

from airflow.decorators import task
from airflow.decorators.python import _PythonDecoratedOperator
from airflow.jobs.job import Job
from airflow.jobs.local_task_job_runner import LocalTaskJobRunner
from airflow.models import MappedOperator
from airflow.models.asset import (
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dag import DAG, DagModel, create_timetable
from airflow.serialization.pydantic.asset import AssetEventPydantic
from airflow.serialization.pydantic.dag import DagModelPydantic
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.serialization.pydantic.job import JobPydantic
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.settings import _ENABLE_AIP_44, TracebackSessionForTests
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import AttributeRemoved, DagRunType
from tests.models import DEFAULT_DATE
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

pytest.importorskip("pydantic", minversion="2.0.0")


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
def test_serializing_pydantic_task_instance(session, create_task_instance):
    dag_id = "test-dag"
    ti = create_task_instance(dag_id=dag_id, session=session)
    ti.state = State.RUNNING
    ti.next_kwargs = {"foo": "bar"}
    session.commit()

    pydantic_task_instance = TaskInstancePydantic.model_validate(ti)

    json_string = pydantic_task_instance.model_dump_json()
    print(json_string)

    deserialized_model = TaskInstancePydantic.model_validate_json(json_string)
    assert deserialized_model.dag_id == dag_id
    assert deserialized_model.state == State.RUNNING
    assert deserialized_model.try_number == ti.try_number
    assert deserialized_model.execution_date == ti.execution_date
    assert deserialized_model.next_kwargs == {"foo": "bar"}


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
def test_deserialize_ti_mapped_op_reserialized_with_refresh_from_task(session, dag_maker):
    op_class_dict_expected = {
        "_needs_expansion": True,
        "_task_type": "_PythonDecoratedOperator",
        "downstream_task_ids": [],
        "start_from_trigger": False,
        "start_trigger_args": None,
        "_operator_name": "@task",
        "ui_fgcolor": "#000",
        "ui_color": "#ffefeb",
        "template_fields": ["templates_dict", "op_args", "op_kwargs"],
        "template_fields_renderers": {"templates_dict": "json", "op_args": "py", "op_kwargs": "py"},
        "template_ext": [],
        "task_id": "target",
    }

    with dag_maker() as dag:

        @task
        def source():
            return [1, 2, 3]

        @task
        def target(val=None):
            print(val)

        # source() >> target()
        target.expand(val=source())
    dr = dag_maker.create_dagrun()
    ti = dr.task_instances[1]

    # roundtrip task
    ser_task = BaseSerialization.serialize(ti.task, use_pydantic_models=True)
    deser_task = BaseSerialization.deserialize(ser_task, use_pydantic_models=True)
    ti.task.operator_class
    # this is part of the problem!
    assert isinstance(ti.task.operator_class, type)
    assert isinstance(deser_task.operator_class, dict)

    assert ti.task.operator_class == _PythonDecoratedOperator
    ti.refresh_from_task(deser_task)
    # roundtrip ti
    sered = BaseSerialization.serialize(ti, use_pydantic_models=True)
    desered = BaseSerialization.deserialize(sered, use_pydantic_models=True)
    assert desered.task.dag.__class__ is AttributeRemoved
    assert "operator_class" not in sered["__var"]["task"]

    assert desered.task.__class__ == MappedOperator

    assert desered.task.operator_class == op_class_dict_expected

    desered.refresh_from_task(deser_task)

    assert desered.task.__class__ == MappedOperator

    assert isinstance(desered.task.operator_class, dict)

    # let's check that we can safely add back dag...
    assert isinstance(dag, DAG)
    # dag already has this task
    assert dag.has_task(desered.task.task_id) is True
    # but the task has no dag
    assert desered.task.dag.__class__ is AttributeRemoved
    # and there are no upstream / downstreams on the task cus those are wiped out on serialization
    # and this is wrong / not great but that's how it is
    assert desered.task.upstream_task_ids == set()
    assert desered.task.downstream_task_ids == set()
    # add the dag back
    desered.task.dag = dag
    # great, no error
    # but still, there are no upstream downstreams
    assert desered.task.upstream_task_ids == set()
    assert desered.task.downstream_task_ids == set()


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
def test_serializing_pydantic_dagrun(session, create_task_instance):
    dag_id = "test-dag"
    ti = create_task_instance(dag_id=dag_id, session=session)
    ti.dag_run.state = State.RUNNING
    session.commit()

    pydantic_dag_run = DagRunPydantic.model_validate(ti.dag_run)

    json_string = pydantic_dag_run.model_dump_json()
    print(json_string)

    deserialized_model = DagRunPydantic.model_validate_json(json_string)
    assert deserialized_model.dag_id == dag_id
    assert deserialized_model.state == State.RUNNING


@pytest.mark.parametrize(
    "schedule",
    [
        None,
        "*/10 * * *",
        datetime.timedelta(days=1),
        relativedelta.relativedelta(days=+12),
    ],
)
def test_serializing_pydantic_dagmodel(schedule):
    timetable = create_timetable(schedule, timezone.utc)
    dag_model = DagModel(
        dag_id="test-dag",
        fileloc="/tmp/dag_1.py",
        timetable_summary=timetable.summary,
        timetable_description=timetable.description,
        is_active=True,
        is_paused=False,
    )

    pydantic_dag_model = DagModelPydantic.model_validate(dag_model)
    json_string = pydantic_dag_model.model_dump_json()

    deserialized_model = DagModelPydantic.model_validate_json(json_string)
    assert deserialized_model.dag_id == "test-dag"
    assert deserialized_model.fileloc == "/tmp/dag_1.py"
    assert deserialized_model.timetable_summary == timetable.summary
    assert deserialized_model.timetable_description == timetable.description
    assert deserialized_model.is_active is True
    assert deserialized_model.is_paused is False


def test_serializing_pydantic_local_task_job(session, create_task_instance):
    dag_id = "test-dag"
    ti = create_task_instance(dag_id=dag_id, session=session)
    ltj = Job(dag_id=ti.dag_id)
    LocalTaskJobRunner(job=ltj, task_instance=ti)
    ltj.state = State.RUNNING
    session.commit()
    pydantic_job = JobPydantic.model_validate(ltj)

    json_string = pydantic_job.model_dump_json()

    deserialized_model = JobPydantic.model_validate_json(json_string)
    assert deserialized_model.dag_id == dag_id
    assert deserialized_model.job_type == "LocalTaskJob"
    assert deserialized_model.state == State.RUNNING


# This test should not be run in DB isolation mode as it accesses the database directly - deliberately
@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
def test_serializing_pydantic_dataset_event(session, create_task_instance, create_dummy_dag):
    ds1 = AssetModel(id=1, uri="one", extra={"foo": "bar"})
    ds2 = AssetModel(id=2, uri="two")

    session.add_all([ds1, ds2])
    session.commit()

    # it's easier to fake a manual run here
    dag, task1 = create_dummy_dag(
        dag_id="test_triggering_asset_events",
        schedule=None,
        start_date=DEFAULT_DATE,
        task_id="test_context",
        with_dagrun_type=DagRunType.MANUAL,
        session=session,
    )
    execution_date = timezone.utcnow()
    TracebackSessionForTests.set_allow_db_access(session, True)

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    dr = dag.create_dagrun(
        run_id="test2",
        run_type=DagRunType.DATASET_TRIGGERED,
        execution_date=execution_date,
        state=None,
        session=session,
        data_interval=(execution_date, execution_date),
        **triggered_by_kwargs,
    )
    asset1_event = AssetEvent(dataset_id=1)
    asset2_event_1 = AssetEvent(dataset_id=2)
    asset2_event_2 = AssetEvent(dataset_id=2)

    dag_asset_ref = DagScheduleAssetReference(dag_id=dag.dag_id)
    session.add(dag_asset_ref)
    dag_asset_ref.dataset = ds1
    task_ds_ref = TaskOutletAssetReference(task_id=task1.task_id, dag_id=dag.dag_id)
    session.add(task_ds_ref)
    task_ds_ref.dataset = ds1

    dr.consumed_dataset_events.append(asset1_event)
    dr.consumed_dataset_events.append(asset2_event_1)
    dr.consumed_dataset_events.append(asset2_event_2)
    session.commit()
    TracebackSessionForTests.set_allow_db_access(session, False)

    print(asset2_event_2.dataset.consuming_dags)
    pydantic_dse1 = AssetEventPydantic.model_validate(asset1_event)
    json_string1 = pydantic_dse1.model_dump_json()
    print(json_string1)

    pydantic_dse2 = AssetEventPydantic.model_validate(asset2_event_1)
    json_string2 = pydantic_dse2.model_dump_json()
    print(json_string2)

    pydantic_dag_run = DagRunPydantic.model_validate(dr)
    json_string_dr = pydantic_dag_run.model_dump_json()
    print(json_string_dr)

    deserialized_model1 = AssetEventPydantic.model_validate_json(json_string1)
    assert deserialized_model1.dataset.id == 1
    assert deserialized_model1.dataset.uri == "one"
    assert len(deserialized_model1.dataset.consuming_dags) == 1
    assert len(deserialized_model1.dataset.producing_tasks) == 1

    deserialized_model2 = AssetEventPydantic.model_validate_json(json_string2)
    assert deserialized_model2.dataset.id == 2
    assert deserialized_model2.dataset.uri == "two"
    assert len(deserialized_model2.dataset.consuming_dags) == 0
    assert len(deserialized_model2.dataset.producing_tasks) == 0

    deserialized_dr = DagRunPydantic.model_validate_json(json_string_dr)
    assert len(deserialized_dr.consumed_dataset_events) == 3
