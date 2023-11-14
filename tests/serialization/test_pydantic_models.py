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

import pytest

from airflow.jobs.job import Job
from airflow.jobs.local_task_job_runner import LocalTaskJobRunner
from airflow.models.dag import DagModel
from airflow.models.dataset import (
    DagScheduleDatasetReference,
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.serialization.pydantic.dag import DagModelPydantic
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.serialization.pydantic.dataset import DatasetEventPydantic
from airflow.serialization.pydantic.job import JobPydantic
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.settings import _ENABLE_AIP_44
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.models import DEFAULT_DATE

pytestmark = pytest.mark.db_test


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


def test_serializing_pydantic_dagmodel():
    dag_model = DagModel(
        dag_id="test-dag",
        fileloc="/tmp/dag_1.py",
        schedule_interval="2 2 * * *",
        is_active=True,
        is_paused=False,
    )

    pydantic_dag_model = DagModelPydantic.model_validate(dag_model)
    json_string = pydantic_dag_model.model_dump_json()
    print(json_string)

    deserialized_model = DagModelPydantic.model_validate_json(json_string)
    assert deserialized_model.dag_id == "test-dag"
    assert deserialized_model.fileloc == "/tmp/dag_1.py"
    assert deserialized_model.schedule_interval == "2 2 * * *"
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
    print(json_string)

    deserialized_model = JobPydantic.model_validate_json(json_string)
    assert deserialized_model.dag_id == dag_id
    assert deserialized_model.job_type == "LocalTaskJob"
    assert deserialized_model.state == State.RUNNING


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
def test_serializing_pydantic_dataset_event(session, create_task_instance, create_dummy_dag):
    ds1 = DatasetModel(id=1, uri="one", extra={"foo": "bar"})
    ds2 = DatasetModel(id=2, uri="two")

    session.add_all([ds1, ds2])
    session.commit()

    # it's easier to fake a manual run here
    dag, task1 = create_dummy_dag(
        dag_id="test_triggering_dataset_events",
        schedule=None,
        start_date=DEFAULT_DATE,
        task_id="test_context",
        with_dagrun_type=DagRunType.MANUAL,
        session=session,
    )
    dr = dag.create_dagrun(
        run_id="test2",
        run_type=DagRunType.DATASET_TRIGGERED,
        execution_date=timezone.utcnow(),
        state=None,
        session=session,
    )
    ds1_event = DatasetEvent(dataset_id=1)
    ds2_event_1 = DatasetEvent(dataset_id=2)
    ds2_event_2 = DatasetEvent(dataset_id=2)

    DagScheduleDatasetReference(dag_id=dag.dag_id, dataset=ds1)
    TaskOutletDatasetReference(task_id=task1.task_id, dag_id=dag.dag_id, dataset=ds1)

    dr.consumed_dataset_events.append(ds1_event)
    dr.consumed_dataset_events.append(ds2_event_1)
    dr.consumed_dataset_events.append(ds2_event_2)
    session.commit()

    print(ds2_event_2.dataset.consuming_dags)
    pydantic_dse1 = DatasetEventPydantic.model_validate(ds1_event)
    json_string1 = pydantic_dse1.model_dump_json()
    print(json_string1)

    pydantic_dse2 = DatasetEventPydantic.model_validate(ds2_event_1)
    json_string2 = pydantic_dse2.model_dump_json()
    print(json_string2)

    pydantic_dag_run = DagRunPydantic.model_validate(dr)
    json_string_dr = pydantic_dag_run.model_dump_json()
    print(json_string_dr)

    deserialized_model1 = DatasetEventPydantic.model_validate_json(json_string1)
    assert deserialized_model1.dataset.id == 1
    assert deserialized_model1.dataset.uri == "one"
    assert len(deserialized_model1.dataset.consuming_dags) == 1
    assert len(deserialized_model1.dataset.producing_tasks) == 1

    deserialized_model2 = DatasetEventPydantic.model_validate_json(json_string2)
    assert deserialized_model2.dataset.id == 2
    assert deserialized_model2.dataset.uri == "two"
    assert len(deserialized_model2.dataset.consuming_dags) == 0
    assert len(deserialized_model2.dataset.producing_tasks) == 0

    deserialized_dr = DagRunPydantic.model_validate_json(json_string_dr)
    assert len(deserialized_dr.consumed_dataset_events) == 3
