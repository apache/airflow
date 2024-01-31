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

import os
from collections import defaultdict

import pytest
from sqlalchemy.sql import select

from airflow.datasets import Dataset
from airflow.models import DagModel
from airflow.models.dataset import DatasetAll, DatasetAny, DatasetDagRunQueue, DatasetModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.serialization.serialized_objects import BaseSerialization, SerializedDAG


@pytest.mark.parametrize(
    ["uri"],
    [
        pytest.param("", id="empty"),
        pytest.param("\n\t", id="whitespace"),
        pytest.param("a" * 3001, id="too_long"),
        pytest.param("airflow:" * 3001, id="reserved_scheme"),
        pytest.param("😊" * 3001, id="non-ascii"),
    ],
)
def test_invalid_uris(uri):
    with pytest.raises(ValueError):
        Dataset(uri=uri)


def test_uri_with_scheme():
    dataset = Dataset(uri="s3://example_dataset")
    EmptyOperator(task_id="task1", outlets=[dataset])


def test_uri_without_scheme():
    dataset = Dataset(uri="example_dataset")
    EmptyOperator(task_id="task1", outlets=[dataset])


def test_fspath():
    uri = "s3://example_dataset"
    dataset = Dataset(uri=uri)
    assert os.fspath(dataset) == uri


@pytest.mark.parametrize(
    "input",
    [
        (True, True, True),
        (True, True, False),
        (True, False, True),
        (True, False, False),
        (False, False, True),
        (False, False, False),
        (False, True, True),
        (False, True, False),
    ],
)
@pytest.mark.parametrize("scenario", ["any", "all"])
def test_dataset_cond(input, scenario):
    if scenario == "any":
        expected = any(input)
        class_ = DatasetAny
    else:
        expected = all(input)
        class_ = DatasetAll
    d1 = Dataset(uri="s3://abc/123")
    d2 = Dataset(uri="s3://abc/124")
    d3 = Dataset(uri="s3://abc/125")
    d_cond = class_(d1, d2, d3)
    ser_d_cond = BaseSerialization.serialize(d_cond)
    deser_d_cond = BaseSerialization.deserialize(ser_d_cond)
    statuses = {d1.uri: input[0], d2.uri: input[1], d3.uri: input[2]}
    assert d_cond.evaluate(statuses) == expected
    assert deser_d_cond.evaluate(statuses) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ((False, True, True), False),
        ((True, True, True), True),
        ((True, False, True), True),
        ((True, False, False), False),
    ],
)
def test_dataset_cond_nested(input, expected):
    d1 = Dataset(uri="s3://abc/123")
    d2 = Dataset(uri="s3://abc/124")
    d3 = Dataset(uri="s3://abc/125")
    sub_cond = DatasetAny(d2, d3)
    d_cond = DatasetAll(d1, sub_cond)
    statuses = {d1.uri: input[0], d2.uri: input[1], d3.uri: input[2]}
    ser_d_cond = BaseSerialization.serialize(d_cond)
    deser_d_cond = BaseSerialization.deserialize(ser_d_cond)
    assert d_cond.evaluate(statuses) == expected
    assert deser_d_cond.evaluate(statuses) == expected


def test_this(session, dag_maker):
    d1 = Dataset(uri="hello1")
    d1.uri
    dm1 = DatasetModel(uri=d1.uri)
    d2 = Dataset(uri="hello2")
    dm2 = DatasetModel(uri=d2.uri)
    session.add(dm1)
    session.add(dm2)
    session.commit()
    session.query(DagModel).all()
    d1.uri
    with dag_maker(schedule=DatasetAny(d1, d2)) as dag:
        EmptyOperator(task_id="hello")
    dag.dataset_triggers
    dm1.id
    ddrq = DatasetDagRunQueue(dataset_id=dm1.id, target_dag_id=dag.dag_id)
    session.add(ddrq)
    assert isinstance(dag.dataset_triggers, DatasetAny)
    SerializedDAG.serialize_to_json(dag, SerializedDAG._decorated_fields)
    SerializedDAG.serialize(dag.dataset_triggers).values()
    dtr = SerializedDAG.to_dict(dag)["dag"]["dataset_triggers"]
    assert isinstance(dtr, dict)
    deser_dtr = SerializedDAG.deserialize(dtr)
    assert isinstance(deser_dtr, DatasetAny)
    assert deser_dtr.objects == dag.dataset_triggers.objects
    SerializedDagModel.write_dag(dag)
    session.commit()
    with dag_maker(dag_id="dag2"):
        EmptyOperator(task_id="hello2")

    # here we start the scheduling logic
    records = session.scalars(select(DatasetDagRunQueue)).all()
    dag_statuses = defaultdict(dict)
    ddrq_times = defaultdict(list)
    for ddrq in records:
        dag_statuses[ddrq.target_dag_id][ddrq.dataset.uri] = True
        ddrq_times[ddrq.target_dag_id].append(ddrq.created_at)
    # dataset_triggered_dag_info = {dag_id: (min(times), max(times)) for dag_id, times in ddrq_times.items()}
    ser_dags = session.execute(
        select(SerializedDagModel).where(SerializedDagModel.dag_id.in_(dag_statuses.keys()))
    ).all()
    for (ser_dag,) in ser_dags:
        print(ser_dag)
        statuses = dag_statuses[ser_dag.dag_id]
        ser_dag.dag.dataset_triggers.evaluate(statuses)


@pytest.fixture(autouse=True)
def clear_datasets():
    from tests.test_utils.db import clear_db_datasets

    clear_db_datasets()


def test_this2(session, dag_maker):
    d1 = Dataset(uri="hello1")
    d1.uri
    dm1 = DatasetModel(uri=d1.uri)
    d2 = Dataset(uri="hello2")
    dm2 = DatasetModel(uri=d2.uri)
    session.add(dm1)
    session.add(dm2)
    session.commit()
    session.query(DagModel).all()
    d1.uri
    with dag_maker(schedule=DatasetAny(d1, DatasetAll(d2, d1))) as dag:
        EmptyOperator(task_id="hello")
    dag.dataset_triggers
    SerializedDAG.serialize_to_json(dag, SerializedDAG._decorated_fields)
    SerializedDAG.serialize(dag.dataset_triggers).values()
    SerializedDAG.to_dict(dag)["dag"]["dataset_triggers"]
