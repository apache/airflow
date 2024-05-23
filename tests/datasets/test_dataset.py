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
from typing import Callable
from unittest.mock import patch

import pytest
from sqlalchemy.sql import select

from airflow.datasets import BaseDataset, Dataset, DatasetAll, DatasetAny, _sanitize_uri
from airflow.models.dataset import DatasetDagRunQueue, DatasetModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.serialization.serialized_objects import BaseSerialization, SerializedDAG
from tests.test_utils.config import conf_vars


@pytest.fixture
def clear_datasets():
    from tests.test_utils.db import clear_db_datasets

    clear_db_datasets()
    yield
    clear_db_datasets()


@pytest.mark.parametrize(
    ["uri"],
    [
        pytest.param("", id="empty"),
        pytest.param("\n\t", id="whitespace"),
        pytest.param("a" * 3001, id="too_long"),
        pytest.param("airflow://xcom/dag/task", id="reserved_scheme"),
        pytest.param("😊", id="non-ascii"),
    ],
)
def test_invalid_uris(uri):
    with pytest.raises(ValueError):
        Dataset(uri=uri)


@pytest.mark.parametrize(
    "uri, normalized",
    [
        pytest.param("foobar", "foobar", id="scheme-less"),
        pytest.param("foo:bar", "foo:bar", id="scheme-less-colon"),
        pytest.param("foo/bar", "foo/bar", id="scheme-less-slash"),
        pytest.param("s3://bucket/key/path", "s3://bucket/key/path", id="normal"),
        pytest.param("file:///123/456/", "file:///123/456", id="trailing-slash"),
    ],
)
def test_uri_with_scheme(uri: str, normalized: str) -> None:
    dataset = Dataset(uri)
    EmptyOperator(task_id="task1", outlets=[dataset])
    assert dataset.uri == normalized
    assert os.fspath(dataset) == normalized


def test_uri_with_auth() -> None:
    with pytest.warns(UserWarning) as record:
        dataset = Dataset("ftp://user@localhost/foo.txt")
    assert len(record) == 1
    assert str(record[0].message) == (
        "A dataset URI should not contain auth info (e.g. username or "
        "password). It has been automatically dropped."
    )
    EmptyOperator(task_id="task1", outlets=[dataset])
    assert dataset.uri == "ftp://localhost/foo.txt"
    assert os.fspath(dataset) == "ftp://localhost/foo.txt"


def test_uri_without_scheme():
    dataset = Dataset(uri="example_dataset")
    EmptyOperator(task_id="task1", outlets=[dataset])


def test_fspath():
    uri = "s3://example/dataset"
    dataset = Dataset(uri=uri)
    assert os.fspath(dataset) == uri


def test_equal_when_same_uri():
    uri = "s3://example/dataset"
    dataset1 = Dataset(uri=uri)
    dataset2 = Dataset(uri=uri)
    assert dataset1 == dataset2


def test_not_equal_when_different_uri():
    dataset1 = Dataset(uri="s3://example/dataset")
    dataset2 = Dataset(uri="s3://other/dataset")
    assert dataset1 != dataset2


def test_hash():
    uri = "s3://example/dataset"
    dataset = Dataset(uri=uri)
    hash(dataset)


def test_dataset_logic_operations():
    result_or = dataset1 | dataset2
    assert isinstance(result_or, DatasetAny)
    result_and = dataset1 & dataset2
    assert isinstance(result_and, DatasetAll)


def test_dataset_iter_datasets():
    assert list(dataset1.iter_datasets()) == [("s3://bucket1/data1", dataset1)]


def test_dataset_evaluate():
    assert dataset1.evaluate({"s3://bucket1/data1": True}) is True
    assert dataset1.evaluate({"s3://bucket1/data1": False}) is False


def test_dataset_any_operations():
    result_or = (dataset1 | dataset2) | dataset3
    assert isinstance(result_or, DatasetAny)
    assert len(result_or.objects) == 3
    result_and = (dataset1 | dataset2) & dataset3
    assert isinstance(result_and, DatasetAll)


def test_dataset_all_operations():
    result_or = (dataset1 & dataset2) | dataset3
    assert isinstance(result_or, DatasetAny)
    result_and = (dataset1 & dataset2) & dataset3
    assert isinstance(result_and, DatasetAll)


def test_datasetbooleancondition_evaluate_iter():
    """
    Tests _DatasetBooleanCondition's evaluate and iter_datasets methods through DatasetAny and DatasetAll.
    Ensures DatasetAny evaluate returns True with any true condition, DatasetAll evaluate returns False if
    any condition is false, and both classes correctly iterate over datasets without duplication.
    """
    any_condition = DatasetAny(dataset1, dataset2)
    all_condition = DatasetAll(dataset1, dataset2)
    assert any_condition.evaluate({"s3://bucket1/data1": False, "s3://bucket2/data2": True}) is True
    assert all_condition.evaluate({"s3://bucket1/data1": True, "s3://bucket2/data2": False}) is False

    # Testing iter_datasets indirectly through the subclasses
    datasets_any = set(any_condition.iter_datasets())
    datasets_all = set(all_condition.iter_datasets())
    assert datasets_any == {("s3://bucket1/data1", dataset1), ("s3://bucket2/data2", dataset2)}
    assert datasets_all == {("s3://bucket1/data1", dataset1), ("s3://bucket2/data2", dataset2)}


@pytest.mark.parametrize(
    "inputs, scenario, expected",
    [
        # Scenarios for DatasetAny
        ((True, True, True), "any", True),
        ((True, True, False), "any", True),
        ((True, False, True), "any", True),
        ((True, False, False), "any", True),
        ((False, False, True), "any", True),
        ((False, True, False), "any", True),
        ((False, True, True), "any", True),
        ((False, False, False), "any", False),
        # Scenarios for DatasetAll
        ((True, True, True), "all", True),
        ((True, True, False), "all", False),
        ((True, False, True), "all", False),
        ((True, False, False), "all", False),
        ((False, False, True), "all", False),
        ((False, True, False), "all", False),
        ((False, True, True), "all", False),
        ((False, False, False), "all", False),
    ],
)
def test_dataset_logical_conditions_evaluation_and_serialization(inputs, scenario, expected):
    class_ = DatasetAny if scenario == "any" else DatasetAll
    datasets = [Dataset(uri=f"s3://abc/{i}") for i in range(123, 126)]
    condition = class_(*datasets)

    statuses = {dataset.uri: status for dataset, status in zip(datasets, inputs)}
    assert (
        condition.evaluate(statuses) == expected
    ), f"Condition evaluation failed for inputs {inputs} and scenario '{scenario}'"

    # Serialize and deserialize the condition to test persistence
    serialized = BaseSerialization.serialize(condition)
    deserialized = BaseSerialization.deserialize(serialized)
    assert deserialized.evaluate(statuses) == expected, "Serialization round-trip failed"


@pytest.mark.parametrize(
    "status_values, expected_evaluation",
    [
        ((False, True, True), False),  # DatasetAll requires all conditions to be True, but d1 is False
        ((True, True, True), True),  # All conditions are True
        ((True, False, True), True),  # d1 is True, and DatasetAny condition (d2 or d3 being True) is met
        ((True, False, False), False),  # d1 is True, but neither d2 nor d3 meet the DatasetAny condition
    ],
)
def test_nested_dataset_conditions_with_serialization(status_values, expected_evaluation):
    # Define datasets
    d1 = Dataset(uri="s3://abc/123")
    d2 = Dataset(uri="s3://abc/124")
    d3 = Dataset(uri="s3://abc/125")

    # Create a nested condition: DatasetAll with d1 and DatasetAny with d2 and d3
    nested_condition = DatasetAll(d1, DatasetAny(d2, d3))

    statuses = {
        d1.uri: status_values[0],
        d2.uri: status_values[1],
        d3.uri: status_values[2],
    }

    assert nested_condition.evaluate(statuses) == expected_evaluation, "Initial evaluation mismatch"

    serialized_condition = BaseSerialization.serialize(nested_condition)
    deserialized_condition = BaseSerialization.deserialize(serialized_condition)

    assert (
        deserialized_condition.evaluate(statuses) == expected_evaluation
    ), "Post-serialization evaluation mismatch"


@pytest.fixture
def create_test_datasets(session):
    """Fixture to create test datasets and corresponding models."""
    datasets = [Dataset(uri=f"hello{i}") for i in range(1, 3)]
    for dataset in datasets:
        session.add(DatasetModel(uri=dataset.uri))
    session.commit()
    return datasets


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_datasets")
def test_dataset_trigger_setup_and_serialization(session, dag_maker, create_test_datasets):
    datasets = create_test_datasets

    # Create DAG with dataset triggers
    with dag_maker(schedule=DatasetAny(*datasets)) as dag:
        EmptyOperator(task_id="hello")

    # Verify dataset triggers are set up correctly
    assert isinstance(
        dag.dataset_triggers, DatasetAny
    ), "DAG dataset triggers should be an instance of DatasetAny"

    # Serialize and deserialize DAG dataset triggers
    serialized_trigger = SerializedDAG.serialize(dag.dataset_triggers)
    deserialized_trigger = SerializedDAG.deserialize(serialized_trigger)

    # Verify serialization and deserialization integrity
    assert isinstance(
        deserialized_trigger, DatasetAny
    ), "Deserialized trigger should maintain type DatasetAny"
    assert (
        deserialized_trigger.objects == dag.dataset_triggers.objects
    ), "Deserialized trigger objects should match original"


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_datasets")
def test_dataset_dag_run_queue_processing(session, clear_datasets, dag_maker, create_test_datasets):
    datasets = create_test_datasets
    dataset_models = session.query(DatasetModel).all()

    with dag_maker(schedule=DatasetAny(*datasets)) as dag:
        EmptyOperator(task_id="hello")

    # Add DatasetDagRunQueue entries to simulate dataset event processing
    for dm in dataset_models:
        session.add(DatasetDagRunQueue(dataset_id=dm.id, target_dag_id=dag.dag_id))
    session.commit()

    # Fetch and evaluate dataset triggers for all DAGs affected by dataset events
    records = session.scalars(select(DatasetDagRunQueue)).all()
    dag_statuses = defaultdict(lambda: defaultdict(bool))
    for record in records:
        dag_statuses[record.target_dag_id][record.dataset.uri] = True

    serialized_dags = session.execute(
        select(SerializedDagModel).where(SerializedDagModel.dag_id.in_(dag_statuses.keys()))
    ).fetchall()

    for (serialized_dag,) in serialized_dags:
        dag = SerializedDAG.deserialize(serialized_dag.data)
        for dataset_uri, status in dag_statuses[dag.dag_id].items():
            assert dag.dataset_triggers.evaluate({dataset_uri: status}), "DAG trigger evaluation failed"


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_datasets")
def test_dag_with_complex_dataset_triggers(session, dag_maker):
    # Create Dataset instances
    d1 = Dataset(uri="hello1")
    d2 = Dataset(uri="hello2")

    # Create and add DatasetModel instances to the session
    dm1 = DatasetModel(uri=d1.uri)
    dm2 = DatasetModel(uri=d2.uri)
    session.add_all([dm1, dm2])
    session.commit()

    # Setup a DAG with complex dataset triggers (DatasetAny with DatasetAll)
    with dag_maker(schedule=DatasetAny(d1, DatasetAll(d2, d1))) as dag:
        EmptyOperator(task_id="hello")

    assert isinstance(
        dag.dataset_triggers, DatasetAny
    ), "DAG's dataset trigger should be an instance of DatasetAny"
    assert any(
        isinstance(trigger, DatasetAll) for trigger in dag.dataset_triggers.objects
    ), "DAG's dataset trigger should include DatasetAll"

    serialized_triggers = SerializedDAG.serialize(dag.dataset_triggers)

    deserialized_triggers = SerializedDAG.deserialize(serialized_triggers)

    assert isinstance(
        deserialized_triggers, DatasetAny
    ), "Deserialized triggers should be an instance of DatasetAny"
    assert any(
        isinstance(trigger, DatasetAll) for trigger in deserialized_triggers.objects
    ), "Deserialized triggers should include DatasetAll"

    serialized_dag_dict = SerializedDAG.to_dict(dag)["dag"]
    assert "dataset_triggers" in serialized_dag_dict, "Serialized DAG should contain 'dataset_triggers'"
    assert isinstance(
        serialized_dag_dict["dataset_triggers"], dict
    ), "Serialized 'dataset_triggers' should be a dict"


def datasets_equal(d1: BaseDataset, d2: BaseDataset) -> bool:
    if type(d1) != type(d2):
        return False

    if isinstance(d1, Dataset) and isinstance(d2, Dataset):
        return d1.uri == d2.uri

    elif isinstance(d1, (DatasetAny, DatasetAll)) and isinstance(d2, (DatasetAny, DatasetAll)):
        if len(d1.objects) != len(d2.objects):
            return False

        # Compare each pair of objects
        for obj1, obj2 in zip(d1.objects, d2.objects):
            # If obj1 or obj2 is a Dataset, DatasetAny, or DatasetAll instance,
            # recursively call datasets_equal
            if not datasets_equal(obj1, obj2):
                return False
        return True

    return False


dataset1 = Dataset(uri="s3://bucket1/data1")
dataset2 = Dataset(uri="s3://bucket2/data2")
dataset3 = Dataset(uri="s3://bucket3/data3")
dataset4 = Dataset(uri="s3://bucket4/data4")
dataset5 = Dataset(uri="s3://bucket5/data5")

test_cases = [
    (lambda: dataset1, dataset1),
    (lambda: dataset1 & dataset2, DatasetAll(dataset1, dataset2)),
    (lambda: dataset1 | dataset2, DatasetAny(dataset1, dataset2)),
    (lambda: dataset1 | (dataset2 & dataset3), DatasetAny(dataset1, DatasetAll(dataset2, dataset3))),
    (lambda: dataset1 | dataset2 & dataset3, DatasetAny(dataset1, DatasetAll(dataset2, dataset3))),
    (
        lambda: ((dataset1 & dataset2) | dataset3) & (dataset4 | dataset5),
        DatasetAll(DatasetAny(DatasetAll(dataset1, dataset2), dataset3), DatasetAny(dataset4, dataset5)),
    ),
    (lambda: dataset1 & dataset2 | dataset3, DatasetAny(DatasetAll(dataset1, dataset2), dataset3)),
    (
        lambda: (dataset1 | dataset2) & (dataset3 | dataset4),
        DatasetAll(DatasetAny(dataset1, dataset2), DatasetAny(dataset3, dataset4)),
    ),
    (
        lambda: (dataset1 & dataset2) | (dataset3 & (dataset4 | dataset5)),
        DatasetAny(DatasetAll(dataset1, dataset2), DatasetAll(dataset3, DatasetAny(dataset4, dataset5))),
    ),
    (
        lambda: (dataset1 & dataset2) & (dataset3 & dataset4),
        DatasetAll(dataset1, dataset2, DatasetAll(dataset3, dataset4)),
    ),
    (lambda: dataset1 | dataset2 | dataset3, DatasetAny(dataset1, dataset2, dataset3)),
    (lambda: dataset1 & dataset2 & dataset3, DatasetAll(dataset1, dataset2, dataset3)),
    (
        lambda: ((dataset1 & dataset2) | dataset3) & (dataset4 | dataset5),
        DatasetAll(DatasetAny(DatasetAll(dataset1, dataset2), dataset3), DatasetAny(dataset4, dataset5)),
    ),
]


@pytest.mark.parametrize("expression, expected", test_cases)
def test_evaluate_datasets_expression(expression, expected):
    expr = expression()
    assert datasets_equal(expr, expected)


@pytest.mark.parametrize(
    "expression, error",
    [
        pytest.param(
            lambda: dataset1 & 1,  # type: ignore[operator]
            "unsupported operand type(s) for &: 'Dataset' and 'int'",
            id="&",
        ),
        pytest.param(
            lambda: dataset1 | 1,  # type: ignore[operator]
            "unsupported operand type(s) for |: 'Dataset' and 'int'",
            id="|",
        ),
        pytest.param(
            lambda: DatasetAll(1, dataset1),  # type: ignore[arg-type]
            "expect dataset expressions in condition",
            id="DatasetAll",
        ),
        pytest.param(
            lambda: DatasetAny(1, dataset1),  # type: ignore[arg-type]
            "expect dataset expressions in condition",
            id="DatasetAny",
        ),
    ],
)
def test_datasets_expression_error(expression: Callable[[], None], error: str) -> None:
    with pytest.raises(TypeError) as info:
        expression()
    assert str(info.value) == error


def mock_get_uri_normalizer(normalized_scheme):
    def normalizer(uri):
        raise ValueError("Incorrect URI format")

    return normalizer


@patch("airflow.datasets._get_uri_normalizer", mock_get_uri_normalizer)
@patch("airflow.datasets.warnings.warn")
def test__sanitize_uri_raises_warning(mock_warn):
    _sanitize_uri("postgres://localhost:5432/database.schema.table")
    msg = mock_warn.call_args.args[0]
    assert "The dataset URI postgres://localhost:5432/database.schema.table is not AIP-60 compliant" in msg
    assert "In Airflow 3, this will raise an exception." in msg


@patch("airflow.datasets._get_uri_normalizer", mock_get_uri_normalizer)
@conf_vars({("core", "strict_dataset_uri_validation"): "True"})
def test__sanitize_uri_raises_exception():
    with pytest.raises(ValueError) as e_info:
        _sanitize_uri("postgres://localhost:5432/database.schema.table")
    assert isinstance(e_info.value, ValueError)
    assert str(e_info.value) == "Incorrect URI format"
