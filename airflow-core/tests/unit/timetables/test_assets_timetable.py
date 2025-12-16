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

from collections import defaultdict
from typing import Any

import pytest
from pendulum import DateTime
from sqlalchemy.sql import select

from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel
from airflow.models.serialized_dag import SerializedDAG, SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset, AssetAll, AssetAny
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.simple import AssetTriggeredTimetable
from airflow.utils.types import DagRunType


class MockTimetable(Timetable):
    """
    A mock Timetable class for testing purposes in Apache Airflow.
    """

    __test__ = False

    def __init__(self) -> None:
        """
        Initializes the MockTimetable with the current DateTime.
        """
        self._now = DateTime.now()

    def next_dagrun_info(
        self, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction
    ) -> DagRunInfo | None:
        """
        Calculates the next DagRun information based on the provided interval and restrictions.

        :param last_automated_data_interval: The last automated data interval.
        :param restriction: The time restriction to apply.
        """
        if last_automated_data_interval is None:
            next_run_date = self._now
        else:
            next_run_date = last_automated_data_interval.end.add(days=1)

        if restriction.earliest and next_run_date < restriction.earliest:
            next_run_date = restriction.earliest

        if restriction.latest and next_run_date > restriction.latest:
            return None

        return DagRunInfo.interval(start=next_run_date, end=next_run_date.add(days=1))

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """
        Infers the data interval for manual triggers.

        :param run_after: The datetime after which the run is triggered.
        """
        return DataInterval.exact(run_after)


def serialize_timetable(timetable: Timetable) -> str:
    """
    Mock serialization function for Timetable objects.

    :param timetable: The Timetable object to serialize.
    """
    return "serialized_timetable"


def deserialize_timetable(serialized: str) -> MockTimetable:
    """
    Mock deserialization function for Timetable objects.

    :param serialized: The serialized data of the timetable.
    """
    return MockTimetable()


@pytest.fixture
def test_timetable() -> MockTimetable:
    """Pytest fixture for creating a MockTimetable object."""
    return MockTimetable()


@pytest.fixture
def test_assets() -> list[Asset]:
    """Pytest fixture for creating a list of Asset objects."""
    return [Asset(name="test_asset", uri="test://asset")]


@pytest.fixture
def asset_timetable(test_timetable: MockTimetable, test_assets: list[Asset]) -> AssetOrTimeSchedule:
    """
    Pytest fixture for creating an AssetOrTimeSchedule object.

    :param test_timetable: The test timetable instance.
    :param test_assets: A list of Asset instances.
    """
    return AssetOrTimeSchedule(timetable=test_timetable, assets=test_assets)


def test_serialization(asset_timetable: AssetOrTimeSchedule, monkeypatch: Any) -> None:
    """
    Tests the serialization method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    :param monkeypatch: The monkeypatch fixture from pytest.
    """
    monkeypatch.setattr(
        "airflow.serialization.encoders.encode_timetable", lambda x: "mock_serialized_timetable"
    )
    serialized = asset_timetable.serialize()
    assert serialized == {
        "timetable": "mock_serialized_timetable",
        "asset_condition": {
            "__type": "asset_all",
            "objects": [
                {
                    "__type": "asset",
                    "name": "test_asset",
                    "uri": "test://asset/",
                    "group": "asset",
                    "extra": {},
                }
            ],
        },
    }


def test_deserialization(monkeypatch: Any) -> None:
    """
    Tests the deserialization method of AssetOrTimeSchedule.

    :param monkeypatch: The monkeypatch fixture from pytest.
    """
    monkeypatch.setattr("airflow.serialization.decoders.decode_timetable", lambda x: MockTimetable())
    mock_serialized_data = {
        "timetable": "mock_serialized_timetable",
        "asset_condition": {
            "__type": "asset_all",
            "objects": [
                {
                    "__type": "asset",
                    "name": "test_asset",
                    "uri": "test://asset/",
                    "group": "asset",
                    "extra": None,
                }
            ],
        },
    }
    deserialized = AssetOrTimeSchedule.deserialize(mock_serialized_data)
    assert isinstance(deserialized, AssetOrTimeSchedule)


def test_infer_manual_data_interval(asset_timetable: AssetOrTimeSchedule) -> None:
    """
    Tests the infer_manual_data_interval method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    run_after = DateTime.now()
    result = asset_timetable.infer_manual_data_interval(run_after=run_after)
    assert isinstance(result, DataInterval)


def test_next_dagrun_info(asset_timetable: AssetOrTimeSchedule) -> None:
    """
    Tests the next_dagrun_info method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    last_interval = DataInterval.exact(DateTime.now())
    restriction = TimeRestriction(earliest=DateTime.now(), latest=None, catchup=True)
    result = asset_timetable.next_dagrun_info(
        last_automated_data_interval=last_interval, restriction=restriction
    )
    assert result is None or isinstance(result, DagRunInfo)


def test_generate_run_id(asset_timetable: AssetOrTimeSchedule) -> None:
    """
    Tests the generate_run_id method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    run_id = asset_timetable.generate_run_id(
        run_type=DagRunType.MANUAL,
        extra_args="test",
        logical_date=DateTime.now(),
        run_after=DateTime.now(),
        data_interval=None,
    )
    assert isinstance(run_id, str)


@pytest.fixture
def asset_events(mocker) -> list[AssetEvent]:
    """Pytest fixture for creating mock AssetEvent objects."""
    now = DateTime.now()
    earlier = now.subtract(days=1)
    later = now.add(days=1)

    # Create mock source_dag_run objects
    mock_dag_run_earlier = mocker.MagicMock()
    mock_dag_run_earlier.data_interval_start = earlier
    mock_dag_run_earlier.data_interval_end = now

    mock_dag_run_later = mocker.MagicMock()
    mock_dag_run_later.data_interval_start = now
    mock_dag_run_later.data_interval_end = later

    # Create AssetEvent objects with mock source_dag_run
    event_earlier = AssetEvent(timestamp=earlier, asset_id=1)
    event_later = AssetEvent(timestamp=later, asset_id=1)

    # Use mocker to set the source_dag_run attribute to avoid SQLAlchemy's instrumentation
    mocker.patch.object(event_earlier, "source_dag_run", new=mock_dag_run_earlier)
    mocker.patch.object(event_later, "source_dag_run", new=mock_dag_run_later)

    return [event_earlier, event_later]


def test_run_ordering_inheritance(asset_timetable: AssetOrTimeSchedule) -> None:
    """
    Tests that AssetOrTimeSchedule inherits run_ordering from its parent class correctly.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    assert hasattr(asset_timetable, "run_ordering"), (
        "AssetOrTimeSchedule should have 'run_ordering' attribute"
    )
    parent_run_ordering = getattr(AssetTriggeredTimetable, "run_ordering", None)
    assert asset_timetable.run_ordering == parent_run_ordering, "run_ordering does not match the parent class"


@pytest.mark.db_test
class TestAssetConditionWithTimetable:
    @pytest.fixture(autouse=True)
    def clear_assets(self):
        from tests_common.test_utils.db import clear_db_assets

        clear_db_assets()
        yield
        clear_db_assets()

    @pytest.fixture
    def create_test_assets(self):
        """Fixture to create test assets and corresponding models."""
        return [Asset(uri=f"test://asset{i}", name=f"hello{i}") for i in range(1, 3)]

    def test_asset_dag_run_queue_processing(self, session, dag_maker, create_test_assets):
        from airflow.assets.evaluation import AssetEvaluator

        assets = create_test_assets
        asset_models = session.scalars(select(AssetModel)).all()
        evaluator = AssetEvaluator(session)

        with dag_maker(schedule=AssetAny(*assets)) as dag:
            EmptyOperator(task_id="hello")

        # Add AssetDagRunQueue entries to simulate asset event processing
        for am in asset_models:
            session.add(AssetDagRunQueue(asset_id=am.id, target_dag_id=dag.dag_id))
        session.commit()

        # Fetch and evaluate asset triggers for all DAGs affected by asset events
        records = session.scalars(select(AssetDagRunQueue)).all()
        dag_statuses = defaultdict(lambda: defaultdict(bool))
        for record in records:
            dag_statuses[record.target_dag_id][record.asset.uri] = True

        serialized_dags = session.execute(
            select(SerializedDagModel).where(SerializedDagModel.dag_id.in_(dag_statuses.keys()))
        ).fetchall()

        for (serialized_dag,) in serialized_dags:
            dag = SerializedDAG.deserialize(serialized_dag.data)
            for asset_uri, status in dag_statuses[dag.dag_id].items():
                cond = dag.timetable.asset_condition
                assert evaluator.run(cond, {asset_uri: status}), "DAG trigger evaluation failed"

    def test_dag_with_complex_asset_condition(self, session, dag_maker):
        # Create Asset instances
        asset1 = Asset(uri="test://asset1", name="hello1")
        asset2 = Asset(uri="test://asset2", name="hello2")

        # Create and add AssetModel instances to the session
        am1 = AssetModel(uri=asset1.uri, name=asset1.name, group="asset")
        am2 = AssetModel(uri=asset2.uri, name=asset2.name, group="asset")
        session.add_all([am1, am2])
        session.commit()

        # Setup a DAG with complex asset triggers (AssetAny with AssetAll)
        with dag_maker(schedule=AssetAny(asset1, AssetAll(asset2, asset1))) as dag:
            EmptyOperator(task_id="hello")

        assert isinstance(dag.timetable.asset_condition, AssetAny), (
            "DAG's asset trigger should be an instance of AssetAny"
        )
        assert any(isinstance(trigger, AssetAll) for trigger in dag.timetable.asset_condition.objects), (
            "DAG's asset trigger should include AssetAll"
        )

        serialized_triggers = SerializedDAG.serialize(dag.timetable.asset_condition)

        deserialized_triggers = SerializedDAG.deserialize(serialized_triggers)

        assert isinstance(deserialized_triggers, AssetAny), (
            "Deserialized triggers should be an instance of AssetAny"
        )
        assert any(isinstance(trigger, AssetAll) for trigger in deserialized_triggers.objects), (
            "Deserialized triggers should include AssetAll"
        )

        serialized_timetable_dict = SerializedDAG.to_dict(dag)["dag"]["timetable"]["__var"]
        assert "asset_condition" in serialized_timetable_dict, (
            "Serialized timetable should contain 'asset_condition'"
        )
        assert isinstance(serialized_timetable_dict["asset_condition"], dict), (
            "Serialized 'asset_condition' should be a dict"
        )
