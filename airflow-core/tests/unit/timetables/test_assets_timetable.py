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
from pendulum import UTC, DateTime
from sqlalchemy import select

from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel
from airflow.models.serialized_dag import SerializedDAG, SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetAll, AssetAny, AssetOrTimeSchedule as SdkAssetOrTimeSchedule
from airflow.serialization.definitions.assets import SerializedAsset, SerializedAssetAll, SerializedAssetAny
from airflow.timetables.assets import AssetOrTimeSchedule as CoreAssetOrTimeSchedule
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
def sdk_asset_timetable(test_timetable, test_assets) -> SdkAssetOrTimeSchedule:
    """
    Pytest fixture for creating an SDK AssetOrTimeSchedule object.

    :param test_timetable: The test timetable instance.
    :param test_assets: A list of Asset instances.
    """
    return SdkAssetOrTimeSchedule(timetable=test_timetable, assets=test_assets)


@pytest.fixture
def core_asset_timetable(test_timetable: MockTimetable) -> CoreAssetOrTimeSchedule:
    return CoreAssetOrTimeSchedule(
        timetable=test_timetable,
        assets=SerializedAssetAll([SerializedAsset("test_asset", "test://asset/", "asset", {}, [])]),
    )


def test_serialization(sdk_asset_timetable: SdkAssetOrTimeSchedule, monkeypatch: Any) -> None:
    """
    Tests the serialization method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    :param monkeypatch: The monkeypatch fixture from pytest.
    """
    from airflow.serialization.encoders import _serializer

    monkeypatch.setattr(
        "airflow.serialization.encoders.encode_timetable", lambda x: "mock_serialized_timetable"
    )
    serialized = _serializer.serialize(sdk_asset_timetable)
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


def test_deserialization(monkeypatch: Any, core_asset_timetable: CoreAssetOrTimeSchedule) -> None:
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
    deserialized = CoreAssetOrTimeSchedule.deserialize(mock_serialized_data)
    assert deserialized == core_asset_timetable


def test_infer_manual_data_interval(core_asset_timetable: CoreAssetOrTimeSchedule) -> None:
    """
    Tests the infer_manual_data_interval method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    run_after = DateTime(2025, 6, 7, 8, 9, tzinfo=UTC)
    result = core_asset_timetable.infer_manual_data_interval(run_after=run_after)
    assert result == DataInterval.exact(run_after)


def test_next_dagrun_info(core_asset_timetable: CoreAssetOrTimeSchedule) -> None:
    """
    Tests the next_dagrun_info method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    last_interval = DataInterval.exact(DateTime(2025, 6, 7, 8, 9, tzinfo=UTC))
    restriction = TimeRestriction(earliest=None, latest=None, catchup=False)
    result = core_asset_timetable.next_dagrun_info(
        last_automated_data_interval=last_interval, restriction=restriction
    )
    assert result == DagRunInfo.interval(
        DateTime(2025, 6, 8, 8, 9, tzinfo=UTC),
        DateTime(2025, 6, 9, 8, 9, tzinfo=UTC),
    )


def test_generate_run_id(core_asset_timetable: CoreAssetOrTimeSchedule) -> None:
    """
    Tests the generate_run_id method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    date = DateTime(2025, 6, 7, 8, 9, tzinfo=UTC)
    run_id = core_asset_timetable.generate_run_id(
        run_type=DagRunType.MANUAL,
        extra_args="test",
        logical_date=date,
        run_after=date,
        data_interval=None,
    )
    assert run_id == "manual__2025-06-07T08:09:00+00:00"


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


def test_run_ordering_inheritance(core_asset_timetable) -> None:
    """
    Tests that AssetOrTimeSchedule inherits run_ordering from its parent class correctly.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    assert core_asset_timetable.run_ordering == ("logical_date",)
    assert core_asset_timetable.run_ordering == AssetTriggeredTimetable.run_ordering


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

        serialized_dags = session.scalars(
            select(SerializedDagModel).where(SerializedDagModel.dag_id.in_(dag_statuses.keys()))
        )

        for serialized_dag in serialized_dags:
            dag = SerializedDAG.deserialize(serialized_dag.data)
            for asset_uri, status in dag_statuses[dag.dag_id].items():
                cond = dag.timetable.asset_condition
                assert evaluator.run(cond, {asset_uri: status}), "DAG trigger evaluation failed"

    def test_dag_with_complex_asset_condition(self, dag_maker):
        # Create Asset instances
        asset1 = Asset(uri="test://asset1", name="hello1")
        asset2 = Asset(uri="test://asset2", name="hello2")

        # Setup a DAG with complex asset triggers (AssetAny with AssetAll)
        with dag_maker(schedule=AssetAny(asset1, AssetAll(asset2, asset1))) as dag:
            EmptyOperator(task_id="hello")

        assert dag.timetable.asset_condition == AssetAny(asset1, AssetAll(asset2, asset1))

        serialized_triggers = SerializedDAG.serialize(dag.timetable.asset_condition)
        deserialized_triggers = SerializedDAG.deserialize(serialized_triggers)
        assert deserialized_triggers == SerializedAssetAny(
            [
                SerializedAsset("hello1", "test://asset1/", "asset", {}, []),
                SerializedAssetAll(
                    [
                        SerializedAsset("hello2", "test://asset2/", "asset", {}, []),
                        SerializedAsset("hello1", "test://asset1/", "asset", {}, []),
                    ],
                ),
            ],
        )

        serialized_timetable_dict = SerializedDAG.to_dict(dag)["dag"]["timetable"]["__var"]
        assert serialized_timetable_dict == {
            "asset_condition": {
                "__type": "asset_any",
                "objects": [
                    {
                        "__type": "asset",
                        "name": "hello1",
                        "uri": "test://asset1/",
                        "group": "asset",
                        "extra": {},
                    },
                    {
                        "__type": "asset_all",
                        "objects": [
                            {
                                "__type": "asset",
                                "name": "hello2",
                                "uri": "test://asset2/",
                                "group": "asset",
                                "extra": {},
                            },
                            {
                                "__type": "asset",
                                "name": "hello1",
                                "uri": "test://asset1/",
                                "group": "asset",
                                "extra": {},
                            },
                        ],
                    },
                ],
            },
        }
