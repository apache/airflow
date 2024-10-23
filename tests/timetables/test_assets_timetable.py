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

from typing import TYPE_CHECKING, Any

import pytest
from pendulum import DateTime

from airflow.assets import Asset, AssetAlias
from airflow.models.asset import AssetAliasModel, AssetEvent, AssetModel
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.simple import AssetTriggeredTimetable
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from sqlalchemy import Session


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
    return [Asset("test_asset")]


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
        "airflow.serialization.serialized_objects.encode_timetable", lambda x: "mock_serialized_timetable"
    )
    serialized = asset_timetable.serialize()
    assert serialized == {
        "timetable": "mock_serialized_timetable",
        "asset_condition": {
            "__type": "asset_all",
            "objects": [{"__type": "asset", "uri": "test_asset", "extra": {}}],
        },
    }


def test_deserialization(monkeypatch: Any) -> None:
    """
    Tests the deserialization method of AssetOrTimeSchedule.

    :param monkeypatch: The monkeypatch fixture from pytest.
    """
    monkeypatch.setattr(
        "airflow.serialization.serialized_objects.decode_timetable", lambda x: MockTimetable()
    )
    mock_serialized_data = {
        "timetable": "mock_serialized_timetable",
        "asset_condition": {
            "__type": "asset_all",
            "objects": [{"__type": "asset", "uri": "test_asset", "extra": None}],
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
        run_type=DagRunType.MANUAL, extra_args="test", logical_date=DateTime.now(), data_interval=None
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


def test_data_interval_for_events(
    asset_timetable: AssetOrTimeSchedule, asset_events: list[AssetEvent]
) -> None:
    """
    Tests the data_interval_for_events method of AssetOrTimeSchedule.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    :param asset_events: A list of mock AssetEvent instances.
    """
    data_interval = asset_timetable.data_interval_for_events(logical_date=DateTime.now(), events=asset_events)
    assert data_interval.start == min(
        event.timestamp for event in asset_events
    ), "Data interval start does not match"
    assert data_interval.end == max(
        event.timestamp for event in asset_events
    ), "Data interval end does not match"


def test_run_ordering_inheritance(asset_timetable: AssetOrTimeSchedule) -> None:
    """
    Tests that AssetOrTimeSchedule inherits run_ordering from its parent class correctly.

    :param asset_timetable: The AssetOrTimeSchedule instance to test.
    """
    assert hasattr(
        asset_timetable, "run_ordering"
    ), "AssetOrTimeSchedule should have 'run_ordering' attribute"
    parent_run_ordering = getattr(AssetTriggeredTimetable, "run_ordering", None)
    assert asset_timetable.run_ordering == parent_run_ordering, "run_ordering does not match the parent class"


@pytest.mark.db_test
def test_summary(session: Session) -> None:
    asset_model = AssetModel(uri="test_asset")
    asset_alias_model = AssetAliasModel(name="test_asset_alias")
    session.add_all([asset_model, asset_alias_model])
    session.commit()

    asset_alias = AssetAlias("test_asset_alias")
    table = AssetTriggeredTimetable(asset_alias)
    assert table.summary == "Unresolved AssetAlias"

    asset_alias_model.assets.append(asset_model)
    session.add(asset_alias_model)
    session.commit()

    table = AssetTriggeredTimetable(asset_alias)
    assert table.summary == "Asset"
