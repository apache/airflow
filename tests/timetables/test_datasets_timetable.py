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

from airflow.assets import AssetAlias, Dataset
from airflow.models.dataset import AssetAliasModel, DatasetEvent, DatasetModel
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.simple import DatasetTriggeredTimetable
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
def test_datasets() -> list[Dataset]:
    """Pytest fixture for creating a list of Dataset objects."""
    return [Dataset("test_dataset")]


@pytest.fixture
def dataset_timetable(test_timetable: MockTimetable, test_datasets: list[Dataset]) -> DatasetOrTimeSchedule:
    """
    Pytest fixture for creating a DatasetTimetable object.

    :param test_timetable: The test timetable instance.
    :param test_datasets: A list of Dataset instances.
    """
    return DatasetOrTimeSchedule(timetable=test_timetable, datasets=test_datasets)


def test_serialization(dataset_timetable: DatasetOrTimeSchedule, monkeypatch: Any) -> None:
    """
    Tests the serialization method of DatasetTimetable.

    :param dataset_timetable: The DatasetTimetable instance to test.
    :param monkeypatch: The monkeypatch fixture from pytest.
    """
    monkeypatch.setattr(
        "airflow.serialization.serialized_objects.encode_timetable", lambda x: "mock_serialized_timetable"
    )
    serialized = dataset_timetable.serialize()
    assert serialized == {
        "timetable": "mock_serialized_timetable",
        "dataset_condition": {
            "__type": "asset_all",
            "objects": [{"__type": "dataset", "uri": "test_dataset", "extra": {}}],
        },
    }


def test_deserialization(monkeypatch: Any) -> None:
    """
    Tests the deserialization method of DatasetTimetable.

    :param monkeypatch: The monkeypatch fixture from pytest.
    """
    monkeypatch.setattr(
        "airflow.serialization.serialized_objects.decode_timetable", lambda x: MockTimetable()
    )
    mock_serialized_data = {
        "timetable": "mock_serialized_timetable",
        "dataset_condition": {
            "__type": "asset_all",
            "objects": [{"__type": "dataset", "uri": "test_dataset", "extra": None}],
        },
    }
    deserialized = DatasetOrTimeSchedule.deserialize(mock_serialized_data)
    assert isinstance(deserialized, DatasetOrTimeSchedule)


def test_infer_manual_data_interval(dataset_timetable: DatasetOrTimeSchedule) -> None:
    """
    Tests the infer_manual_data_interval method of DatasetTimetable.

    :param dataset_timetable: The DatasetTimetable instance to test.
    """
    run_after = DateTime.now()
    result = dataset_timetable.infer_manual_data_interval(run_after=run_after)
    assert isinstance(result, DataInterval)


def test_next_dagrun_info(dataset_timetable: DatasetOrTimeSchedule) -> None:
    """
    Tests the next_dagrun_info method of DatasetTimetable.

    :param dataset_timetable: The DatasetTimetable instance to test.
    """
    last_interval = DataInterval.exact(DateTime.now())
    restriction = TimeRestriction(earliest=DateTime.now(), latest=None, catchup=True)
    result = dataset_timetable.next_dagrun_info(
        last_automated_data_interval=last_interval, restriction=restriction
    )
    assert result is None or isinstance(result, DagRunInfo)


def test_generate_run_id(dataset_timetable: DatasetOrTimeSchedule) -> None:
    """
    Tests the generate_run_id method of DatasetTimetable.

    :param dataset_timetable: The DatasetTimetable instance to test.
    """
    run_id = dataset_timetable.generate_run_id(
        run_type=DagRunType.MANUAL, extra_args="test", logical_date=DateTime.now(), data_interval=None
    )
    assert isinstance(run_id, str)


@pytest.fixture
def dataset_events(mocker) -> list[DatasetEvent]:
    """Pytest fixture for creating mock DatasetEvent objects."""
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

    # Create DatasetEvent objects with mock source_dag_run
    event_earlier = DatasetEvent(timestamp=earlier, dataset_id=1)
    event_later = DatasetEvent(timestamp=later, dataset_id=1)

    # Use mocker to set the source_dag_run attribute to avoid SQLAlchemy's instrumentation
    mocker.patch.object(event_earlier, "source_dag_run", new=mock_dag_run_earlier)
    mocker.patch.object(event_later, "source_dag_run", new=mock_dag_run_later)

    return [event_earlier, event_later]


def test_data_interval_for_events(
    dataset_timetable: DatasetOrTimeSchedule, dataset_events: list[DatasetEvent]
) -> None:
    """
    Tests the data_interval_for_events method of DatasetTimetable.

    :param dataset_timetable: The DatasetTimetable instance to test.
    :param dataset_events: A list of mock DatasetEvent instances.
    """
    data_interval = dataset_timetable.data_interval_for_events(
        logical_date=DateTime.now(), events=dataset_events
    )
    assert data_interval.start == min(
        event.timestamp for event in dataset_events
    ), "Data interval start does not match"
    assert data_interval.end == max(
        event.timestamp for event in dataset_events
    ), "Data interval end does not match"


def test_run_ordering_inheritance(dataset_timetable: DatasetOrTimeSchedule) -> None:
    """
    Tests that DatasetOrTimeSchedule inherits run_ordering from its parent class correctly.

    :param dataset_timetable: The DatasetTimetable instance to test.
    """
    assert hasattr(
        dataset_timetable, "run_ordering"
    ), "DatasetOrTimeSchedule should have 'run_ordering' attribute"
    parent_run_ordering = getattr(DatasetTriggeredTimetable, "run_ordering", None)
    assert (
        dataset_timetable.run_ordering == parent_run_ordering
    ), "run_ordering does not match the parent class"


@pytest.mark.db_test
def test_summary(session: Session) -> None:
    dataset_model = DatasetModel(uri="test_dataset")
    asset_alias_model = AssetAliasModel(name="test_asset_alias")
    session.add_all([dataset_model, asset_alias_model])
    session.commit()

    asset_alias = AssetAlias("test_asset_alias")
    table = DatasetTriggeredTimetable(asset_alias)
    assert table.summary == "Unresolved AssetAlias"

    asset_alias_model.datasets.append(dataset_model)
    session.add(asset_alias_model)
    session.commit()

    table = DatasetTriggeredTimetable(asset_alias)
    assert table.summary == "Dataset"
