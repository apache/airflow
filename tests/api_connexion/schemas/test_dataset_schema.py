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
import time_machine

from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    DatasetEventCollection,
    dataset_collection_schema,
    dataset_event_collection_schema,
    dataset_event_schema,
    dataset_schema,
)
from airflow.assets import Dataset
from airflow.models.dataset import AssetAliasModel, AssetModel, DatasetEvent
from airflow.operators.empty import EmptyOperator
from tests.test_utils.db import clear_db_dags, clear_db_datasets

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestDatasetSchemaBase:
    def setup_method(self) -> None:
        clear_db_dags()
        clear_db_datasets()
        self.timestamp = "2022-06-10T12:02:44+00:00"
        self.freezer = time_machine.travel(self.timestamp, tick=False)
        self.freezer.start()

    def teardown_method(self) -> None:
        self.freezer.stop()
        clear_db_dags()
        clear_db_datasets()


class TestDatasetSchema(TestDatasetSchemaBase):
    def test_serialize(self, dag_maker, session):
        dataset = Dataset(
            uri="s3://bucket/key",
            extra={"foo": "bar"},
        )
        with dag_maker(dag_id="test_dataset_upstream_schema", serialized=True, session=session):
            EmptyOperator(task_id="task1", outlets=[dataset])
        with dag_maker(
            dag_id="test_dataset_downstream_schema", schedule=[dataset], serialized=True, session=session
        ):
            EmptyOperator(task_id="task2")

        asset_model = session.query(AssetModel).filter_by(uri=dataset.uri).one()

        serialized_data = dataset_schema.dump(asset_model)
        serialized_data["id"] = 1
        assert serialized_data == {
            "id": 1,
            "uri": "s3://bucket/key",
            "extra": {"foo": "bar"},
            "created_at": self.timestamp,
            "updated_at": self.timestamp,
            "consuming_dags": [
                {
                    "dag_id": "test_dataset_downstream_schema",
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                }
            ],
            "producing_tasks": [
                {
                    "task_id": "task1",
                    "dag_id": "test_dataset_upstream_schema",
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                }
            ],
            "aliases": [],
        }


class TestDatasetCollectionSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        assets = [
            AssetModel(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
            )
            for i in range(2)
        ]
        asset_aliases = [AssetAliasModel(name=f"alias_{i}") for i in range(2)]
        for asset_alias in asset_aliases:
            asset_alias.datasets.append(assets[0])
        session.add_all(assets)
        session.add_all(asset_aliases)
        session.flush()
        serialized_data = dataset_collection_schema.dump(DatasetCollection(datasets=assets, total_entries=2))
        serialized_data["datasets"][0]["id"] = 1
        serialized_data["datasets"][1]["id"] = 2
        serialized_data["datasets"][0]["aliases"][0]["id"] = 1
        serialized_data["datasets"][0]["aliases"][1]["id"] = 2
        assert serialized_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [
                        {"id": 1, "name": "alias_0"},
                        {"id": 2, "name": "alias_1"},
                    ],
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"foo": "bar"},
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
            ],
            "total_entries": 2,
        }


class TestDatasetEventSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        assetssetsset = AssetModel("s3://abc")
        session.add(assetssetsset)
        session.commit()
        event = DatasetEvent(
            id=1,
            dataset_id=assetssetsset.id,
            extra={"foo": "bar"},
            source_dag_id="foo",
            source_task_id="bar",
            source_run_id="custom",
            source_map_index=-1,
        )
        session.add(event)
        session.flush()
        serialized_data = dataset_event_schema.dump(event)
        assert serialized_data == {
            "id": 1,
            "dataset_id": assetssetsset.id,
            "dataset_uri": "s3://abc",
            "extra": {"foo": "bar"},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
            "timestamp": self.timestamp,
            "created_dagruns": [],
        }


class TestDatasetEventCreateSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        asset = AssetModel("s3://abc")
        session.add(asset)
        session.commit()
        event = DatasetEvent(
            id=1,
            dataset_id=asset.id,
            extra={"foo": "bar"},
            source_dag_id=None,
            source_task_id=None,
            source_run_id=None,
            source_map_index=-1,
        )
        session.add(event)
        session.flush()
        serialized_data = dataset_event_schema.dump(event)
        assert serialized_data == {
            "id": 1,
            "dataset_id": asset.id,
            "dataset_uri": "s3://abc",
            "extra": {"foo": "bar"},
            "source_dag_id": None,
            "source_task_id": None,
            "source_run_id": None,
            "source_map_index": -1,
            "timestamp": self.timestamp,
            "created_dagruns": [],
        }


class TestDatasetEventCollectionSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        common = {
            "dataset_id": 10,
            "extra": {"foo": "bar"},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
            "created_dagruns": [],
        }

        events = [DatasetEvent(id=i, **common) for i in [1, 2]]
        session.add_all(events)
        session.flush()
        serialized_data = dataset_event_collection_schema.dump(
            DatasetEventCollection(dataset_events=events, total_entries=2)
        )
        assert serialized_data == {
            "dataset_events": [
                {"id": 1, "timestamp": self.timestamp, **common},
                {"id": 2, "timestamp": self.timestamp, **common},
            ],
            "total_entries": 2,
        }
