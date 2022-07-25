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

from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    DatasetEventCollection,
    dataset_collection_schema,
    dataset_event_collection_schema,
    dataset_event_schema,
    dataset_schema,
)
from airflow.models.dataset import Dataset, DatasetEvent
from airflow.utils import timezone
from tests.test_utils.db import clear_db_datasets


class TestDatasetSchemaBase:
    def setup_method(self) -> None:
        clear_db_datasets()
        self.timestamp = "2022-06-10T12:02:44+00:00"

    def teardown_method(self) -> None:
        clear_db_datasets()


class TestDatasetSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        dataset = Dataset(
            uri="s3://bucket/key",
            extra={"foo": "bar"},
            created_at=timezone.parse(self.timestamp),
            updated_at=timezone.parse(self.timestamp),
        )
        session.add(dataset)
        session.flush()
        serialized_data = dataset_schema.dump(dataset)
        serialized_data['id'] = 1
        assert serialized_data == {
            "id": 1,
            "uri": "s3://bucket/key",
            "extra": {'foo': 'bar'},
            "created_at": self.timestamp,
            "updated_at": self.timestamp,
        }


class TestDatasetCollectionSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):

        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.timestamp),
                updated_at=timezone.parse(self.timestamp),
            )
            for i in range(2)
        ]
        session.add_all(datasets)
        session.flush()
        serialized_data = dataset_collection_schema.dump(
            DatasetCollection(datasets=datasets, total_entries=2)
        )
        serialized_data['datasets'][0]['id'] = 1
        serialized_data['datasets'][1]['id'] = 2
        assert serialized_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {'foo': 'bar'},
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {'foo': 'bar'},
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                },
            ],
            "total_entries": 2,
        }


class TestDatasetEventSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        d = Dataset('s3://abc')
        session.add(d)
        session.commit()
        event = DatasetEvent(
            id=1,
            dataset_id=d.id,
            extra={"foo": "bar"},
            source_dag_id="foo",
            source_task_id="bar",
            source_run_id="custom",
            source_map_index=-1,
            created_at=timezone.parse(self.timestamp),
        )
        session.add(event)
        session.flush()
        serialized_data = dataset_event_schema.dump(event)
        assert serialized_data == {
            "id": 1,
            "dataset_id": d.id,
            "dataset_uri": "s3://abc",
            "extra": {'foo': 'bar'},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
            "created_at": self.timestamp,
        }


class TestDatasetEventCollectionSchema(TestDatasetSchemaBase):
    def test_serialize(self, session):
        common = {
            "dataset_id": 10,
            "extra": {'foo': 'bar'},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
        }

        events = [DatasetEvent(id=i, created_at=timezone.parse(self.timestamp), **common) for i in [1, 2]]
        session.add_all(events)
        session.flush()
        serialized_data = dataset_event_collection_schema.dump(
            DatasetEventCollection(dataset_events=events, total_entries=2)
        )
        assert serialized_data == {
            "dataset_events": [
                {"id": 1, "created_at": self.timestamp, **common},
                {"id": 2, "created_at": self.timestamp, **common},
            ],
            "total_entries": 2,
        }
