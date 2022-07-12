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

from airflow import Dataset
from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    dataset_collection_schema,
    dataset_schema,
)
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
            "extra": "{'foo': 'bar'}",
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
                    "extra": "{'foo': 'bar'}",
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": "{'foo': 'bar'}",
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp,
                },
            ],
            "total_entries": 2,
        }
