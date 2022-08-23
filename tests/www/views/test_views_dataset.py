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

import pytest
from parameterized import parameterized

from airflow.models.dataset import DatasetModel
from airflow.utils import timezone
from airflow.utils.session import provide_session
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_datasets


class TestDatasetEndpoint:

    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def cleanup(self) -> None:
        clear_db_datasets()
        yield
        clear_db_datasets()


class TestGetDatasets(TestDatasetEndpoint):
    def test_should_respond_200(self, admin_client, session):
        datasets = [
            DatasetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        with assert_queries_count(8):
            response = admin_client.get("/object/list_datasets")

        print(response.headers)
        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "last_dataset_update": None,
                    "total_updates": 0,
                    "producing_task_count": 0,
                    "consuming_dag_count": 0,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                    "producing_task_count": 0,
                    "consuming_dag_count": 0,
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        response = admin_client.get("/object/list_datasets?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json['detail'] == msg


class TestGetDatasetsEndpointPagination(TestDatasetEndpoint):
    @parameterized.expand(
        [
            # Limit test data
            ("/object/list_datasets?limit=1", ["s3://bucket/key/1"]),
            ("/object/list_datasets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/object/list_datasets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ("/object/list_datasets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/object/list_datasets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ]
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_dataset_uris, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(datasets)
        session.commit()

        response = self.client.get(url)

        assert response.status_code == 200
        dataset_uris = [dataset["uri"] for dataset in response.json["datasets"]]
        assert dataset_uris == expected_dataset_uris

    def test_should_respect_page_size_limit_default(self, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(datasets)
        session.commit()

        response = self.client.get("/object/list_datasets")

        assert response.status_code == 200
        assert len(response.json['datasets']) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 200)
        ]
        session.add_all(datasets)
        session.commit()

        response = self.client.get("/object/list_datasets?limit=180")

        assert response.status_code == 200
        assert len(response.json['datasets']) == 150
