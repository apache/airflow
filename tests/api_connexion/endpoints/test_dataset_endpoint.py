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

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import Dataset
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import provide_session
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_datasets


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestDatasetEndpoint:

    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()
        clear_db_datasets()

    def teardown_method(self) -> None:
        clear_db_datasets()

    def _create_dataset(self, session):
        dataset_model = Dataset(
            uri="s3://bucket/key",
            extra={"foo": "bar"},
            created_at=timezone.parse(self.default_time),
            updated_at=timezone.parse(self.default_time),
        )
        session.add(dataset_model)
        session.commit()

    @staticmethod
    def _normalize_dataset_ids(datasets):
        for i, dataset in enumerate(datasets, 1):
            dataset["id"] = i


class TestGetDatasetEndpoint(TestDatasetEndpoint):
    def test_should_respond_200(self, session):
        self._create_dataset(session)
        result = session.query(Dataset).all()
        assert len(result) == 1
        response = self.client.get(
            f"/api/v1/datasets/{result[0].id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == {
            "id": result[0].id,
            "uri": "s3://bucket/key",
            "extra": "{'foo': 'bar'}",
            "created_at": self.default_time,
            "updated_at": self.default_time,
        }

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/datasets/1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404
        assert {
            'detail': "The Dataset with id: `1` was not found",
            'status': 404,
            'title': 'Dataset not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, session):
        self._create_dataset(session)
        dataset = session.query(Dataset).first()
        response = self.client.get(f"/api/v1/datasets/{dataset.id}")

        assert_401(response)


class TestGetDatasets(TestDatasetEndpoint):
    def test_should_respond_200(self, session):
        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(2)
        ]
        session.add_all(datasets)
        session.commit()
        result = session.query(Dataset).all()
        assert len(result) == 2
        response = self.client.get("/api/v1/datasets", environ_overrides={'REMOTE_USER': "test"})

        assert response.status_code == 200
        response_data = response.json
        self._normalize_dataset_ids(response_data['datasets'])
        assert response_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": "{'foo': 'bar'}",
                    "created_at": self.default_time,
                    "updated_at": self.default_time,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": "{'foo': 'bar'}",
                    "created_at": self.default_time,
                    "updated_at": self.default_time,
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, session):
        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(2)
        ]
        session.add_all(datasets)
        session.commit()
        result = session.query(Dataset).all()
        assert len(result) == 2

        response = self.client.get(
            "/api/v1/datasets?order_by=fake", environ_overrides={'REMOTE_USER': "test"}
        )  # missing attr

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json['detail'] == msg

    def test_should_raises_401_unauthenticated(self, session):
        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(2)
        ]
        session.add_all(datasets)
        session.commit()
        result = session.query(Dataset).all()
        assert len(result) == 2

        response = self.client.get("/api/v1/datasets")

        assert_401(response)


class TestGetDatasetsEndpointPagination(TestDatasetEndpoint):
    @parameterized.expand(
        [
            # Limit test data
            ("/api/v1/datasets?limit=1", ["s3://bucket/key/1"]),
            ("/api/v1/datasets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/api/v1/datasets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ("/api/v1/datasets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/api/v1/datasets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ]
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_dataset_uris, session):
        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(datasets)
        session.commit()

        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})

        assert response.status_code == 200
        dataset_uris = [dataset["uri"] for dataset in response.json["datasets"]]
        assert dataset_uris == expected_dataset_uris

    def test_should_respect_page_size_limit_default(self, session):
        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(datasets)
        session.commit()
        response = self.client.get("/api/v1/datasets", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert len(response.json['datasets']) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        datasets = [
            Dataset(
                uri=f"s3://bucket/key/{i+1}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(200)
        ]
        session.add_all(datasets)
        session.commit()
        response = self.client.get("/api/v1/datasets?limit=180", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert len(response.json['datasets']) == 150
