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

import urllib

import pytest

from airflow.models import DagModel
from airflow.models.asset import AssetModel, DagScheduleAssetReference, TaskOutletAssetReference
from airflow.utils import timezone
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_assets

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


def _create_assets(session, num: int = 2) -> None:
    default_time = "2020-06-11T18:00:00+00:00"
    assets = [
        AssetModel(
            id=i,
            uri=f"s3://bucket/key/{i}",
            extra={"foo": "bar"},
            created_at=timezone.parse(default_time),
            updated_at=timezone.parse(default_time),
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets)
    session.commit()


def _create_provided_asset(session, asset: AssetModel) -> None:
    session.add(asset)
    session.commit()


class TestAssets:
    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_assets()

    def teardown_method(self) -> None:
        clear_db_assets()

    @provide_session
    def create_assets(self, session, num: int = 2):
        _create_assets(session=session, num=num)

    @provide_session
    def create_provided_asset(self, session, asset: AssetModel):
        _create_provided_asset(session=session, asset=asset)


class TestGetAssets(TestAssets):
    def test_should_respond_200(self, test_client, session):
        self.create_assets()
        assets = session.query(AssetModel).all()
        assert len(assets) == 2

        response = test_client.get("/public/assets")
        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = self.default_time.replace("+00:00", "Z")
        assert response_data == {
            "assets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/public/assets?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        "params, expected_assets",
        [
            ({"uri_pattern": "s3"}, {"s3://folder/key"}),
            ({"uri_pattern": "bucket"}, {"gcp://bucket/key", "wasb://some_asset_bucket_/key"}),
            (
                {"uri_pattern": "asset"},
                {"somescheme://asset/key", "wasb://some_asset_bucket_/key"},
            ),
            (
                {"uri_pattern": ""},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
        ],
    )
    @provide_session
    def test_filter_assets_by_uri_pattern_works(self, test_client, params, expected_assets, session):
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        asset4 = AssetModel("wasb://some_asset_bucket_/key")

        assets = [asset1, asset2, asset3, asset4]
        for a in assets:
            self.create_provided_asset(asset=a)

        response = test_client.get("/public/assets", params=params)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls

    @pytest.mark.parametrize("dag_ids, expected_num", [("dag1,dag2", 2), ("dag3", 1), ("dag2,dag3", 2)])
    @provide_session
    def test_filter_assets_by_dag_ids_works(self, test_client, dag_ids, expected_num, session):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", asset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", asset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = test_client.get(
            f"/public/assets?dag_ids={dag_ids}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num

    @pytest.mark.parametrize(
        "dag_ids, uri_pattern,expected_num",
        [("dag1,dag2", "folder", 1), ("dag3", "nothing", 0), ("dag2,dag3", "key", 2)],
    )
    @provide_session
    def test_filter_assets_by_dag_ids_and_uri_pattern_works(
        self, test_client, dag_ids, uri_pattern, expected_num, session
    ):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", asset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", asset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = test_client.get(
            f"/public/assets?dag_ids={dag_ids}&uri_pattern={uri_pattern}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num


class TestGetAssetsEndpointPagination(TestAssets):
    @pytest.mark.parametrize(
        "url, expected_asset_uris",
        [
            # Limit test data
            ("/public/assets?limit=1", ["s3://bucket/key/1"]),
            ("/public/assets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/public/assets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ("/public/assets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/public/assets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ],
    )
    def test_limit_and_offset(self, test_client, url, expected_asset_uris):
        self.create_assets(num=110)

        response = test_client.get(url)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json()["assets"]]
        assert asset_uris == expected_asset_uris

    def test_should_respect_page_size_limit_default(self, test_client):
        self.create_assets(num=110)

        response = test_client.get("/public/assets")

        assert response.status_code == 200
        assert len(response.json()["assets"]) == 100


class TestGetAssetEndpoint(TestAssets):
    @pytest.mark.parametrize(
        "url",
        [
            urllib.parse.quote(
                "s3://bucket/key/1", safe=""
            ),  # api should cover raw as well as unquoted case like legacy
            "s3://bucket/key/1",
        ],
    )
    @provide_session
    def test_should_respond_200(self, test_client, url, session):
        self.create_assets(num=1)
        assert session.query(AssetModel).count() == 1
        tz_datetime_format = self.default_time.replace("+00:00", "Z")
        response = test_client.get(
            f"/public/assets/{url}",
        )
        assert response.status_code == 200
        assert response.json() == {
            "id": 1,
            "uri": "s3://bucket/key/1",
            "extra": {"foo": "bar"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "consuming_dags": [],
            "producing_tasks": [],
            "aliases": [],
        }

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            f"/public/assets/{urllib.parse.quote('s3://bucket/key', safe='')}",
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "The Asset with uri: `s3://bucket/key` was not found"
