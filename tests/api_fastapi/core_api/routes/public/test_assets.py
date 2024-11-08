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

from airflow.models.asset import AssetModel
from airflow.utils import timezone
from airflow.utils.session import provide_session

from tests_common.test_utils.config import conf_vars
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


def _create_provided_asset(asset: AssetModel, session) -> None:
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
        _create_assets(session, num=num)

    @provide_session
    def create_provided_asset(self, session, asset: AssetModel):
        _create_provided_asset(session, asset)


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
        "url, expected_assets",
        [
            ("/public/assets?uri_pattern=s3", {"s3://folder/key"}),
            ("/public/assets?uri_pattern=bucket", {"gcp://bucket/key", "wasb://some_asset_bucket_/key"}),
            (
                "/public/assets?uri_pattern=asset",
                {"somescheme://asset/key", "wasb://some_asset_bucket_/key"},
            ),
            (
                "/public/assets?uri_pattern=",
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
    def test_filter_assets_by_uri_pattern_works(self, test_client, url, expected_assets, session):
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        asset4 = AssetModel("wasb://some_asset_bucket_/key")

        assets = [asset1, asset2, asset3, asset4]
        for a in assets:
            self.create_provided_asset(a)

        response = test_client.get(url)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls


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
        self.create_assets(110)

        response = test_client.get(url)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json()["assets"]]
        assert asset_uris == expected_asset_uris

    def test_should_respect_page_size_limit_default(self, test_client):
        self.create_assets(110)

        response = test_client.get("/public/assets")

        assert response.status_code == 200
        assert len(response.json()["assets"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, test_client):
        self.create_assets(200)

        # change to 180 once format_parameters is integrated
        response = test_client.get("/public/assets?limit=150")

        assert response.status_code == 200
        assert len(response.json()["assets"]) == 150
