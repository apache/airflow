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

import pendulum
import pytest
from dateutil.tz import UTC

from airflow.assets import Asset
from airflow.models.asset import AssetActive, AssetEvent, AssetModel
from airflow.operators.empty import EmptyOperator

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_assets

pytestmark = pytest.mark.db_test


class TestAssetEndpoint:
    @pytest.fixture(autouse=True)
    def _cleanup(self):
        clear_db_assets()
        yield
        clear_db_assets()

    @pytest.fixture
    def create_assets(self, session):
        def create(indexes):
            assets = [AssetModel(id=i, uri=f"s3://bucket/key/{i}") for i in indexes]
            session.add_all(assets)
            session.flush()
            session.add_all(AssetActive.for_asset(a) for a in assets)
            session.flush()
            return assets

        return create


class TestGetAssets(TestAssetEndpoint):
    def test_should_respond_200(self, admin_client, create_assets, session):
        create_assets([1, 2])
        session.commit()
        assert session.query(AssetModel).count() == 2

        with assert_queries_count(10):
            response = admin_client.get("/object/assets_summary")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "assets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "last_asset_update": None,
                    "total_updates": 0,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "last_asset_update": None,
                    "total_updates": 0,
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(
        self, admin_client, create_assets, session
    ):
        create_assets([1, 2])
        session.commit()
        assert session.query(AssetModel).count() == 2

        response = admin_client.get("/object/assets_summary?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_order_by_raises_400_for_invalid_datetimes(
        self, admin_client, create_assets, session
    ):
        create_assets([1, 2])
        session.commit()
        assert session.query(AssetModel).count() == 2

        response = admin_client.get("/object/assets_summary?updated_before=null")

        assert response.status_code == 400
        assert "Invalid datetime:" in response.text

        response = admin_client.get("/object/assets_summary?updated_after=null")

        assert response.status_code == 400
        assert "Invalid datetime:" in response.text

    def test_filter_by_datetimes(self, admin_client, create_assets, session):
        today = pendulum.today("UTC")

        assets = create_assets(range(1, 4))
        # Update assets, one per day, starting with assets[0], ending with assets[2]
        asset_events = [
            AssetEvent(
                asset_id=a.id,
                timestamp=today.add(days=-len(assets) + i + 1),
            )
            for i, a in enumerate(assets)
        ]
        session.add_all(asset_events)
        session.commit()
        assert session.query(AssetModel).count() == len(assets)

        cutoff = today.add(days=-1).add(minutes=-5).to_iso8601_string()
        response = admin_client.get(f"/object/assets_summary?updated_after={cutoff}")

        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        assert [json_dict["id"] for json_dict in response.json["assets"]] == [2, 3]

        cutoff = today.add(days=-1).add(minutes=5).to_iso8601_string()
        response = admin_client.get(f"/object/assets_summary?updated_before={cutoff}")

        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        assert [json_dict["id"] for json_dict in response.json["assets"]] == [1, 2]

    @pytest.mark.parametrize(
        "order_by, ordered_asset_ids",
        [
            ("uri", [1, 2, 3, 4]),
            ("-uri", [4, 3, 2, 1]),
            ("last_asset_update", [4, 1, 3, 2]),
            ("-last_asset_update", [2, 3, 1, 4]),
        ],
    )
    def test_order_by(
        self, admin_client, session, create_assets, order_by, ordered_asset_ids
    ):
        assets = create_assets(range(1, len(ordered_asset_ids) + 1))
        asset_events = [
            AssetEvent(
                asset_id=assets[2].id,
                timestamp=pendulum.today("UTC").add(days=-3),
            ),
            AssetEvent(
                asset_id=assets[1].id,
                timestamp=pendulum.today("UTC").add(days=-2),
            ),
            AssetEvent(
                asset_id=assets[1].id,
                timestamp=pendulum.today("UTC").add(days=-1),
            ),
        ]
        session.add_all(asset_events)
        session.commit()
        assert session.query(AssetModel).count() == len(ordered_asset_ids)

        response = admin_client.get(f"/object/assets_summary?order_by={order_by}")

        assert response.status_code == 200
        assert ordered_asset_ids == [
            json_dict["id"] for json_dict in response.json["assets"]
        ]
        assert response.json["total_entries"] == len(ordered_asset_ids)

    def test_search_uri_pattern(self, admin_client, create_assets, session):
        create_assets([1, 2])
        session.commit()
        assert session.query(AssetModel).count() == 2

        uri_pattern = "key_2"
        response = admin_client.get(f"/object/assets_summary?uri_pattern={uri_pattern}")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "assets": [
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "last_asset_update": None,
                    "total_updates": 0,
                },
            ],
            "total_entries": 1,
        }

        uri_pattern = "s3://bucket/key_"
        response = admin_client.get(f"/object/assets_summary?uri_pattern={uri_pattern}")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "assets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "last_asset_update": None,
                    "total_updates": 0,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "last_asset_update": None,
                    "total_updates": 0,
                },
            ],
            "total_entries": 2,
        }

    @pytest.mark.need_serialized_dag
    def test_correct_counts_update(
        self, admin_client, session, dag_maker, app, monkeypatch
    ):
        with monkeypatch.context() as m:
            assets = [Asset(uri=f"s3://bucket/key/{i}") for i in [1, 2, 3, 4, 5]]

            # DAG that produces asset #1
            with dag_maker(
                dag_id="upstream", schedule=None, serialized=True, session=session
            ):
                EmptyOperator(task_id="task1", outlets=[assets[0]])

            # DAG that is consumes only assets #1 and #2
            with dag_maker(
                dag_id="downstream", schedule=assets[:2], serialized=True, session=session
            ):
                EmptyOperator(task_id="task1")

            # We create multiple asset-producing and asset-consuming DAGs because the query requires
            # COUNT(DISTINCT ...) for total_updates, or else it returns a multiple of the correct number due
            # to the outer joins with DagScheduleAssetReference and TaskOutletAssetReference
            # Two independent DAGs that produce asset #3
            with dag_maker(
                dag_id="independent_producer_1", serialized=True, session=session
            ):
                EmptyOperator(task_id="task1", outlets=[assets[2]])
            with dag_maker(
                dag_id="independent_producer_2", serialized=True, session=session
            ):
                EmptyOperator(task_id="task1", outlets=[assets[2]])
            # Two independent DAGs that consume asset #4
            with dag_maker(
                dag_id="independent_consumer_1",
                schedule=[assets[3]],
                serialized=True,
                session=session,
            ):
                EmptyOperator(task_id="task1")
            with dag_maker(
                dag_id="independent_consumer_2",
                schedule=[assets[3]],
                serialized=True,
                session=session,
            ):
                EmptyOperator(task_id="task1")

            # Independent DAG that is produces and consumes the same asset, #5
            with dag_maker(
                dag_id="independent_producer_self_consumer",
                schedule=[assets[4]],
                serialized=True,
                session=session,
            ):
                EmptyOperator(task_id="task1", outlets=[assets[4]])

            m.setattr(app, "dag_bag", dag_maker.dagbag)

            asset1_id = session.query(AssetModel.id).filter_by(uri=assets[0].uri).scalar()
            asset2_id = session.query(AssetModel.id).filter_by(uri=assets[1].uri).scalar()
            asset3_id = session.query(AssetModel.id).filter_by(uri=assets[2].uri).scalar()
            asset4_id = session.query(AssetModel.id).filter_by(uri=assets[3].uri).scalar()
            asset5_id = session.query(AssetModel.id).filter_by(uri=assets[4].uri).scalar()

            # asset 1 events
            session.add_all(
                [
                    AssetEvent(
                        asset_id=asset1_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(3)
                ]
            )
            # asset 3 events
            session.add_all(
                [
                    AssetEvent(
                        asset_id=asset3_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(3)
                ]
            )
            # asset 4 events
            session.add_all(
                [
                    AssetEvent(
                        asset_id=asset4_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(4)
                ]
            )
            # asset 5 events
            session.add_all(
                [
                    AssetEvent(
                        asset_id=asset5_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(5)
                ]
            )
            session.commit()

            response = admin_client.get("/object/assets_summary")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "assets": [
                {
                    "id": asset1_id,
                    "uri": "s3://bucket/key/1",
                    "last_asset_update": "2022-08-01T02:00:00+00:00",
                    "total_updates": 3,
                },
                {
                    "id": asset2_id,
                    "uri": "s3://bucket/key/2",
                    "last_asset_update": None,
                    "total_updates": 0,
                },
                {
                    "id": asset3_id,
                    "uri": "s3://bucket/key/3",
                    "last_asset_update": "2022-08-01T02:00:00+00:00",
                    "total_updates": 3,
                },
                {
                    "id": asset4_id,
                    "uri": "s3://bucket/key/4",
                    "last_asset_update": "2022-08-01T03:00:00+00:00",
                    "total_updates": 4,
                },
                {
                    "id": asset5_id,
                    "uri": "s3://bucket/key/5",
                    "last_asset_update": "2022-08-01T04:00:00+00:00",
                    "total_updates": 5,
                },
            ],
            "total_entries": 5,
        }


class TestGetAssetsEndpointPagination(TestAssetEndpoint):
    @pytest.mark.parametrize(
        "url, expected_asset_uris",
        [
            # Limit test data
            ("/object/assets_summary?limit=1", ["s3://bucket/key/1"]),
            (
                "/object/assets_summary?limit=5",
                [f"s3://bucket/key/{i}" for i in range(1, 6)],
            ),
            # Offset test data
            (
                "/object/assets_summary?offset=1",
                [f"s3://bucket/key/{i}" for i in range(2, 10)],
            ),
            (
                "/object/assets_summary?offset=3",
                [f"s3://bucket/key/{i}" for i in range(4, 10)],
            ),
            # Limit and offset test data
            (
                "/object/assets_summary?offset=3&limit=3",
                [f"s3://bucket/key/{i}" for i in [4, 5, 6]],
            ),
        ],
    )
    def test_limit_and_offset(
        self, admin_client, create_assets, session, url, expected_asset_uris
    ):
        create_assets(range(1, 10))
        session.commit()

        response = admin_client.get(url)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json["assets"]]
        assert asset_uris == expected_asset_uris

    def test_should_respect_page_size_limit_default(
        self, admin_client, create_assets, session
    ):
        create_assets(range(1, 60))
        session.commit()

        response = admin_client.get("/object/assets_summary")

        assert response.status_code == 200
        assert len(response.json["assets"]) == 25

    def test_should_return_max_if_req_above(self, admin_client, create_assets, session):
        create_assets(range(1, 60))
        session.commit()

        response = admin_client.get("/object/assets_summary?limit=180")

        assert response.status_code == 200
        assert len(response.json["assets"]) == 50


class TestGetAssetNextRunSummary(TestAssetEndpoint):
    def test_next_run_asset_summary(self, dag_maker, admin_client):
        with dag_maker(
            dag_id="upstream", schedule=[Asset(uri="s3://bucket/key/1")], serialized=True
        ):
            EmptyOperator(task_id="task1")

        response = admin_client.post(
            "/next_run_assets_summary", data={"dag_ids": ["upstream"]}
        )

        assert response.status_code == 200
        assert response.json == {
            "upstream": {"ready": 0, "total": 1, "uri": "s3://bucket/key/1"}
        }
