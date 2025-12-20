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

from unittest import mock

import pendulum
import pytest
from sqlalchemy import select

from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_dags()
    clear_db_serialized_dags()


class TestNextRunAssets:
    def test_should_response_200(self, test_client, dag_maker):
        with dag_maker(
            dag_id="upstream",
            schedule=[Asset(uri="s3://bucket/next-run-asset/1", name="asset1")],
            serialized=True,
        ):
            EmptyOperator(task_id="task1")

        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with assert_queries_count(4):
            response = test_client.get("/next_run_assets/upstream")

        assert response.status_code == 200
        assert response.json() == {
            "asset_expression": {
                "all": [
                    {
                        "asset": {
                            "uri": "s3://bucket/next-run-asset/1",
                            "name": "asset1",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    }
                ]
            },
            "events": [
                {"id": mock.ANY, "uri": "s3://bucket/next-run-asset/1", "name": "asset1", "lastUpdate": None}
            ],
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/next_run_assets/upstream")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/next_run_assets/upstream")
        assert response.status_code == 403

    def test_should_set_last_update_only_for_queued_and_hide_flag(self, test_client, dag_maker, session):
        with dag_maker(
            dag_id="two_assets_equal",
            schedule=[
                Asset(uri="s3://bucket/A", name="A"),
                Asset(uri="s3://bucket/B", name="B"),
            ],
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        assets = {
            a.uri: a
            for a in session.scalars(
                select(AssetModel).where(AssetModel.uri.in_(["s3://bucket/A", "s3://bucket/B"]))
            )
        }
        # Queue and add an event only for A
        session.add(AssetDagRunQueue(asset_id=assets["s3://bucket/A"].id, target_dag_id="two_assets_equal"))
        session.add(
            AssetEvent(asset_id=assets["s3://bucket/A"].id, timestamp=dr.logical_date or pendulum.now())
        )
        session.commit()

        response = test_client.get("/next_run_assets/two_assets_equal")
        assert response.status_code == 200
        assert response.json() == {
            "asset_expression": {
                "all": [
                    {
                        "asset": {
                            "uri": "s3://bucket/A",
                            "name": "A",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    },
                    {
                        "asset": {
                            "uri": "s3://bucket/B",
                            "name": "B",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    },
                ]
            },
            # events are ordered by uri
            "events": [
                {"id": mock.ANY, "uri": "s3://bucket/A", "name": "A", "lastUpdate": mock.ANY},
                {"id": mock.ANY, "uri": "s3://bucket/B", "name": "B", "lastUpdate": None},
            ],
        }

    def test_last_update_respects_latest_run_filter(self, test_client, dag_maker, session):
        with dag_maker(
            dag_id="filter_run",
            schedule=[Asset(uri="s3://bucket/F", name="F")],
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset = session.scalars(select(AssetModel).where(AssetModel.uri == "s3://bucket/F")).one()
        session.add(AssetDagRunQueue(asset_id=asset.id, target_dag_id="filter_run"))
        # event before latest_run should be ignored
        ts_base = dr.logical_date or pendulum.now()
        session.add(AssetEvent(asset_id=asset.id, timestamp=ts_base.subtract(minutes=10)))
        # event after latest_run counts
        session.add(AssetEvent(asset_id=asset.id, timestamp=ts_base.add(minutes=10)))
        session.commit()

        resp = test_client.get("/next_run_assets/filter_run")
        assert resp.status_code == 200
        ev = resp.json()["events"][0]
        assert ev["lastUpdate"] is not None
        assert "queued" not in ev
