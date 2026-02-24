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
from sqlalchemy import select

from airflow.models.asset import AssetEvent, AssetModel, AssetPartitionDagRun, PartitionedAssetKeyLog
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_apdr, clear_db_dags, clear_db_pakl, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_apdr()
    clear_db_pakl()


class TestGetPartitionedDagRuns:
    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/partitioned_dag_runs?dag_id=any")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/partitioned_dag_runs?dag_id=any")
        assert response.status_code == 403

    def test_should_response_404(self, test_client):
        assert test_client.get("/partitioned_dag_runs?dag_id=no_such_dag").status_code == 404

    def test_should_response_200_non_partitioned_dag_returns_empty(self, test_client, dag_maker):
        with dag_maker(dag_id="normal", schedule=[Asset(uri="s3://bucket/a", name="a")], serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with assert_queries_count(2):
            resp = test_client.get("/partitioned_dag_runs?dag_id=normal&has_created_dag_run_id=false")
        assert resp.status_code == 200
        assert resp.json() == {"partitioned_dag_runs": [], "total": 0, "asset_expressions": None}

    @pytest.mark.parametrize(
        (
            "num_assets",
            "received_count",
            "fulfilled",
            "has_created_dag_run_id",
            "expected_total",
            "expected_state",
        ),
        [
            (1, 1, False, False, 1, "pending"),
            (1, 1, True, False, 0, None),
            (1, 1, True, True, 1, "running"),
            (3, 0, False, False, 1, "pending"),
            (3, 1, False, False, 1, "pending"),
            (3, 2, False, False, 1, "pending"),
            (3, 3, False, False, 1, "pending"),
        ],
        ids=[
            "filter-pending-included",
            "filter-fulfilled-excluded",
            "filter-fulfilled-included",
            "received-0/3",
            "received-1/3",
            "received-2/3",
            "received-3/3",
        ],
    )
    def test_should_response_200(
        self,
        test_client,
        dag_maker,
        session,
        num_assets,
        received_count,
        fulfilled,
        has_created_dag_run_id,
        expected_total,
        expected_state,
    ):
        uris = [f"s3://bucket/lr{i}" for i in range(num_assets)]
        asset_defs = [Asset(uri=uri, name=f"lr{i}") for i, uri in enumerate(uris)]
        schedule = asset_defs[0]
        for a in asset_defs[1:]:
            schedule = schedule & a

        with dag_maker(
            dag_id="list_dag",
            schedule=PartitionedAssetTimetable(assets=schedule),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        assets = {a.uri: a for a in session.scalars(select(AssetModel).where(AssetModel.uri.in_(uris)))}

        pdr = AssetPartitionDagRun(
            target_dag_id="list_dag",
            partition_key="2024-06-01",
            created_dag_run_id=dr.id if fulfilled else None,
        )
        session.add(pdr)
        session.flush()

        for uri in uris[:received_count]:
            event = AssetEvent(asset_id=assets[uri].id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=assets[uri].id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-06-01",
                    target_dag_id="list_dag",
                    target_partition_key="2024-06-01",
                )
            )
        session.commit()

        with assert_queries_count(2):
            resp = test_client.get(
                f"/partitioned_dag_runs?dag_id=list_dag"
                f"&has_created_dag_run_id={str(has_created_dag_run_id).lower()}"
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == expected_total
        if expected_total > 0:
            pdr_resp = body["partitioned_dag_runs"][0]
            assert pdr_resp["state"] == expected_state
            assert pdr_resp["total_received"] == received_count
            assert pdr_resp["total_required"] == num_assets


class TestGetPendingPartitionedDagRun:
    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/pending_partitioned_dag_run/any_dag/any_key")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/pending_partitioned_dag_run/any_dag/any_key")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("dag_id", "partition_key", "fulfilled"),
        [
            ("no_dag", "no_key", False),
            ("fulfilled_dag", "2024-07-01", True),
        ],
        ids=[
            "not-found",
            "fulfilled-excluded",
        ],
    )
    def test_should_response_404(self, test_client, dag_maker, session, dag_id, partition_key, fulfilled):
        if fulfilled:
            with dag_maker(
                dag_id="fulfilled_dag",
                schedule=PartitionedAssetTimetable(assets=Asset(uri="s3://bucket/ful0", name="ful0")),
                serialized=True,
            ):
                EmptyOperator(task_id="t")

            dr = dag_maker.create_dagrun()
            dag_maker.sync_dagbag_to_db()

            session.add(
                AssetPartitionDagRun(
                    target_dag_id="fulfilled_dag",
                    partition_key="2024-07-01",
                    created_dag_run_id=dr.id,
                )
            )
            session.commit()

        resp = test_client.get(f"/pending_partitioned_dag_run/{dag_id}/{partition_key}")
        assert resp.status_code == 404

    @pytest.mark.parametrize(
        ("num_assets", "received_count"),
        [
            (1, 1),
            (1, 0),
            (2, 1),
            (2, 2),
            (2, 0),
        ],
        ids=[
            "1-asset-received-pending",
            "1-asset-none-received-pending",
            "2-assets-partial-pending",
            "2-assets-all-received-pending",
            "2-assets-none-received-pending",
        ],
    )
    def test_should_response_200(self, test_client, dag_maker, session, num_assets, received_count):
        uris = [f"s3://bucket/dt{i}" for i in range(num_assets)]
        asset_defs = [Asset(uri=uri, name=f"dt{i}") for i, uri in enumerate(uris)]
        schedule = asset_defs[0] if num_assets == 1 else asset_defs[0] & asset_defs[1]

        with dag_maker(
            dag_id="detail_dag",
            schedule=PartitionedAssetTimetable(assets=schedule),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        assets = {a.uri: a for a in session.scalars(select(AssetModel).where(AssetModel.uri.in_(uris)))}

        pdr = AssetPartitionDagRun(
            target_dag_id="detail_dag",
            partition_key="2024-07-01",
            created_dag_run_id=None,
        )
        session.add(pdr)
        session.flush()

        for uri in uris[:received_count]:
            event = AssetEvent(asset_id=assets[uri].id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=assets[uri].id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-07-01",
                    target_dag_id="detail_dag",
                    target_partition_key="2024-07-01",
                )
            )
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/detail_dag/2024-07-01")
        assert resp.status_code == 200
        body = resp.json()
        assert body["dag_id"] == "detail_dag"
        assert body["partition_key"] == "2024-07-01"
        assert body["total_required"] == num_assets
        assert body["total_received"] == received_count
        assert len(body["assets"]) == num_assets
        assert body["asset_expression"] is not None
        assert body["created_dag_run_id"] is None

        received_uris = {a["asset_uri"] for a in body["assets"] if a["received"]}
        assert received_uris == set(uris[:received_count])
