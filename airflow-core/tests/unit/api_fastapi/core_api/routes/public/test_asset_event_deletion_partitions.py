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

from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import select

from airflow.models.asset import AssetEvent, AssetPartitionDagRun, PartitionedAssetKeyLog
from airflow.models.dagrun import DagRun

from tests_common.test_utils.db import clear_db_apdr, clear_db_pakl
from unit.api_fastapi.core_api.routes.public.test_assets import DEFAULT_DATE, TestAssets

pytestmark = pytest.mark.db_test


class TestAssetEventDeletionPartitions(TestAssets):
    @pytest.fixture(autouse=True)
    def setup_partitions(self, setup):
        clear_db_pakl()
        clear_db_apdr()
        yield
        clear_db_pakl()
        clear_db_apdr()

    @staticmethod
    def create_partition_reference(*, session, event_id, partition_run_id, dag_id="d", asset_id=1):
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset_id,
                asset_event_id=event_id,
                asset_partition_dag_run_id=partition_run_id,
                source_partition_key="source-key",
                target_dag_id=dag_id,
                target_partition_key=f"partition-{partition_run_id}",
            )
        )

    @pytest.mark.parametrize(
        ("delete_queued_events", "readable", "editable", "query_dag_id", "expected_status"),
        [
            (None, {"d", "d1"}, {"d", "d1"}, None, 409),
            ("false", {"d", "d1"}, {"d", "d1"}, None, 409),
            ("true", {"d"}, {"d", "d1"}, None, 403),
            ("true", {"d", "d1"}, {"d"}, None, 403),
            ("true", {"d"}, {"d"}, "d", 403),
            ("true", {"d", "d1"}, {"d", "d1"}, None, 204),
        ],
    )
    @mock.patch("airflow.api_fastapi.core_api.routes.public.assets._ASSET_EVENT_DELETE_BATCH_SIZE", 1)
    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_dag_ids",
        autospec=True,
    )
    def test_validates_pending_partition_references_across_batches(
        self,
        mock_authorized_ids,
        test_client,
        session,
        delete_queued_events,
        readable,
        editable,
        query_dag_id,
        expected_status,
    ):
        self.create_assets(session=session, num=1)
        session.add_all(AssetEvent(id=i, asset_id=1, timestamp=DEFAULT_DATE) for i in (1, 2))
        for partition_run_id, dag_id in ((1, "d"), (2, "d1")):
            session.add(
                AssetPartitionDagRun(
                    id=partition_run_id,
                    target_dag_id=dag_id,
                    partition_key=f"partition-{partition_run_id}",
                    pending_partition_key=f"partition-{partition_run_id}",
                )
            )
            self.create_partition_reference(
                session=session, event_id=partition_run_id, partition_run_id=partition_run_id, dag_id=dag_id
            )
        session.commit()
        mock_authorized_ids.side_effect = lambda _self, *, user, method: (
            readable if method == "GET" else editable
        )
        params = {"before": (DEFAULT_DATE + timedelta(days=1)).isoformat()}
        if delete_queued_events is not None:
            params["delete_queued_events"] = delete_queued_events
        if query_dag_id is not None:
            params["dag_id"] = query_dag_id

        response = test_client.delete("/assets/1/events", params=params)

        assert response.status_code == expected_status
        session.expire_all()
        remaining_ids = set() if expected_status == 204 else {1, 2}
        assert set(session.scalars(select(AssetEvent.id))) == remaining_ids
        assert set(session.scalars(select(PartitionedAssetKeyLog.asset_event_id))) == remaining_ids
        assert set(session.scalars(select(AssetPartitionDagRun.id))) == remaining_ids

    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_dag_ids",
        autospec=True,
        return_value={"d"},
    )
    def test_removes_only_selected_references_and_newly_empty_pending_runs(
        self, mock_authorized_ids, test_client, session
    ):
        self.create_assets(session=session)
        session.add_all(
            [
                AssetEvent(id=1, asset_id=1, timestamp=DEFAULT_DATE - timedelta(days=1)),
                AssetEvent(id=2, asset_id=1, timestamp=DEFAULT_DATE),
                AssetEvent(id=3, asset_id=2, timestamp=DEFAULT_DATE - timedelta(days=1)),
            ]
        )
        for partition_run_id, dag_id in ((1, "d"), (2, "d"), (3, "d1"), (4, "d1")):
            session.add(
                AssetPartitionDagRun(
                    id=partition_run_id,
                    target_dag_id=dag_id,
                    partition_key=f"partition-{partition_run_id}",
                    pending_partition_key=f"partition-{partition_run_id}",
                )
            )
        for event_id, partition_run_id, dag_id, asset_id in (
            (1, 1, "d", 1),
            (1, 2, "d", 1),
            (2, 2, "d", 1),
            (3, 2, "d", 2),
            (2, 3, "d1", 1),
            (3, 3, "d1", 2),
        ):
            self.create_partition_reference(
                session=session,
                event_id=event_id,
                partition_run_id=partition_run_id,
                dag_id=dag_id,
                asset_id=asset_id,
            )
        session.commit()

        response = test_client.delete(
            "/assets/1/events",
            params={"before": DEFAULT_DATE.isoformat(), "delete_queued_events": "true"},
        )

        assert response.status_code == 204
        session.expire_all()
        assert set(session.scalars(select(AssetEvent.id))) == {2, 3}
        assert set(
            session.execute(
                select(
                    PartitionedAssetKeyLog.asset_partition_dag_run_id, PartitionedAssetKeyLog.asset_event_id
                )
            )
        ) == {(2, 2), (2, 3), (3, 2), (3, 3)}
        assert set(session.scalars(select(AssetPartitionDagRun.id))) == {2, 3, 4}

    @pytest.mark.parametrize("delete_queued_events", [None, "false", "true"])
    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_dag_ids",
        autospec=True,
        return_value=set(),
    )
    def test_removes_completed_and_orphan_references_without_pending_queue_permissions(
        self, mock_authorized_ids, test_client, session, delete_queued_events
    ):
        self.create_assets(session=session, num=1)
        self.create_dag_run(session=session, num=1)
        run_id = session.scalar(
            select(DagRun.id).where(DagRun.dag_id == "source_dag_id", DagRun.run_id == "source_run_id_1")
        )
        session.add(AssetEvent(id=1, asset_id=1, timestamp=DEFAULT_DATE))
        session.add(
            AssetPartitionDagRun(
                id=1,
                target_dag_id="source_dag_id",
                partition_key="partition-1",
                created_dag_run_id=run_id,
            )
        )
        self.create_partition_reference(
            session=session, event_id=1, partition_run_id=1, dag_id="source_dag_id"
        )
        self.create_partition_reference(session=session, event_id=1, partition_run_id=99, dag_id="d1")
        session.commit()
        params = {"before": (DEFAULT_DATE + timedelta(days=1)).isoformat()}
        if delete_queued_events is not None:
            params["delete_queued_events"] = delete_queued_events

        response = test_client.delete("/assets/1/events", params=params)

        assert response.status_code == 204
        session.expire_all()
        assert session.scalars(select(AssetEvent.id)).all() == []
        assert session.scalars(select(PartitionedAssetKeyLog.id)).all() == []
        assert session.execute(
            select(AssetPartitionDagRun.id, AssetPartitionDagRun.created_dag_run_id)
        ).all() == [(1, run_id)]
        assert session.get(DagRun, run_id) is not None
