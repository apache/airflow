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

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from sqlalchemy import delete, event, func, select

from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models import Log
from airflow.models.asset import (
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    PartitionedAssetKeyLog,
    association_table,
)
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_apdr, clear_db_pakl
from tests_common.test_utils.logs import check_last_log
from unit.api_fastapi.core_api.routes.public.test_assets import DEFAULT_DATE, TestAssets

pytestmark = pytest.mark.db_test


class TestAssetEventDeletionConcurrency(TestAssets):
    @pytest.fixture(autouse=True)
    def setup_partitions(self, setup):
        clear_db_pakl()
        clear_db_apdr()
        yield
        clear_db_pakl()
        clear_db_apdr()

    @pytest.mark.parametrize("queue_type", ["regular", "partitioned"])
    @pytest.mark.parametrize("delete_queued_events", ["false", "true"])
    def test_retries_after_scheduler_consumption_lock_conflict(
        self, test_client, session, delete_queued_events, queue_type
    ):
        engine = session.get_bind()
        if engine.dialect.name != "postgresql":
            pytest.skip("Requires PostgreSQL deadlock detection")
        self.create_assets(session=session, num=1)
        self.create_dag_run(session=session, num=1)
        run_id = session.scalar(
            select(DagRun.id).where(DagRun.dag_id == "source_dag_id", DagRun.run_id == "source_run_id_1")
        )
        before = DEFAULT_DATE + timedelta(days=1)
        session.add_all(
            [
                AssetEvent(id=1, asset_id=1, timestamp=DEFAULT_DATE),
                AssetEvent(id=2, asset_id=1, timestamp=before),
            ]
        )
        session.flush()
        if queue_type == "partitioned":
            session.add(
                AssetPartitionDagRun(
                    id=1,
                    target_dag_id="source_dag_id",
                    partition_key="partition-1",
                    pending_partition_key="partition-1",
                )
            )
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=1,
                    asset_event_id=1,
                    asset_partition_dag_run_id=1,
                    source_partition_key="partition-1",
                    target_dag_id="source_dag_id",
                    target_partition_key="partition-1",
                )
            )
        else:
            session.add(AssetDagRunQueue(target_dag_id="source_dag_id", asset_id=1, asset_event_id=1))
        session.commit()
        queue_locked = Event()
        event_locked = Event()
        scheduler_inserting = Event()
        scheduler_committed = Event()

        def consume_event():
            with create_session(scoped=False) as scheduler_session:
                # For regular queues, deletion must detect the cycle first so its retry is exercised.
                scheduler_session.connection().exec_driver_sql("SET LOCAL deadlock_timeout = '10s'")
                scheduler_session.connection().exec_driver_sql("SET LOCAL lock_timeout = '15s'")
                if queue_type == "partitioned":
                    partition_run = scheduler_session.scalar(
                        select(AssetPartitionDagRun).where(AssetPartitionDagRun.id == 1).with_for_update()
                    )
                    assert partition_run is not None
                else:
                    runner = object.__new__(SchedulerJobRunner)
                    records = runner._lock_queued_asset_records(
                        dag_id="source_dag_id", load_assets=False, session=scheduler_session
                    )
                    assert len(records) == 1
                queue_locked.set()
                assert event_locked.wait(timeout=10)
                scheduler_inserting.set()
                scheduler_session.execute(association_table.insert(), [{"dag_run_id": run_id, "event_id": 1}])
                if queue_type == "partitioned":
                    partition_run.created_dag_run_id = run_id
                else:
                    scheduler_session.execute(
                        delete(AssetDagRunQueue).where(
                            AssetDagRunQueue.target_dag_id == "source_dag_id",
                            AssetDagRunQueue.asset_event_id == 1,
                        )
                    )
            scheduler_committed.set()

        def before_queue_read(conn, cursor, statement, parameters, context, executemany):
            compiled = context.compiled
            if compiled is None:
                return
            query = compiled.statement
            if scheduler_inserting.is_set():
                if (
                    queue_type == "partitioned"
                    and query.is_select
                    and query._for_update_arg is not None
                    and "FROM asset_event" in statement
                ):
                    # Let consumption commit before the retry can reacquire the event's FK parent lock.
                    assert scheduler_committed.wait(timeout=10)
                return
            queue_table = "asset_partition_dag_run" if queue_type == "partitioned" else "asset_dag_run_queue"
            if not (
                query.is_select
                and query._for_update_arg is not None
                and f"FROM {queue_table}" in statement
                and "SKIP LOCKED" not in statement
            ):
                return
            conn.exec_driver_sql("SET LOCAL deadlock_timeout = '100ms'")
            conn.exec_driver_sql("SET LOCAL lock_timeout = '15s'")
            event_locked.set()
            assert scheduler_inserting.wait(timeout=10)

        database_errors = []

        def record_database_error(context):
            error = context.original_exception
            database_errors.append(getattr(error, "sqlstate", None) or getattr(error, "pgcode", None))

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(consume_event)
            assert queue_locked.wait(timeout=10)
            event.listen(engine, "before_cursor_execute", before_queue_read)
            event.listen(engine, "handle_error", record_database_error)
            try:
                response = test_client.delete(
                    "/assets/1/events",
                    params={"before": before.isoformat(), "delete_queued_events": delete_queued_events},
                )
            finally:
                event.remove(engine, "before_cursor_execute", before_queue_read)
                event.remove(engine, "handle_error", record_database_error)
                event_locked.set()
            future.result(timeout=20)

        assert database_errors == (["55P03"] if queue_type == "partitioned" else ["40P01"])
        assert response.status_code == 204
        session.expire_all()
        assert session.scalars(select(AssetEvent.id)).all() == [2]
        assert session.scalars(select(AssetDagRunQueue.asset_event_id)).all() == []
        assert session.scalars(select(PartitionedAssetKeyLog.id)).all() == []
        assert session.execute(
            select(AssetPartitionDagRun.id, AssetPartitionDagRun.created_dag_run_id)
        ).all() == ([(1, run_id)] if queue_type == "partitioned" else [])
        assert session.execute(select(association_table)).all() == []
        assert session.get(AssetModel, 1) is not None
        assert session.get(DagRun, run_id) is not None
        assert (
            session.scalar(select(func.count()).select_from(Log).where(Log.event == "delete_asset_events"))
            == 1
        )
        check_last_log(
            session,
            dag_id=None,
            event="delete_asset_events",
            logical_date=None,
            expected_extra={
                "asset_id": "1",
                "before": before.isoformat(),
                "delete_queued_events": delete_queued_events,
                "method": "DELETE",
            },
        )
