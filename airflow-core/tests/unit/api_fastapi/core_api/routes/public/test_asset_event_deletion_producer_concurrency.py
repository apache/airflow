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
from threading import Event, get_ident
from unittest import mock

import pytest
from sqlalchemy import event, select

from airflow.assets.manager import AssetManager
from airflow.models.asset import AssetEvent, AssetPartitionDagRun, PartitionedAssetKeyLog
from airflow.partition_mappers.identity import IdentityMapper
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_apdr, clear_db_pakl
from unit.api_fastapi.core_api.routes.public.test_assets import DEFAULT_DATE, TestAssets

pytestmark = pytest.mark.db_test


class TestAssetEventDeletionProducerConcurrency(TestAssets):
    @pytest.fixture(autouse=True)
    def setup_partitions(self, setup):
        clear_db_pakl()
        clear_db_apdr()
        yield
        clear_db_pakl()
        clear_db_apdr()

    @pytest.mark.backend("postgres", "mysql")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.assets._ASSET_EVENT_DELETE_BATCH_SIZE", 1)
    def test_retries_without_aborting_producer_with_opposite_pending_run_order(
        self, test_client, session, dag_maker
    ):
        self.create_assets(session=session, num=1)
        asset = Asset(name="simple1", uri="s3://bucket/key/1")
        for dag_id in ("d", "d1"):
            with dag_maker(
                dag_id=dag_id,
                schedule=PartitionedAssetTimetable(
                    assets=asset, default_partition_mapper=IdentityMapper(), batch_asset_events=True
                ),
                serialized=True,
            ):
                EmptyOperator(task_id="task")
        dag_maker.sync_dagbag_to_db()

        # Producers visit d before d1, whereas deletion visits these rows in primary-key order.
        for partition_run_id, dag_id in ((1, "d1"), (2, "d")):
            session.add(
                AssetPartitionDagRun(
                    id=partition_run_id,
                    target_dag_id=dag_id,
                    partition_key="partition-1",
                    pending_partition_key="partition-1",
                    rollup_fingerprint={},
                )
            )
        # Make deletion the larger transaction so MySQL chooses the producer if a cycle forms.
        old_events = [AssetEvent(asset_id=1, timestamp=DEFAULT_DATE) for _ in range(20)]
        session.add_all(old_events)
        session.flush()
        for old_event in old_events:
            for partition_run_id, dag_id in ((1, "d1"), (2, "d")):
                session.add(
                    PartitionedAssetKeyLog(
                        asset_id=1,
                        asset_event_id=old_event.id,
                        asset_partition_dag_run_id=partition_run_id,
                        source_partition_key="partition-1",
                        target_dag_id=dag_id,
                        target_partition_key="partition-1",
                    )
                )
        session.commit()
        engine = session.get_bind()
        start_producer = Event()
        producer_first_lock = Event()
        deletion_first_lock = Event()
        producer_finished = Event()
        deletion_failed = Event()
        producer_lock_count = 0
        deletion_lock_count = 0
        producer_thread_id: int | None = None
        errors = []

        def register_event():
            nonlocal producer_thread_id
            producer_thread_id = get_ident()
            try:
                assert start_producer.wait(timeout=20)
                with create_session(scoped=False) as producer:
                    connection = producer.connection()
                    if engine.dialect.name == "postgresql":
                        connection.exec_driver_sql("SET LOCAL deadlock_timeout = '100ms'")
                        connection.exec_driver_sql("SET LOCAL lock_timeout = '15s'")
                    new_event = AssetManager.register_asset_change(
                        asset=asset, partition_key="partition-1", session=producer
                    )
                    return new_event.id
            finally:
                producer_finished.set()

        def before_execute(conn, cursor, statement, parameters, context, executemany):
            if get_ident() == producer_thread_id:
                return
            if deletion_failed.is_set():
                # Let the producer finish after rollback before the deletion retry takes any locks.
                assert producer_finished.wait(timeout=20)
            compiled = context.compiled
            if compiled is None:
                return
            query = compiled.statement
            if (
                query.is_select
                and query._for_update_arg is not None
                and "FROM asset_partition_dag_run" in statement
                and engine.dialect.name == "postgresql"
            ):
                conn.exec_driver_sql("SET LOCAL deadlock_timeout = '10s'")
                conn.exec_driver_sql("SET LOCAL lock_timeout = '15s'")

        def after_execute(conn, cursor, statement, parameters, context, executemany):
            nonlocal producer_lock_count, deletion_lock_count
            compiled = context.compiled
            if compiled is None:
                return
            query = compiled.statement
            if not query.is_select or query._for_update_arg is None:
                return
            if get_ident() == producer_thread_id:
                if "FROM asset_partition_dag_run" in statement:
                    producer_lock_count += 1
                    if producer_lock_count == 1:
                        producer_first_lock.set()
                        assert deletion_first_lock.wait(timeout=20)
            elif "FROM partitioned_asset_key_log" in statement and "JOIN asset_event" in statement:
                if not start_producer.is_set():
                    start_producer.set()
                    assert producer_first_lock.wait(timeout=20)
            elif "FROM asset_partition_dag_run" in statement:
                deletion_lock_count += 1
                deletion_first_lock.set()

        def record_error(context):
            is_producer = get_ident() == producer_thread_id
            error = context.original_exception
            error_code = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None) or error.args[0]
            errors.append((is_producer, error_code))
            if not is_producer:
                deletion_failed.set()

        event.listen(engine, "before_cursor_execute", before_execute)
        event.listen(engine, "after_cursor_execute", after_execute)
        event.listen(engine, "handle_error", record_error)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                producer_future = executor.submit(register_event)
                try:
                    response = test_client.delete(
                        "/assets/1/events",
                        params={
                            "before": (DEFAULT_DATE + timedelta(days=1)).isoformat(),
                            "delete_queued_events": "true",
                        },
                    )
                finally:
                    start_producer.set()
                    deletion_first_lock.set()
                new_event_id = producer_future.result(timeout=20)
        finally:
            event.remove(engine, "before_cursor_execute", before_execute)
            event.remove(engine, "after_cursor_execute", after_execute)
            event.remove(engine, "handle_error", record_error)

        assert response.status_code == 204
        assert producer_lock_count == 2
        assert deletion_failed.is_set()
        assert deletion_lock_count >= 3
        assert errors == [(False, "55P03" if engine.dialect.name == "postgresql" else 3572)]
        session.expire_all()
        assert session.scalars(select(AssetEvent.id)).all() == [new_event_id]
        assert set(session.scalars(select(AssetPartitionDagRun.id))) == {1, 2}
        assert set(
            session.execute(
                select(
                    PartitionedAssetKeyLog.asset_event_id, PartitionedAssetKeyLog.asset_partition_dag_run_id
                )
            )
        ) == {(new_event_id, 1), (new_event_id, 2)}
