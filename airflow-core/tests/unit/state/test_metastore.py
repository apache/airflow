#
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

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.asset import AssetModel
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.task_state import TaskStateModel
from airflow.state import AssetScope, TaskScope, resolve_state_backend
from airflow.state.metastore import MetastoreStateBackend
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_runs

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "scheduled__2026-04-24"


@pytest.fixture(autouse=True)
def clean_tables():
    clear_db_dags()
    clear_db_runs()
    clear_db_assets()
    yield
    clear_db_dags()
    clear_db_runs()
    clear_db_assets()


@pytest.fixture
def backend() -> MetastoreStateBackend:
    return MetastoreStateBackend()


@pytest.fixture
def dag_run(session: Session) -> DagRun:
    run = DagRun(
        dag_id=DAG_ID,
        run_id=RUN_ID,
        run_type=DagRunType.SCHEDULED,
        logical_date=timezone.datetime(2026, 4, 24),
        run_after=timezone.datetime(2026, 4, 24),
    )
    session.add(run)
    session.flush()
    return run


@pytest.fixture
def dag_run_committed() -> DagRun:
    """DagRun committed to DB so async sessions (which open their own connections) can see it."""
    with create_session() as session:
        run = DagRun(
            dag_id=DAG_ID,
            run_id=RUN_ID,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.datetime(2026, 4, 24),
            run_after=timezone.datetime(2026, 4, 24),
        )
        session.add(run)
        session.flush()
        session.expunge(run)
    return run


@pytest.fixture
def asset(session: Session) -> AssetModel:
    a = AssetModel(uri="s3://bucket/prefix", name="test_asset", group="test")
    session.add(a)
    session.flush()
    return a


@pytest.fixture
def asset_committed() -> AssetModel:
    """AssetModel committed to DB so async sessions (which open their own connections) can see it."""
    with create_session() as session:
        a = AssetModel(uri="s3://bucket/prefix", name="test_asset", group="test")
        session.add(a)
        session.flush()
        session.expunge(a)
    return a


class TestMetastoreStateBackendTaskScope:
    def test_get_returns_none_for_missing_key(
        self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun
    ):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        assert backend.get(scope, "missing", session=session) is None

    def test_set_and_get_roundtrip(self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        backend.set(scope, "job_id", "app_1234", session=session)
        session.flush()
        assert backend.get(scope, "job_id", session=session) == "app_1234"

    @pytest.mark.backend("postgres", "mysql", "sqlite")
    def test_set_twice_overrides_existing_value(
        self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun
    ):
        """Calling set twice on the same key updates the value in place."""
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        backend.set(scope, "job_id", "app_try1", session=session)
        session.flush()
        backend.set(scope, "job_id", "app_try2", session=session)
        session.flush()
        assert backend.get(scope, "job_id", session=session) == "app_try2"

    def test_set_stores_dag_run_id_fk(
        self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun
    ):
        """set resolves dag_run_id from (dag_id, run_id) and persists it as the FK."""
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        backend.set(scope, "job_id", "app_1234", session=session)
        session.flush()

        row = session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_id == DAG_ID,
                TaskStateModel.task_id == TASK_ID,
                TaskStateModel.key == "job_id",
            )
        )
        assert row is not None
        assert row.dag_run_id == dag_run.id

    def test_delete_removes_key(self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        backend.set(scope, "job_id", "app_1234", session=session)
        backend.set(scope, "checkpoint", "step_3", session=session)
        session.flush()

        backend.delete(scope, "job_id", session=session)
        session.flush()

        assert backend.get(scope, "job_id", session=session) is None
        assert backend.get(scope, "checkpoint", session=session) == "step_3"

    def test_delete_is_noop_for_missing_key(
        self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun
    ):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        backend.delete(scope, "nonexistent", session=session)

    def test_clear_removes_all_keys(self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun):
        """clear removes every key under the given task scope."""
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        for key in ("job_id", "checkpoint", "watermark"):
            backend.set(scope, key, f"val_{key}", session=session)
        session.flush()

        backend.clear(scope, session=session)
        session.flush()

        for key in ("job_id", "checkpoint", "watermark"):
            assert backend.get(scope, key, session=session) is None

    def test_map_index_isolation(self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun):
        """Mapped task instances with different map_index values have isolated namespaces."""
        scope0 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=0)
        scope1 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=1)

        backend.set(scope0, "job_id", "app_0", session=session)
        backend.set(scope1, "job_id", "app_1", session=session)
        session.flush()

        assert backend.get(scope0, "job_id", session=session) == "app_0"
        assert backend.get(scope1, "job_id", session=session) == "app_1"

    def test_set_raises_for_missing_dag_run(self, session: Session, backend: MetastoreStateBackend):
        scope = TaskScope(dag_id="nonexistent_dag", run_id="nonexistent_run", task_id=TASK_ID)
        with pytest.raises(ValueError, match="No DagRun found"):
            backend.set(scope, "job_id", "app_1234", session=session)

    def test_clear_scoped_to_map_index(
        self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun
    ):
        """clear for map_index=0 does not remove state belonging to map_index=1."""
        scope0 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=0)
        scope1 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=1)

        backend.set(scope0, "job_id", "app_0", session=session)
        backend.set(scope1, "job_id", "app_1", session=session)
        session.flush()

        backend.clear(scope0, session=session)
        session.flush()

        assert backend.get(scope0, "job_id", session=session) is None
        assert backend.get(scope1, "job_id", session=session) == "app_1"

    def test_clear_with_all_map_indices_flag_wipes_wide(
        self, session: Session, backend: MetastoreStateBackend, dag_run: DagRun
    ):
        """clear(scope, all_map_indices=True) removes state for every map index of the task."""
        scope0 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=0)
        scope1 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=1)

        backend.set(scope0, "job_id", "app_0", session=session)
        backend.set(scope1, "job_id", "app_1", session=session)
        session.flush()

        backend.clear(scope0, all_map_indices=True, session=session)
        session.flush()

        assert backend.get(scope0, "job_id", session=session) is None
        assert backend.get(scope1, "job_id", session=session) is None


class TestMetastoreStateBackendAssetScope:
    def test_get_returns_none_for_missing_key(
        self, session: Session, backend: MetastoreStateBackend, asset: AssetModel
    ):
        scope = AssetScope(asset_id=asset.id)
        assert backend.get(scope, "missing", session=session) is None

    def test_set_and_get_roundtrip(self, session: Session, backend: MetastoreStateBackend, asset: AssetModel):
        scope = AssetScope(asset_id=asset.id)
        backend.set(scope, "watermark", "2026-04-24T00:00:00Z", session=session)
        session.flush()
        assert backend.get(scope, "watermark", session=session) == "2026-04-24T00:00:00Z"

    @pytest.mark.backend("postgres", "mysql", "sqlite")
    def test_set_twice_overwrites_existing_value(
        self, session: Session, backend: MetastoreStateBackend, asset: AssetModel
    ):
        scope = AssetScope(asset_id=asset.id)
        backend.set(scope, "watermark", "2026-04-01T00:00:00Z", session=session)
        session.flush()
        backend.set(scope, "watermark", "2026-04-24T00:00:00Z", session=session)
        session.flush()
        assert backend.get(scope, "watermark", session=session) == "2026-04-24T00:00:00Z"

    def test_delete_removes_key(self, session: Session, backend: MetastoreStateBackend, asset: AssetModel):
        scope = AssetScope(asset_id=asset.id)
        backend.set(scope, "watermark", "2026-04-24T00:00:00Z", session=session)
        backend.set(scope, "file_count", "42", session=session)
        session.flush()

        backend.delete(scope, "watermark", session=session)
        session.flush()

        assert backend.get(scope, "watermark", session=session) is None
        assert backend.get(scope, "file_count", session=session) == "42"

    def test_delete_is_noop_for_missing_key(
        self, session: Session, backend: MetastoreStateBackend, asset: AssetModel
    ):
        scope = AssetScope(asset_id=asset.id)
        backend.delete(scope, "nonexistent", session=session)

    def test_clear_removes_all_keys(
        self, session: Session, backend: MetastoreStateBackend, asset: AssetModel
    ):
        scope = AssetScope(asset_id=asset.id)
        for key in ("watermark", "file_count", "last_error"):
            backend.set(scope, key, f"val_{key}", session=session)
        session.flush()

        backend.clear(scope, session=session)
        session.flush()

        for key in ("watermark", "file_count", "last_error"):
            assert backend.get(scope, key, session=session) is None

    def test_different_assets_are_isolated(
        self, session: Session, backend: MetastoreStateBackend, asset: AssetModel
    ):
        asset2 = AssetModel(uri="s3://bucket/other", name="other_asset", group="test")
        session.add(asset2)
        session.flush()

        scope1 = AssetScope(asset_id=asset.id)
        scope2 = AssetScope(asset_id=asset2.id)

        backend.set(scope1, "watermark", "asset1_value", session=session)
        session.flush()

        assert backend.get(scope2, "watermark", session=session) is None


@pytest.mark.asyncio(loop_scope="class")
class TestMetastoreStateBackendAsync:
    async def test_aset_and_aget_task_roundtrip(
        self, backend: MetastoreStateBackend, dag_run_committed: DagRun
    ):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        await backend.aset(scope, "job_id", "app_async")
        result = await backend.aget(scope, "job_id")
        assert result == "app_async"

    async def test_adelete_task_removes_key(self, backend: MetastoreStateBackend, dag_run_committed: DagRun):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        await backend.aset(scope, "job_id", "app_async")
        await backend.adelete(scope, "job_id")
        assert await backend.aget(scope, "job_id") is None

    async def test_aclear_task_removes_all_keys(
        self, backend: MetastoreStateBackend, dag_run_committed: DagRun
    ):
        scope = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID)
        await backend.aset(scope, "job_id", "app_async")
        await backend.aset(scope, "checkpoint", "step_1")
        await backend.aclear(scope)
        assert await backend.aget(scope, "job_id") is None
        assert await backend.aget(scope, "checkpoint") is None

    async def test_aclear_with_all_map_indices_flag_wipes_fleet(
        self, backend: MetastoreStateBackend, dag_run_committed: DagRun
    ):
        """aclear(scope, all_map_indices=True) removes state for every map index of the task."""
        scope0 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=0)
        scope1 = TaskScope(dag_id=DAG_ID, run_id=RUN_ID, task_id=TASK_ID, map_index=1)

        await backend.aset(scope0, "job_id", "app_0")
        await backend.aset(scope1, "job_id", "app_1")

        await backend.aclear(scope0, all_map_indices=True)

        assert await backend.aget(scope0, "job_id") is None
        assert await backend.aget(scope1, "job_id") is None

    async def test_aset_and_aget_asset_roundtrip(
        self, backend: MetastoreStateBackend, asset_committed: AssetModel
    ):
        scope = AssetScope(asset_id=asset_committed.id)
        await backend.aset(scope, "watermark", "2026-04-27T00:00:00Z")
        result = await backend.aget(scope, "watermark")
        assert result == "2026-04-27T00:00:00Z"

    async def test_adelete_asset_removes_key(
        self, backend: MetastoreStateBackend, asset_committed: AssetModel
    ):
        scope = AssetScope(asset_id=asset_committed.id)
        await backend.aset(scope, "watermark", "2026-04-27T00:00:00Z")
        await backend.adelete(scope, "watermark")
        assert await backend.aget(scope, "watermark") is None

    async def test_aclear_asset_removes_all_keys(
        self, backend: MetastoreStateBackend, asset_committed: AssetModel
    ):
        scope = AssetScope(asset_id=asset_committed.id)
        await backend.aset(scope, "watermark", "2026-04-27T00:00:00Z")
        await backend.aset(scope, "file_count", "42")
        await backend.aclear(scope)
        assert await backend.aget(scope, "watermark") is None
        assert await backend.aget(scope, "file_count") is None

    async def test_aset_task_raises_for_missing_dag_run(self, backend: MetastoreStateBackend):
        scope = TaskScope(dag_id="nonexistent_dag", run_id="nonexistent_run", task_id=TASK_ID)
        with pytest.raises(ValueError, match="No DagRun found"):
            await backend.aset(scope, "job_id", "app_async")


class TestResolveStateBackend:
    @conf_vars({("state_store", "backend"): "airflow.state.metastore.MetastoreStateBackend"})
    def test_resolve_returns_configured_backend(self):
        """resolve_state_backend() imports and returns the explicitly configured backend class."""
        assert resolve_state_backend() is MetastoreStateBackend

    @conf_vars({("state_store", "backend"): ""})
    def test_empty_backend_raises_value_error(self):
        """resolve_state_backend() raises ValueError when backend is explicitly set to empty string."""
        with pytest.raises(ValueError, match="state_store.backend is not configured"):
            resolve_state_backend()

    @conf_vars({("state_store", "backend"): "airflow.models.dagrun.DagRun"})
    def test_invalid_backend_raises_type_error(self):
        """resolve_state_backend() raises TypeError when the configured class is not a BaseStateBackend subclass."""
        with pytest.raises(TypeError, match="not a subclass of `BaseStateBackend`"):
            resolve_state_backend()
