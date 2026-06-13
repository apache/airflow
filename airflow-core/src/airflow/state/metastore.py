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

import functools
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import delete, select

from airflow._shared.state import (
    AssetScope,
    AssetStateStoreWriterKind,
    BaseStoreBackend,
    StoreScope,
    TaskScope,
)
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.models.asset_state_store import AssetStateStoreModel
from airflow.models.dagrun import DagRun
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.typing_compat import assert_never
from airflow.utils.session import NEW_SESSION, create_session, create_session_async, provide_session
from airflow.utils.sqlalchemy import get_dialect_name

if TYPE_CHECKING:
    from sqlalchemy.dialects.mysql.dml import Insert as MySQLInsert
    from sqlalchemy.dialects.postgresql.dml import Insert as PostgreSQLInsert
    from sqlalchemy.dialects.sqlite.dml import Insert as SQLiteInsert
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session


log = structlog.get_logger(__name__)


@asynccontextmanager
async def _async_session(session: AsyncSession | None) -> AsyncGenerator[AsyncSession, None]:
    """Use provided async session or create a new one."""
    if session is not None:
        yield session
    else:
        async with create_session_async() as s:
            yield s


def _build_upsert_stmt(
    dialect: str | None,
    model: type,
    conflict_cols: list[str],
    values: dict,
    update_fields: dict,
):
    """Return a dialect-specific INSERT ... ON CONFLICT UPDATE statement."""
    stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(model).values(**values)
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_fields)
    elif dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(model).values(**values)
        stmt = stmt.on_duplicate_key_update(**update_fields)
    else:
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(model).values(**values)
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_fields)
    return stmt


def _build_asset_writer_fields(
    kind: AssetStateStoreWriterKind | None,
    dag_id: str | None,
    run_id: str | None,
    task_id: str | None,
    map_index: int | None,
    *,
    value: str,
    now: datetime,
) -> tuple[dict, dict]:
    kind_str = kind.value if kind is not None else None
    writer_info = dict(
        last_updated_by_kind=kind_str,
        last_updated_by_dag_id=dag_id,
        last_updated_by_run_id=run_id,
        last_updated_by_task_id=task_id,
        last_updated_by_map_index=map_index,
    )
    update_fields = dict(value=value, updated_at=now, **(writer_info if kind is not None else {}))
    return writer_info, update_fields


class MetastoreBackend(BaseStoreBackend):
    """Default state backend for tasks and assets. Stores task and asset state in the Airflow metadata database."""

    @provide_session
    def get(self, scope: StoreScope, key: str, *, session: Session | None = NEW_SESSION) -> str | None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                return self._get_task_state_store(scope, key, session=session)
            case AssetScope():
                return self._get_asset_state_store(scope, key, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def set(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: Session | None = NEW_SESSION,
    ) -> None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                self._store_task_state(scope, key, value, expires_at=expires_at, session=session)
            case AssetScope():
                self._store_asset_state(scope, key, value, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def delete(self, scope: StoreScope, key: str, *, session: Session | None = NEW_SESSION) -> None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                self._delete_task_state_store(scope, key, session=session)
            case AssetScope():
                self._delete_asset_state_store(scope, key, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def clear(
        self,
        scope: StoreScope,
        *,
        all_map_indices: bool = False,
        session: Session | None = NEW_SESSION,
    ) -> None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                self._clear_task_state_store(scope, all_map_indices=all_map_indices, session=session)
            case AssetScope():
                self._clear_asset_state_store(scope, session=session)
            case _:
                assert_never(scope)

    async def aget(self, scope: StoreScope, key: str, *, session: AsyncSession | None = None) -> str | None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    return await self._aget_task_state_store(scope, key, session=s)
                case AssetScope():
                    return await self._aget_asset_state_store(scope, key, session=s)
                case _:
                    assert_never(scope)

    async def aset(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    await self._aset_task_state_store(scope, key, value, expires_at=expires_at, session=s)
                case AssetScope():
                    await self._aset_asset_state_store(scope, key, value, session=s)
                case _:
                    assert_never(scope)

    async def adelete(self, scope: StoreScope, key: str, *, session: AsyncSession | None = None) -> None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    await self._adelete_task_state_store(scope, key, session=s)
                case AssetScope():
                    await self._adelete_asset_state_store(scope, key, session=s)
                case _:
                    assert_never(scope)

    async def aclear(
        self, scope: StoreScope, *, all_map_indices: bool = False, session: AsyncSession | None = None
    ) -> None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    await self._aclear_task_state_store(scope, all_map_indices=all_map_indices, session=s)
                case AssetScope():
                    await self._aclear_asset_state_store(scope, session=s)
                case _:
                    assert_never(scope)

    def _get_task_state_store(self, scope: TaskScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == scope.dag_id,
                TaskStateStoreModel.run_id == scope.run_id,
                TaskStateStoreModel.task_id == scope.task_id,
                TaskStateStoreModel.map_index == scope.map_index,
                TaskStateStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _store_task_state(
        self,
        scope: TaskScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: Session,
    ) -> None:
        dag_run_id = session.scalar(
            select(DagRun.id).where(
                DagRun.dag_id == scope.dag_id,
                DagRun.run_id == scope.run_id,
            )
        )
        if dag_run_id is None:
            raise ValueError(f"No DagRun found for dag_id={scope.dag_id!r} run_id={scope.run_id!r}")
        now = timezone.utcnow()
        values = dict(
            dag_run_id=dag_run_id,
            dag_id=scope.dag_id,
            run_id=scope.run_id,
            task_id=scope.task_id,
            map_index=scope.map_index,
            key=key,
            value=value,
            updated_at=now,
            expires_at=expires_at,
        )
        stmt = _build_upsert_stmt(
            get_dialect_name(session),
            TaskStateStoreModel,
            ["dag_run_id", "task_id", "map_index", "key"],
            values,
            dict(value=value, updated_at=now, expires_at=expires_at),
        )
        session.execute(stmt)

    def _delete_task_state_store(self, scope: TaskScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == scope.dag_id,
                TaskStateStoreModel.run_id == scope.run_id,
                TaskStateStoreModel.task_id == scope.task_id,
                TaskStateStoreModel.map_index == scope.map_index,
                TaskStateStoreModel.key == key,
            )
        )

    def _clear_task_state_store(
        self, scope: TaskScope, *, all_map_indices: bool = False, session: Session
    ) -> None:
        conditions = [
            TaskStateStoreModel.dag_id == scope.dag_id,
            TaskStateStoreModel.run_id == scope.run_id,
            TaskStateStoreModel.task_id == scope.task_id,
        ]
        if not all_map_indices:
            conditions.append(TaskStateStoreModel.map_index == scope.map_index)
        session.execute(delete(TaskStateStoreModel).where(*conditions))

    def _get_asset_state_store(self, scope: AssetScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == scope.asset_id,
                AssetStateStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _store_asset_state(
        self,
        scope: AssetScope,
        key: str,
        value: str,
        *,
        kind: AssetStateStoreWriterKind | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        session: Session,
    ) -> None:
        now = timezone.utcnow()
        writer_info, update_fields = _build_asset_writer_fields(
            kind, dag_id, run_id, task_id, map_index, value=value, now=now
        )
        values = dict(asset_id=scope.asset_id, key=key, value=value, updated_at=now, **writer_info)
        stmt = _build_upsert_stmt(
            get_dialect_name(session),
            AssetStateStoreModel,
            ["asset_id", "key"],
            values,
            update_fields,
        )
        session.execute(stmt)

    @provide_session
    def set_asset_state_store(
        self,
        scope: AssetScope,
        key: str,
        value: str,
        *,
        kind: AssetStateStoreWriterKind,
        dag_id: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        session: Session | None = NEW_SESSION,
    ) -> None:
        """Write an asset state store entry, recording who made the write."""
        kind.validate_writer_fields(dag_id, run_id, task_id, map_index)
        if TYPE_CHECKING:
            assert session is not None
        self._store_asset_state(
            scope,
            key,
            value,
            kind=kind,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            session=session,
        )

    def _delete_asset_state_store(self, scope: AssetScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == scope.asset_id,
                AssetStateStoreModel.key == key,
            )
        )

    def _clear_asset_state_store(self, scope: AssetScope, *, session: Session) -> None:
        session.execute(
            delete(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == scope.asset_id,
            )
        )

    def cleanup(self) -> None:
        """
        Remove expired task state rows.

        ``expires_at`` is set at write time on every ``set()`` call, so cleanup is a single
        ``WHERE expires_at < now()`` pass. Rows with ``expires_at=NULL`` (default_retention_days=0)
        are never deleted. Batching is configurable via ``[state_store] state_cleanup_batch_size``.
        """
        batch_size = conf.getint("state_store", "state_cleanup_batch_size")
        now = timezone.utcnow()

        def _delete_batched(where_clause) -> int:
            total = 0
            with create_session() as session:
                while True:
                    id_query = select(TaskStateStoreModel.id).where(where_clause)
                    if batch_size > 0:
                        id_query = id_query.limit(batch_size)
                    ids = session.scalars(id_query).all()
                    if not ids:
                        break
                    session.execute(delete(TaskStateStoreModel).where(TaskStateStoreModel.id.in_(ids)))
                    session.commit()
                    total += len(ids)
                    if batch_size <= 0 or len(ids) < batch_size:
                        break
            return total

        deleted = _delete_batched(TaskStateStoreModel.expires_at < now)
        log.info("Deleted expired task_state_store rows", rows_deleted=deleted)

    def _summary_dry_run(self) -> dict[str, list]:
        """Return rows that would be deleted by cleanup() without deleting anything."""
        now = timezone.utcnow()
        cols = (
            TaskStateStoreModel.dag_id,
            TaskStateStoreModel.run_id,
            TaskStateStoreModel.task_id,
            TaskStateStoreModel.map_index,
            TaskStateStoreModel.key,
        )
        with create_session() as session:
            expired = session.execute(select(*cols).where(TaskStateStoreModel.expires_at < now)).all()
        return {"expired": list(expired)}

    async def _aget_task_state_store(
        self, scope: TaskScope, key: str, *, session: AsyncSession
    ) -> str | None:
        row = await session.scalar(
            select(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == scope.dag_id,
                TaskStateStoreModel.run_id == scope.run_id,
                TaskStateStoreModel.task_id == scope.task_id,
                TaskStateStoreModel.map_index == scope.map_index,
                TaskStateStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_task_state_store(
        self,
        scope: TaskScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: AsyncSession,
    ) -> None:
        dag_run_id = await session.scalar(
            select(DagRun.id).where(
                DagRun.dag_id == scope.dag_id,
                DagRun.run_id == scope.run_id,
            )
        )
        if dag_run_id is None:
            raise ValueError(f"No DagRun found for dag_id={scope.dag_id!r} run_id={scope.run_id!r}")
        now = timezone.utcnow()
        values = dict(
            dag_run_id=dag_run_id,
            dag_id=scope.dag_id,
            run_id=scope.run_id,
            task_id=scope.task_id,
            map_index=scope.map_index,
            key=key,
            value=value,
            updated_at=now,
            expires_at=expires_at,
        )
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        stmt = _build_upsert_stmt(
            get_dialect_name(session.sync_session),
            TaskStateStoreModel,
            ["dag_run_id", "task_id", "map_index", "key"],
            values,
            dict(value=value, updated_at=now, expires_at=expires_at),
        )
        await session.execute(stmt)

    async def _adelete_task_state_store(self, scope: TaskScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == scope.dag_id,
                TaskStateStoreModel.run_id == scope.run_id,
                TaskStateStoreModel.task_id == scope.task_id,
                TaskStateStoreModel.map_index == scope.map_index,
                TaskStateStoreModel.key == key,
            )
        )

    async def _aclear_task_state_store(
        self, scope: TaskScope, *, all_map_indices: bool = False, session: AsyncSession
    ) -> None:
        conditions = [
            TaskStateStoreModel.dag_id == scope.dag_id,
            TaskStateStoreModel.run_id == scope.run_id,
            TaskStateStoreModel.task_id == scope.task_id,
        ]
        if not all_map_indices:
            conditions.append(TaskStateStoreModel.map_index == scope.map_index)
        await session.execute(delete(TaskStateStoreModel).where(*conditions))

    async def _aget_asset_state_store(
        self, scope: AssetScope, key: str, *, session: AsyncSession
    ) -> str | None:
        row = await session.scalar(
            select(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == scope.asset_id,
                AssetStateStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_asset_state_store(
        self,
        scope: AssetScope,
        key: str,
        value: str,
        *,
        kind: AssetStateStoreWriterKind | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        session: AsyncSession,
    ) -> None:
        now = timezone.utcnow()
        writer_info, update_fields = _build_asset_writer_fields(
            kind, dag_id, run_id, task_id, map_index, value=value, now=now
        )
        values = dict(asset_id=scope.asset_id, key=key, value=value, updated_at=now, **writer_info)
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        stmt = _build_upsert_stmt(
            get_dialect_name(session.sync_session),
            AssetStateStoreModel,
            ["asset_id", "key"],
            values,
            update_fields,
        )
        await session.execute(stmt)

    async def aset_asset_state_store(
        self,
        scope: AssetScope,
        key: str,
        value: str,
        *,
        kind: AssetStateStoreWriterKind,
        dag_id: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        """Write an asset state store entry, recording who made the write."""
        kind.validate_writer_fields(dag_id, run_id, task_id, map_index)
        async with _async_session(session) as s:
            await self._aset_asset_state_store(
                scope,
                key,
                value,
                kind=kind,
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_index=map_index,
                session=s,
            )

    async def _adelete_asset_state_store(self, scope: AssetScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == scope.asset_id,
                AssetStateStoreModel.key == key,
            )
        )

    async def _aclear_asset_state_store(self, scope: AssetScope, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == scope.asset_id,
            )
        )


@functools.cache
def _get_db_backend() -> MetastoreBackend:
    """Return a cached MetastoreBackend instance for DB-direct access."""
    return MetastoreBackend()
