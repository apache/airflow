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

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import delete, select

from airflow._shared.state import AssetScope, BaseStoreBackend, StateScope, TaskScope
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.models.asset_store import AssetStoreModel
from airflow.models.dagrun import DagRun
from airflow.models.task_store import TaskStoreModel
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


class MetastoreStateBackend(BaseStoreBackend):
    """Default state backend for tasks and assets. Stores task and asset state in the Airflow metadata database."""

    @provide_session
    def get(self, scope: StateScope, key: str, *, session: Session | None = NEW_SESSION) -> str | None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                return self._get_task_store(scope, key, session=session)
            case AssetScope():
                return self._get_asset_store(scope, key, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def set(
        self,
        scope: StateScope,
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
                self._set_task_store(scope, key, value, expires_at=expires_at, session=session)
            case AssetScope():
                self._set_asset_store(scope, key, value, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def delete(self, scope: StateScope, key: str, *, session: Session | None = NEW_SESSION) -> None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                self._delete_task_store(scope, key, session=session)
            case AssetScope():
                self._delete_asset_store(scope, key, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def clear(
        self,
        scope: StateScope,
        *,
        all_map_indices: bool = False,
        session: Session | None = NEW_SESSION,
    ) -> None:
        if TYPE_CHECKING:
            assert session is not None
        match scope:
            case TaskScope():
                self._clear_task_store(scope, all_map_indices=all_map_indices, session=session)
            case AssetScope():
                self._clear_asset_store(scope, session=session)
            case _:
                assert_never(scope)

    async def aget(self, scope: StateScope, key: str, *, session: AsyncSession | None = None) -> str | None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    return await self._aget_task_store(scope, key, session=s)
                case AssetScope():
                    return await self._aget_asset_store(scope, key, session=s)
                case _:
                    assert_never(scope)

    async def aset(
        self,
        scope: StateScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    await self._aset_task_store(scope, key, value, expires_at=expires_at, session=s)
                case AssetScope():
                    await self._aset_asset_store(scope, key, value, session=s)
                case _:
                    assert_never(scope)

    async def adelete(self, scope: StateScope, key: str, *, session: AsyncSession | None = None) -> None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    await self._adelete_task_store(scope, key, session=s)
                case AssetScope():
                    await self._adelete_asset_store(scope, key, session=s)
                case _:
                    assert_never(scope)

    async def aclear(
        self, scope: StateScope, *, all_map_indices: bool = False, session: AsyncSession | None = None
    ) -> None:
        async with _async_session(session) as s:
            match scope:
                case TaskScope():
                    await self._aclear_task_store(scope, all_map_indices=all_map_indices, session=s)
                case AssetScope():
                    await self._aclear_asset_store(scope, session=s)
                case _:
                    assert_never(scope)

    def _get_task_store(self, scope: TaskScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(TaskStoreModel).where(
                TaskStoreModel.dag_id == scope.dag_id,
                TaskStoreModel.run_id == scope.run_id,
                TaskStoreModel.task_id == scope.task_id,
                TaskStoreModel.map_index == scope.map_index,
                TaskStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _set_task_store(
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
            TaskStoreModel,
            ["dag_run_id", "task_id", "map_index", "key"],
            values,
            dict(value=value, updated_at=now, expires_at=expires_at),
        )
        session.execute(stmt)

    def _delete_task_store(self, scope: TaskScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(TaskStoreModel).where(
                TaskStoreModel.dag_id == scope.dag_id,
                TaskStoreModel.run_id == scope.run_id,
                TaskStoreModel.task_id == scope.task_id,
                TaskStoreModel.map_index == scope.map_index,
                TaskStoreModel.key == key,
            )
        )

    def _clear_task_store(self, scope: TaskScope, *, all_map_indices: bool = False, session: Session) -> None:
        conditions = [
            TaskStoreModel.dag_id == scope.dag_id,
            TaskStoreModel.run_id == scope.run_id,
            TaskStoreModel.task_id == scope.task_id,
        ]
        if not all_map_indices:
            conditions.append(TaskStoreModel.map_index == scope.map_index)
        session.execute(delete(TaskStoreModel).where(*conditions))

    def _get_asset_store(self, scope: AssetScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(AssetStoreModel).where(
                AssetStoreModel.asset_id == scope.asset_id,
                AssetStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _set_asset_store(self, scope: AssetScope, key: str, value: str, *, session: Session) -> None:
        now = timezone.utcnow()
        values = dict(asset_id=scope.asset_id, key=key, value=value, updated_at=now)
        stmt = _build_upsert_stmt(
            get_dialect_name(session),
            AssetStoreModel,
            ["asset_id", "key"],
            values,
            dict(value=value, updated_at=now),
        )
        session.execute(stmt)

    def _delete_asset_store(self, scope: AssetScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(AssetStoreModel).where(
                AssetStoreModel.asset_id == scope.asset_id,
                AssetStoreModel.key == key,
            )
        )

    def _clear_asset_store(self, scope: AssetScope, *, session: Session) -> None:
        session.execute(
            delete(AssetStoreModel).where(
                AssetStoreModel.asset_id == scope.asset_id,
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
                    id_query = select(TaskStoreModel.id).where(where_clause)
                    if batch_size > 0:
                        id_query = id_query.limit(batch_size)
                    ids = session.scalars(id_query).all()
                    if not ids:
                        break
                    session.execute(delete(TaskStoreModel).where(TaskStoreModel.id.in_(ids)))
                    session.commit()
                    total += len(ids)
                    if batch_size <= 0 or len(ids) < batch_size:
                        break
            return total

        deleted = _delete_batched(TaskStoreModel.expires_at < now)
        log.info("Deleted expired task_store rows", rows_deleted=deleted)

    def _summary_dry_run(self) -> dict[str, list]:
        """Return rows that would be deleted by cleanup() without deleting anything."""
        now = timezone.utcnow()
        cols = (
            TaskStoreModel.dag_id,
            TaskStoreModel.run_id,
            TaskStoreModel.task_id,
            TaskStoreModel.map_index,
            TaskStoreModel.key,
        )
        with create_session() as session:
            expired = session.execute(select(*cols).where(TaskStoreModel.expires_at < now)).all()
        return {"expired": list(expired)}

    async def _aget_task_store(self, scope: TaskScope, key: str, *, session: AsyncSession) -> str | None:
        row = await session.scalar(
            select(TaskStoreModel).where(
                TaskStoreModel.dag_id == scope.dag_id,
                TaskStoreModel.run_id == scope.run_id,
                TaskStoreModel.task_id == scope.task_id,
                TaskStoreModel.map_index == scope.map_index,
                TaskStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_task_store(
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
            TaskStoreModel,
            ["dag_run_id", "task_id", "map_index", "key"],
            values,
            dict(value=value, updated_at=now, expires_at=expires_at),
        )
        await session.execute(stmt)

    async def _adelete_task_store(self, scope: TaskScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(TaskStoreModel).where(
                TaskStoreModel.dag_id == scope.dag_id,
                TaskStoreModel.run_id == scope.run_id,
                TaskStoreModel.task_id == scope.task_id,
                TaskStoreModel.map_index == scope.map_index,
                TaskStoreModel.key == key,
            )
        )

    async def _aclear_task_store(
        self, scope: TaskScope, *, all_map_indices: bool = False, session: AsyncSession
    ) -> None:
        conditions = [
            TaskStoreModel.dag_id == scope.dag_id,
            TaskStoreModel.run_id == scope.run_id,
            TaskStoreModel.task_id == scope.task_id,
        ]
        if not all_map_indices:
            conditions.append(TaskStoreModel.map_index == scope.map_index)
        await session.execute(delete(TaskStoreModel).where(*conditions))

    async def _aget_asset_store(self, scope: AssetScope, key: str, *, session: AsyncSession) -> str | None:
        row = await session.scalar(
            select(AssetStoreModel).where(
                AssetStoreModel.asset_id == scope.asset_id,
                AssetStoreModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_asset_store(
        self, scope: AssetScope, key: str, value: str, *, session: AsyncSession
    ) -> None:
        now = timezone.utcnow()
        values = dict(asset_id=scope.asset_id, key=key, value=value, updated_at=now)
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        stmt = _build_upsert_stmt(
            get_dialect_name(session.sync_session),
            AssetStoreModel,
            ["asset_id", "key"],
            values,
            dict(value=value, updated_at=now),
        )
        await session.execute(stmt)

    async def _adelete_asset_store(self, scope: AssetScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStoreModel).where(
                AssetStoreModel.asset_id == scope.asset_id,
                AssetStoreModel.key == key,
            )
        )

    async def _aclear_asset_store(self, scope: AssetScope, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStoreModel).where(
                AssetStoreModel.asset_id == scope.asset_id,
            )
        )
