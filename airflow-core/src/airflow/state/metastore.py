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

from sqlalchemy import delete, select

from airflow._shared.state import AssetScope, BaseStateBackend, StateScope, TaskScope
from airflow._shared.timezones import timezone
from airflow.models.asset_state import AssetStateModel
from airflow.models.dagrun import DagRun
from airflow.models.task_state import TaskStateModel
from airflow.utils.session import NEW_SESSION, create_session_async, provide_session
from airflow.utils.sqlalchemy import get_dialect_name

if TYPE_CHECKING:
    from sqlalchemy.dialects.mysql.dml import Insert as MySQLInsert
    from sqlalchemy.dialects.postgresql.dml import Insert as PostgreSQLInsert
    from sqlalchemy.dialects.sqlite.dml import Insert as SQLiteInsert
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session


class MetastoreStateBackend(BaseStateBackend):
    """Default state backend for tasks and assets. Stores task and asset state in the Airflow metadata database."""

    @provide_session
    def get(self, scope: StateScope, key: str, *, session: Session = NEW_SESSION) -> str | None:
        match scope:
            case TaskScope():
                return self._get_task(scope, key, session=session)
            case AssetScope():
                return self._get_asset(scope, key, session=session)

    @provide_session
    def set(self, scope: StateScope, key: str, value: str, *, session: Session = NEW_SESSION) -> None:
        match scope:
            case TaskScope():
                self._set_task(scope, key, value, session=session)
            case AssetScope():
                self._set_asset(scope, key, value, session=session)

    @provide_session
    def delete(self, scope: StateScope, key: str, *, session: Session = NEW_SESSION) -> None:
        match scope:
            case TaskScope():
                self._delete_task(scope, key, session=session)
            case AssetScope():
                self._delete_asset(scope, key, session=session)

    @provide_session
    def clear(self, scope: StateScope, *, session: Session = NEW_SESSION) -> None:
        match scope:
            case TaskScope():
                self._clear_task(scope, session=session)
            case AssetScope():
                self._clear_asset(scope, session=session)

    async def aget(self, scope: StateScope, key: str) -> str | None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    return await self._aget_task(scope, key, session=session)
                case AssetScope():
                    return await self._aget_asset(scope, key, session=session)

    async def aset(self, scope: StateScope, key: str, value: str) -> None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    await self._aset_task(scope, key, value, session=session)
                case AssetScope():
                    await self._aset_asset(scope, key, value, session=session)

    async def adelete(self, scope: StateScope, key: str) -> None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    await self._adelete_task(scope, key, session=session)
                case AssetScope():
                    await self._adelete_asset(scope, key, session=session)

    async def aclear(self, scope: StateScope) -> None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    await self._aclear_task(scope, session=session)
                case AssetScope():
                    await self._aclear_asset(scope, session=session)

    def _get_task(self, scope: TaskScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _set_task(self, scope: TaskScope, key: str, value: str, *, session: Session) -> None:
        dag_run_id = session.scalar(
            select(DagRun.id).where(
                DagRun.dag_id == scope.dag_id,
                DagRun.run_id == scope.run_id,
            )
        )
        now = timezone.utcnow()
        dialect = get_dialect_name(session)
        stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(TaskStateModel).values(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
                value=value,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["dag_run_id", "task_id", "map_index", "key"],
                set_=dict(value=value, updated_at=now),
            )
        elif dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(TaskStateModel).values(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
                value=value,
                updated_at=now,
            )
            stmt = stmt.on_duplicate_key_update(value=value, updated_at=now)
        else:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(TaskStateModel).values(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
                value=value,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["dag_run_id", "task_id", "map_index", "key"],
                set_=dict(value=value, updated_at=now),
            )
        session.execute(stmt)

    def _delete_task(self, scope: TaskScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )

    def _clear_task(self, scope: TaskScope, *, session: Session) -> None:
        session.execute(
            delete(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
            )
        )

    def _get_asset(self, scope: AssetScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _set_asset(self, scope: AssetScope, key: str, value: str, *, session: Session) -> None:
        now = timezone.utcnow()
        dialect = get_dialect_name(session)
        stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(AssetStateModel).values(
                asset_id=scope.asset_id, key=key, value=value, updated_at=now
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["asset_id", "key"],
                set_=dict(value=value, updated_at=now),
            )
        elif dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(AssetStateModel).values(
                asset_id=scope.asset_id, key=key, value=value, updated_at=now
            )
            stmt = stmt.on_duplicate_key_update(value=value, updated_at=now)
        else:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(AssetStateModel).values(
                asset_id=scope.asset_id, key=key, value=value, updated_at=now
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["asset_id", "key"],
                set_=dict(value=value, updated_at=now),
            )
        session.execute(stmt)

    def _delete_asset(self, scope: AssetScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )

    def _clear_asset(self, scope: AssetScope, *, session: Session) -> None:
        session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
            )
        )

    async def _aget_task(self, scope: TaskScope, key: str, *, session: AsyncSession) -> str | None:
        row = await session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_task(self, scope: TaskScope, key: str, value: str, *, session: AsyncSession) -> None:
        dag_run_id = await session.scalar(
            select(DagRun.id).where(
                DagRun.dag_id == scope.dag_id,
                DagRun.run_id == scope.run_id,
            )
        )
        now = timezone.utcnow()
        stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        dialect = get_dialect_name(session.sync_session)
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(TaskStateModel).values(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
                value=value,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["dag_run_id", "task_id", "map_index", "key"],
                set_=dict(value=value, updated_at=now),
            )
        elif dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(TaskStateModel).values(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
                value=value,
                updated_at=now,
            )
            stmt = stmt.on_duplicate_key_update(value=value, updated_at=now)
        else:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(TaskStateModel).values(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
                value=value,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["dag_run_id", "task_id", "map_index", "key"],
                set_=dict(value=value, updated_at=now),
            )
        await session.execute(stmt)

    async def _adelete_task(self, scope: TaskScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )

    async def _aclear_task(self, scope: TaskScope, *, session: AsyncSession) -> None:
        await session.execute(
            delete(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
            )
        )

    async def _aget_asset(self, scope: AssetScope, key: str, *, session: AsyncSession) -> str | None:
        row = await session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_asset(self, scope: AssetScope, key: str, value: str, *, session: AsyncSession) -> None:
        now = timezone.utcnow()
        stmt: MySQLInsert | PostgreSQLInsert | SQLiteInsert
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        dialect = get_dialect_name(session.sync_session)
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(AssetStateModel).values(
                asset_id=scope.asset_id, key=key, value=value, updated_at=now
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["asset_id", "key"],
                set_=dict(value=value, updated_at=now),
            )
        elif dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(AssetStateModel).values(
                asset_id=scope.asset_id, key=key, value=value, updated_at=now
            )
            stmt = stmt.on_duplicate_key_update(value=value, updated_at=now)
        else:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(AssetStateModel).values(
                asset_id=scope.asset_id, key=key, value=value, updated_at=now
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["asset_id", "key"],
                set_=dict(value=value, updated_at=now),
            )
        await session.execute(stmt)

    async def _adelete_asset(self, scope: AssetScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )

    async def _aclear_asset(self, scope: AssetScope, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
            )
        )
