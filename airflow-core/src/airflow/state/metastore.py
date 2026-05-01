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
from airflow.typing_compat import assert_never
from airflow.utils.session import NEW_SESSION, create_session_async, provide_session
from airflow.utils.sqlalchemy import get_dialect_name

if TYPE_CHECKING:
    from sqlalchemy.dialects.mysql.dml import Insert as MySQLInsert
    from sqlalchemy.dialects.postgresql.dml import Insert as PostgreSQLInsert
    from sqlalchemy.dialects.sqlite.dml import Insert as SQLiteInsert
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session


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


class MetastoreStateBackend(BaseStateBackend):
    """Default state backend for tasks and assets. Stores task and asset state in the Airflow metadata database."""

    @provide_session
    def get(self, scope: StateScope, key: str, *, session: Session = NEW_SESSION) -> str | None:
        match scope:
            case TaskScope():
                return self._get_task_state(scope, key, session=session)
            case AssetScope():
                return self._get_asset_state(scope, key, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def set(self, scope: StateScope, key: str, value: str, *, session: Session = NEW_SESSION) -> None:
        match scope:
            case TaskScope():
                self._set_task_state(scope, key, value, session=session)
            case AssetScope():
                self._set_asset_state(scope, key, value, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def delete(self, scope: StateScope, key: str, *, session: Session = NEW_SESSION) -> None:
        match scope:
            case TaskScope():
                self._delete_task_state(scope, key, session=session)
            case AssetScope():
                self._delete_asset_state(scope, key, session=session)
            case _:
                assert_never(scope)

    @provide_session
    def clear(
        self,
        scope: StateScope,
        *,
        all_map_indices: bool = False,
        session: Session = NEW_SESSION,
    ) -> None:
        match scope:
            case TaskScope():
                self._clear_task_state(scope, all_map_indices=all_map_indices, session=session)
            case AssetScope():
                self._clear_asset_state(scope, session=session)
            case _:
                assert_never(scope)

    async def aget(self, scope: StateScope, key: str) -> str | None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    return await self._aget_task_state(scope, key, session=session)
                case AssetScope():
                    return await self._aget_asset_state(scope, key, session=session)
                case _:
                    assert_never(scope)

    async def aset(self, scope: StateScope, key: str, value: str) -> None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    await self._aset_task_state(scope, key, value, session=session)
                case AssetScope():
                    await self._aset_asset_state(scope, key, value, session=session)
                case _:
                    assert_never(scope)

    async def adelete(self, scope: StateScope, key: str) -> None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    await self._adelete_task_state(scope, key, session=session)
                case AssetScope():
                    await self._adelete_asset_state(scope, key, session=session)
                case _:
                    assert_never(scope)

    async def aclear(self, scope: StateScope, *, all_map_indices: bool = False) -> None:
        async with create_session_async() as session:
            match scope:
                case TaskScope():
                    await self._aclear_task_state(scope, all_map_indices=all_map_indices, session=session)
                case AssetScope():
                    await self._aclear_asset_state(scope, session=session)
                case _:
                    assert_never(scope)

    def _get_task_state(self, scope: TaskScope, key: str, *, session: Session) -> str | None:
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

    def _set_task_state(self, scope: TaskScope, key: str, value: str, *, session: Session) -> None:
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
        )
        stmt = _build_upsert_stmt(
            get_dialect_name(session),
            TaskStateModel,
            ["dag_run_id", "task_id", "map_index", "key"],
            values,
            dict(value=value, updated_at=now),
        )
        session.execute(stmt)

    def _delete_task_state(self, scope: TaskScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )

    def _clear_task_state(self, scope: TaskScope, *, all_map_indices: bool = False, session: Session) -> None:
        conditions = [
            TaskStateModel.dag_id == scope.dag_id,
            TaskStateModel.run_id == scope.run_id,
            TaskStateModel.task_id == scope.task_id,
        ]
        if not all_map_indices:
            conditions.append(TaskStateModel.map_index == scope.map_index)
        session.execute(delete(TaskStateModel).where(*conditions))

    def _get_asset_state(self, scope: AssetScope, key: str, *, session: Session) -> str | None:
        row = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _set_asset_state(self, scope: AssetScope, key: str, value: str, *, session: Session) -> None:
        now = timezone.utcnow()
        values = dict(asset_id=scope.asset_id, key=key, value=value, updated_at=now)
        stmt = _build_upsert_stmt(
            get_dialect_name(session),
            AssetStateModel,
            ["asset_id", "key"],
            values,
            dict(value=value, updated_at=now),
        )
        session.execute(stmt)

    def _delete_asset_state(self, scope: AssetScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )

    def _clear_asset_state(self, scope: AssetScope, *, session: Session) -> None:
        session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
            )
        )

    async def _aget_task_state(self, scope: TaskScope, key: str, *, session: AsyncSession) -> str | None:
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

    async def _aset_task_state(
        self, scope: TaskScope, key: str, value: str, *, session: AsyncSession
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
        )
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        stmt = _build_upsert_stmt(
            get_dialect_name(session.sync_session),
            TaskStateModel,
            ["dag_run_id", "task_id", "map_index", "key"],
            values,
            dict(value=value, updated_at=now),
        )
        await session.execute(stmt)

    async def _adelete_task_state(self, scope: TaskScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )

    async def _aclear_task_state(
        self, scope: TaskScope, *, all_map_indices: bool = False, session: AsyncSession
    ) -> None:
        conditions = [
            TaskStateModel.dag_id == scope.dag_id,
            TaskStateModel.run_id == scope.run_id,
            TaskStateModel.task_id == scope.task_id,
        ]
        if not all_map_indices:
            conditions.append(TaskStateModel.map_index == scope.map_index)
        await session.execute(delete(TaskStateModel).where(*conditions))

    async def _aget_asset_state(self, scope: AssetScope, key: str, *, session: AsyncSession) -> str | None:
        row = await session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    async def _aset_asset_state(
        self, scope: AssetScope, key: str, value: str, *, session: AsyncSession
    ) -> None:
        now = timezone.utcnow()
        values = dict(asset_id=scope.asset_id, key=key, value=value, updated_at=now)
        # get_dialect_name expects a sync Session; sync_session is the underlying Session the async wrapper delegates to
        stmt = _build_upsert_stmt(
            get_dialect_name(session.sync_session),
            AssetStateModel,
            ["asset_id", "key"],
            values,
            dict(value=value, updated_at=now),
        )
        await session.execute(stmt)

    async def _adelete_asset_state(self, scope: AssetScope, key: str, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
                AssetStateModel.key == key,
            )
        )

    async def _aclear_asset_state(self, scope: AssetScope, *, session: AsyncSession) -> None:
        await session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == scope.asset_id,
            )
        )
