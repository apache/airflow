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
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class MetastoreStateBackend(BaseStateBackend):
    """Default state backend for tasks and assets. Stores task and asset state in the Airflow metadata database."""

    @provide_session
    def get(self, scope: StateScope, key: str, *, session: Session = NEW_SESSION) -> str | None:
        if isinstance(scope, TaskScope):
            return self._get_task(scope, key, session=session)
        return self._get_asset(scope, key, session=session)

    @provide_session
    def set(self, scope: StateScope, key: str, value: str, *, session: Session = NEW_SESSION) -> None:
        if isinstance(scope, TaskScope):
            self._set_task(scope, key, value, session=session)
        else:
            self._set_asset(scope, key, value, session=session)

    @provide_session
    def delete(self, scope: StateScope, key: str, *, session: Session = NEW_SESSION) -> None:
        if isinstance(scope, TaskScope):
            self._delete_task(scope, key, session=session)
        else:
            self._delete_asset(scope, key, session=session)

    @provide_session
    def clear(self, scope: StateScope, *, session: Session = NEW_SESSION) -> None:
        if isinstance(scope, TaskScope):
            self._clear_task(scope, session=session)
        else:
            self._clear_asset(scope, session=session)

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
        row = session.scalar(
            select(TaskStateModel).where(
                TaskStateModel.dag_id == scope.dag_id,
                TaskStateModel.run_id == scope.run_id,
                TaskStateModel.task_id == scope.task_id,
                TaskStateModel.map_index == scope.map_index,
                TaskStateModel.key == key,
            )
        )
        if row is None:
            dag_run_id = session.scalar(
                select(DagRun.id).where(
                    DagRun.dag_id == scope.dag_id,
                    DagRun.run_id == scope.run_id,
                )
            )
            row = TaskStateModel(
                dag_run_id=dag_run_id,
                dag_id=scope.dag_id,
                run_id=scope.run_id,
                task_id=scope.task_id,
                map_index=scope.map_index,
                key=key,
            )
            session.add(row)
        row.value = value
        row.updated_at = timezone.utcnow()

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
                AssetStateModel.asset_id == int(scope.asset_id),
                AssetStateModel.key == key,
            )
        )
        return row.value if row is not None else None

    def _set_asset(self, scope: AssetScope, key: str, value: str, *, session: Session) -> None:
        row = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == int(scope.asset_id),
                AssetStateModel.key == key,
            )
        )
        if row is None:
            row = AssetStateModel(asset_id=int(scope.asset_id), key=key)
            session.add(row)
        row.value = value
        row.updated_at = timezone.utcnow()

    def _delete_asset(self, scope: AssetScope, key: str, *, session: Session) -> None:
        session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == int(scope.asset_id),
                AssetStateModel.key == key,
            )
        )

    def _clear_asset(self, scope: AssetScope, *, session: Session) -> None:
        session.execute(
            delete(AssetStateModel).where(
                AssetStateModel.asset_id == int(scope.asset_id),
            )
        )
