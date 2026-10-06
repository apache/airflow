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
"""Save Rendered Template Fields."""

from __future__ import annotations

import os
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID

import sqlalchemy as sa
import uuid6
from sqlalchemy import (
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    UniqueConstraint,
    delete,
    select,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm.attributes import NO_VALUE, set_committed_value

from airflow.configuration import conf
from airflow.models.base import Base, StringID, TaskInstanceDependencies
from airflow.models.taskinstance import LegacyTaskDataOwner
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.serialization.helpers import serialize_template_field
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import build_upsert_stmt, get_dialect_name

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.selectable import ScalarSelect

    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.definitions.baseoperator import SerializedBaseOperator


def _get_nested_value(obj: Any, path: str) -> Any:
    """
    Get a nested value from an object using a dot-separated path.

    :param obj: The object to extract the value from
    :param path: A dot-separated path (e.g., "configuration.query.sql")
    :return: The value at the nested path, or None if the path doesn't exist
    """
    keys = path.split(".")
    current = obj
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif hasattr(current, key):
            current = getattr(current, key)
        else:
            return None
        if current is None:
            return None
    return current


def get_serialized_template_fields(task: SerializedBaseOperator):
    """
    Get and serialize the template fields for a task.

    Used in preparing to store them in RTIF table.

    :param task: Operator instance with rendered template fields

    :meta private:
    """
    rendered_fields = {}

    for field in task.template_fields:
        rendered_fields[field] = serialize_template_field(getattr(task, field), field)

    renderers = getattr(task, "template_fields_renderers", {})
    for renderer_path in renderers:
        if "." in renderer_path:
            base_field = renderer_path.split(".", 1)[0]

            if base_field in task.template_fields:
                base_value = getattr(task, base_field)
                nested_value = _get_nested_value(base_value, renderer_path[len(base_field) + 1 :])

                if nested_value is not None:
                    rendered_fields[renderer_path] = serialize_template_field(nested_value, renderer_path)

    return rendered_fields


class LegacyRenderedTaskInstanceFields(TaskInstanceDependencies):
    """Rendered template fields stored before Airflow 3.4, keyed by dag, task, run and map index."""

    __tablename__ = "rtif_v1"

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    task_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    run_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    map_index: Mapped[int] = mapped_column(Integer, primary_key=True, server_default="-1")
    rendered_fields: Mapped[dict] = mapped_column(sa.JSON(), nullable=False)
    k8s_pod_yaml: Mapped[dict | None] = mapped_column(sa.JSON(), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            name="rendered_task_instance_fields_pkey",
        ),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "legacy_task_data_owner.dag_id",
                "legacy_task_data_owner.task_id",
                "legacy_task_data_owner.run_id",
                "legacy_task_data_owner.map_index",
            ],
            name="rtif_ti_fkey",
            ondelete="CASCADE",
        ),
    )


class RenderedTaskInstanceFields(Base):
    """Rendered template fields stored per task attempt, keyed by the attempt UUID."""

    __tablename__ = "rtif_v2"
    id: Mapped[UUID] = mapped_column(sa.Uuid(), primary_key=True, default=uuid6.uuid7)
    task_instance_id: Mapped[UUID] = mapped_column(sa.Uuid(), nullable=False)
    rendered_fields: Mapped[dict] = mapped_column(sa.JSON(), nullable=False)
    k8s_pod_yaml: Mapped[dict | None] = mapped_column(sa.JSON(), nullable=True)
    __table_args__ = (
        PrimaryKeyConstraint("id", name="rtif_v2_pkey"),
        UniqueConstraint("task_instance_id", name="rtif_v2_ti_uq"),
        ForeignKeyConstraint(
            ["task_instance_id"], ["task_instance.id"], name="rtif_v2_ti_fkey", ondelete="CASCADE"
        ),
    )
    task_instance = relationship("TaskInstance", viewonly=True, lazy="raise")

    @classmethod
    @provide_session
    def get_for_attempt(cls, task_instance_id: UUID, *, session: Session = NEW_SESSION):
        return _rendered_fields_for_ids([task_instance_id], session=session).get(task_instance_id)

    @classmethod
    @provide_session
    def set_for_attempt(
        cls,
        *,
        task_instance_id: UUID,
        rendered_fields: dict,
        k8s_pod_yaml: dict | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        from airflow._shared.secrets_masker import redact

        updated = {
            "rendered_fields": {key: redact(value, key) for key, value in rendered_fields.items()},
            "k8s_pod_yaml": redact(k8s_pod_yaml) if k8s_pod_yaml else k8s_pod_yaml,
        }
        session.flush()
        session.execute(
            build_upsert_stmt(
                get_dialect_name(session),
                cls,
                ["task_instance_id"],
                {"id": uuid6.uuid7(), "task_instance_id": task_instance_id, **updated},
                updated,
            )
        )
        for existing in session.identity_map.values():
            if (
                isinstance(existing, cls)
                and existing.__dict__.get("task_instance_id", task_instance_id) == task_instance_id
            ):
                session.expire(existing, list(updated))

    @classmethod
    @provide_session
    def delete_for_attempts(cls, *, producer_ids, session: Session = NEW_SESSION) -> None:
        owner = LegacyTaskDataOwner.__table__
        legacy = LegacyRenderedTaskInstanceFields.__table__
        owns_legacy = (
            select(1)
            .where(
                owner.c.task_instance_id.in_(producer_ids),
                *(owner.c[name] == legacy.c[name] for name in ("dag_id", "task_id", "run_id", "map_index")),
            )
            .exists()
        )
        session.execute(delete(legacy).where(owns_legacy).execution_options(include_all_attempts=True))
        session.execute(
            delete(cls)
            .where(cls.task_instance_id.in_(producer_ids))
            .execution_options(synchronize_session="fetch", include_all_attempts=True)
        )

    @staticmethod
    def _attempt_id(ti: TaskInstance | TaskInstanceKey, session: Session):
        if not isinstance(ti, TaskInstanceKey):
            return ti.id
        from airflow.models.taskinstance import TaskInstance

        return session.scalar(
            select(TaskInstance.id)
            .where(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.map_index == ti.map_index,
                TaskInstance.try_number == ti.try_number,
            )
            .execution_options(include_all_attempts=True)
        )

    def __init__(self, ti: TaskInstance, render_templates=True, rendered_fields=None):
        self.task_instance_id = ti.id
        self.ti = ti
        if render_templates:
            raise ValueError("render_templates=True is no longer supported")

        if TYPE_CHECKING:
            assert isinstance(ti.task, SerializedBaseOperator)

        self.task = ti.task
        if os.environ.get("AIRFLOW_IS_K8S_EXECUTOR_POD", None):
            # we can safely import it here from provider. In Airflow 2.7.0+ you need to have new version
            # of kubernetes provider installed to reach this place
            from airflow.providers.cncf.kubernetes.template_rendering import render_k8s_pod_yaml

            self.k8s_pod_yaml = render_k8s_pod_yaml(ti)
        self.rendered_fields = (
            rendered_fields if rendered_fields is not None else get_serialized_template_fields(task=ti.task)
        )

        self._redact()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.task_instance_id}>"

    def _redact(self):
        from airflow._shared.secrets_masker import redact

        if self.k8s_pod_yaml:
            self.k8s_pod_yaml = redact(self.k8s_pod_yaml)

        for field, rendered in self.rendered_fields.items():
            self.rendered_fields[field] = redact(rendered, field)

    @classmethod
    @provide_session
    def get_templated_fields(
        cls, ti: TaskInstance | TaskInstanceKey, *, session: Session = NEW_SESSION
    ) -> dict | None:
        result = cls.get_for_attempt(cls._attempt_id(ti, session), session=session)
        return result.rendered_fields if result else None

    @classmethod
    @provide_session
    def get_k8s_pod_yaml(cls, ti: TaskInstance, *, session: Session = NEW_SESSION) -> dict | None:
        result = cls.get_for_attempt(ti.id, session=session)
        return result.k8s_pod_yaml if result else None

    @provide_session
    @retry_db_transaction
    def write(self, *, session: Session = NEW_SESSION) -> None:
        self.set_for_attempt(
            task_instance_id=self.task_instance_id,
            rendered_fields=self.rendered_fields,
            k8s_pod_yaml=self.k8s_pod_yaml,
            session=session,
        )

    @classmethod
    @provide_session
    def delete_old_records(
        cls,
        task_id: str,
        dag_id: str,
        num_to_keep: int = conf.getint("core", "num_dag_runs_to_retain_rendered_fields", fallback=0),
        *,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Keep RTIF records from the most recent dag runs, deleting records from older runs.

        Records are retained for the N most recent dag runs (ordered by run_after timestamp).
        All mapped task instance records for a given run are kept or deleted together,
        ensuring no partial data remains.

        :param task_id: Task ID
        :param dag_id: Dag ID
        :param num_to_keep: Number of recent dag runs to retain RTIF records for
        :param session: SqlAlchemy Session
        """
        if num_to_keep <= 0:
            return

        from airflow.models.dagrun import DagRun

        # Find run_ids from the N most recent dag runs (no RTIF table scan needed).
        # Use run_after instead of logical_date since logical_date can be NULL for manual runs.
        run_ids_to_keep_query = (
            select(DagRun.run_id)
            .where(DagRun.dag_id == dag_id)
            .order_by(DagRun.run_after.desc())
            .limit(num_to_keep)
        )

        if get_dialect_name(session) == "mysql":
            # MySQL doesn't support LIMIT in IN/NOT IN subqueries, so fetch IDs first
            run_ids_to_keep: list[str] | ScalarSelect[str] = list(
                session.scalars(run_ids_to_keep_query).all()
            )
        else:
            run_ids_to_keep = run_ids_to_keep_query.scalar_subquery()

        cls._do_delete_old_records(
            dag_id=dag_id,
            task_id=task_id,
            run_ids_to_keep=run_ids_to_keep,
            session=session,
        )
        session.flush()

    @classmethod
    @retry_db_transaction
    def _do_delete_old_records(
        cls,
        *,
        task_id: str,
        dag_id: str,
        run_ids_to_keep: list[str] | ScalarSelect[str],
        session: Session,
    ) -> None:
        from airflow.models.taskinstance import TaskInstance

        legacy = LegacyRenderedTaskInstanceFields.__table__
        session.execute(
            delete(legacy).where(
                legacy.c.dag_id == dag_id,
                legacy.c.task_id == task_id,
                legacy.c.run_id.not_in(run_ids_to_keep),
            )
        )
        session.execute(
            delete(cls)
            .where(
                select(1)
                .where(
                    TaskInstance.id == cls.task_instance_id,
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.task_id == task_id,
                    TaskInstance.run_id.not_in(run_ids_to_keep),
                )
                .exists()
            )
            .execution_options(synchronize_session="fetch")
        )


def _legacy_rendered_fields_for_ids(
    producer_ids: Collection[UUID], *, session: Session
) -> dict[UUID, LegacyRenderedTaskInstanceFields]:
    if not producer_ids:
        return {}
    owner = LegacyTaskDataOwner
    legacy = LegacyRenderedTaskInstanceFields
    legacy_rows = (
        select(owner.task_instance_id, legacy)
        .join(
            legacy,
            sa.and_(
                *(
                    getattr(owner, name) == getattr(legacy, name)
                    for name in ("dag_id", "task_id", "run_id", "map_index")
                )
            ),
        )
        .where(owner.task_instance_id.in_(producer_ids))
    )
    return {owner_id: row for owner_id, row in session.execute(legacy_rows)}


def _rendered_fields_for_ids(
    producer_ids: Sequence[UUID], *, session: Session
) -> dict[UUID, RenderedTaskInstanceFields | LegacyRenderedTaskInstanceFields]:
    if not producer_ids:
        return {}
    fields: dict[UUID, RenderedTaskInstanceFields | LegacyRenderedTaskInstanceFields] = {
        row.task_instance_id: row
        for row in session.scalars(
            select(RenderedTaskInstanceFields).where(
                RenderedTaskInstanceFields.task_instance_id.in_(producer_ids)
            )
        )
    }
    fields.update(_legacy_rendered_fields_for_ids(set(producer_ids) - fields.keys(), session=session))
    return fields


def load_legacy_rendered_fields(task_instances: Sequence[TaskInstance], *, session: Session) -> None:
    """Give task instances with no joined v2 row the rendered fields of their legacy owner, if any."""
    for start in range(0, len(task_instances), 400):
        batch = task_instances[start : start + 400]
        loaded: dict[UUID, Any] = {
            ti.id: sa.inspect(ti).attrs.rendered_task_instance_fields.loaded_value for ti in batch
        }
        found = {
            **_legacy_rendered_fields_for_ids([i for i, v in loaded.items() if v is None], session=session),
            **_rendered_fields_for_ids([i for i, v in loaded.items() if v is NO_VALUE], session=session),
        }
        for ti in batch:
            if loaded[ti.id] is None or loaded[ti.id] is NO_VALUE:
                set_committed_value(ti, "rendered_task_instance_fields", found.get(ti.id))
