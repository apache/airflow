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

import json
import logging
import threading
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import uuid6
from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    Uuid,
    and_,
    delete,
    event,
    func,
    select,
    union_all,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, aliased, foreign, mapped_column, relationship
from sqlalchemy.sql.visitors import cloned_traverse

from airflow._shared.timezones import timezone
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base, TaskInstanceDependencies
from airflow.models.dynamic_region import SENTINEL_REGION_ID
from airflow.utils.db import LazySelectSequence
from airflow.utils.helpers import is_container
from airflow.utils.json import XComDecoder, XComEncoder
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, build_upsert_stmt

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session
    from sqlalchemy.orm.util import AliasedClass
    from sqlalchemy.sql.expression import CompoundSelect, Select, Subquery, TextClause

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance


XCOM_RETURN_KEY = "return_value"


class XComModelV1(TaskInstanceDependencies):
    """XCom values stored before Airflow 3.4, keyed by dag, task, run and map index."""

    __tablename__ = "xcom_v1"

    dag_run_id: Mapped[int] = mapped_column(Integer(), nullable=False, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(ID_LEN, **COLLATION_ARGS), nullable=False, primary_key=True)
    map_index: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, server_default="-1")
    key: Mapped[str] = mapped_column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)
    dag_result: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=False)

    # Denormalized for easier lookup.
    dag_id: Mapped[str] = mapped_column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    run_id: Mapped[str] = mapped_column(String(ID_LEN, **COLLATION_ARGS), nullable=False)

    value: Mapped[Any] = mapped_column(JSON().with_variant(postgresql.JSONB, "postgresql"), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    # NULL unless the value can expand a downstream mapped task (AIP-42).
    mapped_length: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        # Ideally we should create a unique index over (key, dag_id, task_id, run_id),
        # but it goes over MySQL's index length limit. So we instead index 'key'
        # separately, and enforce uniqueness with DagRun.id instead.
        Index("idx_xcom_key", key),
        Index("idx_xcom_task_instance", dag_id, task_id, run_id, map_index),
        PrimaryKeyConstraint("dag_run_id", "task_id", "map_index", "key", name="xcom_pkey"),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "legacy_task_data_owner.dag_id",
                "legacy_task_data_owner.task_id",
                "legacy_task_data_owner.run_id",
                "legacy_task_data_owner.map_index",
            ],
            name="xcom_task_instance_fkey",
            ondelete="CASCADE",
        ),
    )


class XComModelV2(Base):
    """XCom values stored per task attempt, keyed by the attempt UUID."""

    __tablename__ = "xcom_v2"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    task_instance_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False)
    key: Mapped[str] = mapped_column(String(512, **COLLATION_ARGS), nullable=False)
    value: Mapped[Any] = mapped_column(JSON().with_variant(postgresql.JSONB, "postgresql"), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    dag_result: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=False)
    mapped_length: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint("id", name="xcom_v2_pkey"),
        UniqueConstraint("task_instance_id", "key", name="xcom_v2_ti_key_uq"),
        ForeignKeyConstraint(
            ["task_instance_id"], ["task_instance.id"], name="xcom_v2_ti_fkey", ondelete="CASCADE"
        ),
        CheckConstraint(mapped_length >= 0, name="xcom_v2_mapped_length_not_negative"),
    )
    task = relationship("TaskInstance", viewonly=True, lazy="raise")

    @classmethod
    def get_for_attempt(cls, task_instance_id: UUID, key: str, *, session: Session) -> XComModelV2 | None:
        return session.scalar(select(cls).where(cls.task_instance_id == task_instance_id, cls.key == key))


class _XComOperations:
    @classmethod
    @provide_session
    def set_for_attempt(
        cls,
        *,
        task_instance_id: UUID,
        key: str,
        value: Any,
        serialize: bool = True,
        dag_result: bool = False,
        mapped_length: int | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        if not key:
            raise ValueError(f"XCom key must be a non-empty string. Received: {key!r}")
        if isinstance(value, LazySelectSequence):
            log.warning("Coercing mapped lazy XCom for attempt %s to a list", task_instance_id)
            value = list(value)
        if serialize:
            value = cls.serialize_value(value, key=key)
        updated = {
            "value": value,
            "timestamp": timezone.utcnow(),
            "dag_result": dag_result,
            "mapped_length": mapped_length,
        }
        session.flush()
        session.execute(
            build_upsert_stmt(
                session.get_bind().dialect.name,
                XComModelV2,
                ["task_instance_id", "key"],
                {"id": uuid6.uuid7(), "task_instance_id": task_instance_id, "key": key, **updated},
                updated,
            )
        )
        for existing in session.identity_map.values():
            if (
                isinstance(existing, XComModelV2)
                and existing.__dict__.get("task_instance_id", task_instance_id) == task_instance_id
                and existing.__dict__.get("key", key) == key
            ):
                session.expire(existing, list(updated))

    @classmethod
    @provide_session
    def delete_for_attempts(
        cls,
        *,
        producer_ids: Select,
        key: str | None,
        session: Session = NEW_SESSION,
    ) -> None:
        from airflow.models.taskinstance import LegacyTaskDataOwner

        owner = LegacyTaskDataOwner.__table__
        legacy = XComModelV1.__table__
        owns_legacy = (
            select(1)
            .where(
                owner.c.task_instance_id.in_(producer_ids),
                *(owner.c[name] == legacy.c[name] for name in ("dag_id", "task_id", "run_id", "map_index")),
            )
            .exists()
        )
        v1_delete = delete(legacy).where(owns_legacy)
        v2_delete = delete(XComModelV2).where(XComModelV2.task_instance_id.in_(producer_ids))
        if key is not None:
            v1_delete = v1_delete.where(legacy.c.key == key)
            v2_delete = v2_delete.where(XComModelV2.key == key)
        session.execute(v1_delete.execution_options(include_all_attempts=True))
        session.execute(v2_delete.execution_options(synchronize_session="fetch", include_all_attempts=True))

    @classmethod
    @provide_session
    def clear(
        cls,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Clear all XCom data from the database for the given task instance.

        .. note:: This **will not** purge any data from a custom XCom backend.

        :param dag_id: ID of DAG to clear the XCom for.
        :param task_id: ID of task to clear the XCom for.
        :param run_id: ID of DAG run to clear the XCom for.
        :param map_index: If given, only clear XCom from this particular mapped
            task. The default ``None`` clears *all* XComs from the task.
        :param session: Database session. If not given, a new session will be
            created for this function.
        """
        # Given the historic order of this function (logical_date was first argument) to add a new optional
        # param we need to add default values for everything :(
        if dag_id is None:
            raise TypeError("clear() missing required argument: dag_id")
        if task_id is None:
            raise TypeError("clear() missing required argument: task_id")

        if not run_id:
            raise ValueError(f"run_id must be passed. Passed run_id={run_id}")
        producer_ids = select_producers(
            dag_ids=dag_id, task_ids=task_id, run_id=run_id, map_indexes=map_index
        )
        cls.delete_for_attempts(producer_ids=producer_ids, key=None, session=session)

    @classmethod
    @provide_session
    def set(
        cls,
        key: str,
        value: Any,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int = -1,
        serialize: bool = True,
        dag_result: bool = False,
        mapped_length: int | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Store an XCom value.

        :param key: Key to store the XCom.
        :param value: XCom value to store.
        :param dag_id: DAG ID.
        :param task_id: Task ID.
        :param run_id: DAG run ID for the task.
        :param map_index: Optional map index to assign XCom for a mapped task.
        :param serialize: Optional parameter to specify if value should be serialized or not.
            The default is ``True``.
        :param mapped_length: Length of the value, if it can be used to expand a
            downstream mapped task.
        :param session: Database session. If not given, a new session will be
            created for this function.
        """
        if not run_id:
            raise ValueError(f"run_id must be passed. Passed run_id={run_id}")
        owner = session.scalar(
            select_producers(dag_ids=dag_id, task_ids=task_id, run_id=run_id, map_indexes=map_index)
        )
        if owner is None:
            raise ValueError(
                f"Task instance not found: {dag_id}.{task_id} in {run_id}, map index {map_index}"
            )
        cls.set_for_attempt(
            task_instance_id=owner,
            key=key,
            value=value,
            serialize=serialize,
            dag_result=dag_result,
            mapped_length=mapped_length,
            session=session,
        )

    @classmethod
    def get_many(
        cls,
        *,
        run_id: str,
        key: str | None = None,
        task_ids: str | Iterable[str] | None = None,
        dag_ids: str | Iterable[str] | None = None,
        map_indexes: int | Iterable[int] | None = None,
        region_id: UUID | None = SENTINEL_REGION_ID,
        producer_ids: Select | None = None,
        include_prior_dates: bool = False,
        limit: int | None = None,
        try_number: int | None = None,
    ) -> Select[tuple[XComModel]]:
        """
        Composes a query to get one or more XCom entries.

        This function returns an SQLAlchemy query of full XCom objects. If you
        just want one stored value, use :meth:`get_one` instead.

        ``region_id`` is the exact producer region (the legacy sentinel by default); pass ``None`` to
        enumerate across regions. ``producer_ids`` names attempts already resolved by
        :func:`~airflow.models.dynamic_region.resolve_current_producers` and cannot be combined with
        ``task_ids``, ``dag_ids``, ``map_indexes``, ``region_id`` or ``include_prior_dates``.

        Use :func:`xcom_entity` for columns added to the returned statement.

        :param run_id: DAG run ID for the task.
        :param key: A key for the XComs. If provided, only XComs with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param task_ids: Only XComs from task with matching IDs will be pulled.
            Pass *None* (default) to remove the filter.
        :param dag_ids: Only pulls XComs from specified DAGs. Pass *None*
            (default) to remove the filter.
        :param map_indexes: Only XComs from matching map indexes will be pulled.
            Pass *None* (default) to remove the filter.
        :param include_prior_dates: If *False* (default), only XComs from the
            specified DAG run are returned. If *True*, all matching XComs are
            returned regardless of the run it belongs to.
        :param limit: Limiting returning XComs
        :param try_number: Read the XComs of this public try, current or archived, instead of
            the current attempt.
        """
        if key is not None and not key:
            raise ValueError(f"XCom key must be a non-empty string. Received: {key!r}")
        if not run_id:
            raise ValueError(f"run_id must be passed. Passed run_id={run_id}")
        if producer_ids is not None:
            if (
                any(value is not None for value in (task_ids, dag_ids, map_indexes))
                or include_prior_dates
                or try_number is not None
                or region_id != SENTINEL_REGION_ID
            ):
                raise ValueError("producer_ids cannot be combined with coordinate filters")
        else:
            if include_prior_dates and region_id not in (None, SENTINEL_REGION_ID):
                raise ValueError(
                    "Prior-run lookup requires producer coordinates resolved separately for each run"
                )
            producer_ids = select_producers(
                run_id=run_id,
                task_ids=task_ids,
                dag_ids=dag_ids,
                map_indexes=map_indexes,
                region_id=region_id,
                include_prior_dates=include_prior_dates,
                try_number=try_number,
            )
        statement = build_xcom_read_query(producer_ids=producer_ids, key=key)
        entity = xcom_entity(statement)
        statement = statement.order_by(entity.logical_date.desc(), entity.timestamp.desc())
        if limit:
            statement = statement.limit(limit)
        if try_number is not None:
            statement = statement.execution_options(include_all_attempts=True)
        return statement

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> str:
        """Serialize XCom value to JSON str."""
        try:
            return json.dumps(value, cls=XComEncoder)
        except (ValueError, TypeError):
            raise ValueError("XCom value must be JSON serializable")

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        """
        Deserialize XCom value from a database result.

        If deserialization fails, the raw value is returned, which must still be a valid Python JSON-compatible
        type (e.g., ``dict``, ``list``, ``str``, ``int``, ``float``, or ``bool``).

        XCom values are stored as JSON in the database, and SQLAlchemy automatically handles
        serialization (``json.dumps``) and deserialization (``json.loads``). However, we
        use a custom encoder for serialization (``serialize_value``) and deserialization to handle special
        cases, such as encoding tuples via the Airflow Serialization module. These must be decoded
        using ``XComDecoder`` to restore original types.

        Some XCom values, such as those set via the Task Execution API, bypass ``serialize_value``
        and are stored directly in JSON format. Since these values are already deserialized
        by SQLAlchemy, they are returned as-is.

        **Example: Handling a tuple**:

        .. code-block:: python

            original_value = (1, 2, 3)
            serialized_value = XComModel.serialize_value(original_value)
            print(serialized_value)
            # '{"__classname__": "builtins.tuple", "__version__": 1, "__data__": [1, 2, 3]}'

        This serialized value is stored in the database. When deserialized, the value is restored to the original tuple.

        :param result: The XCom database row or object containing a ``value`` attribute.
        :return: The deserialized Python object.
        """
        if result.value is None:
            return None

        try:
            return json.loads(result.value, cls=XComDecoder)
        except (ValueError, TypeError):
            # Already deserialized (e.g., set via Task Execution API)
            return result.value


def _rows():
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import LegacyTaskDataOwner, TaskInstance

    coordinates = ("dag_id", "task_id", "run_id", "map_index")
    data = ("key", "value", "timestamp", "dag_result", "mapped_length")
    run_join = and_(DagRun.dag_id == TaskInstance.dag_id, DagRun.run_id == TaskInstance.run_id)
    context = [
        *(getattr(TaskInstance, name) for name in coordinates),
        DagRun.id.label("dag_run_id"),
        DagRun.logical_date,
        DagRun.run_after,
    ]
    new_rows = (
        select(XComModelV2.task_instance_id, *(getattr(XComModelV2, name) for name in data), *context)
        .select_from(XComModelV2)
        .join(TaskInstance, XComModelV2.task_instance_id == TaskInstance.id)
        .join(DagRun, run_join)
    )
    old_rows = (
        select(LegacyTaskDataOwner.task_instance_id, *(getattr(XComModelV1, name) for name in data), *context)
        .select_from(LegacyTaskDataOwner)
        .join(
            XComModelV1,
            and_(*(getattr(LegacyTaskDataOwner, name) == getattr(XComModelV1, name) for name in coordinates)),
        )
        .join(TaskInstance, LegacyTaskDataOwner.task_instance_id == TaskInstance.id)
        .join(DagRun, run_join)
        .where(
            ~select(1)
            .select_from(XComModelV2)
            .where(
                XComModelV2.task_instance_id == LegacyTaskDataOwner.task_instance_id,
                XComModelV2.key == XComModelV1.key,
            )
            .exists()
        )
    )
    return union_all(new_rows, old_rows)


def _filter_rows(rows: Subquery, *, producer_ids: Select, key: str | None) -> Subquery:
    """
    Restrict each store's rows inside the union, so the filters apply before the stores are combined.

    The result is a clone of the model's own subquery rather than a new one: relationship joins are adapted
    onto a selectable only when the model's columns are embedded in it, which a sibling subquery is not.
    """

    def restrict_stores(union: CompoundSelect) -> None:
        union.selects = [
            cast("Select", store).where(
                store.selected_columns["task_instance_id"].in_(producer_ids),
                *([store.selected_columns["key"] == key] if key is not None else []),
            )
            for store in union.selects
        ]

    return cloned_traverse(rows, {}, {"compound_select": restrict_stores})


if TYPE_CHECKING:

    class XComModel(_XComOperations, Base):
        """Read both stores without sharing writable ORM identity."""

        task_instance_id: Mapped[UUID]
        key: Mapped[str]
        value: Mapped[Any]
        timestamp: Mapped[datetime]
        dag_result: Mapped[bool | None]
        mapped_length: Mapped[int | None]
        dag_id: Mapped[str]
        task_id: Mapped[str]
        run_id: Mapped[str]
        map_index: Mapped[int]
        dag_run_id: Mapped[int]
        logical_date: Mapped[datetime | None]
        run_after: Mapped[datetime]
        dag_run: Mapped[DagRun]
        task: Mapped[TaskInstance]


_xcom_model_lock = threading.RLock()
_xcom_model: type[XComModel] | None = None


def get_xcom_model() -> type[XComModel]:
    """
    Return ``XComModel``, defining it on first use.

    Its table is built from ``TaskInstance`` and ``DagRun`` columns, so defining the class when this module
    is imported would force the module to import them first, and ``taskinstance`` could then no longer
    import this module without a circular import.
    """
    global _xcom_model
    with _xcom_model_lock:
        if _xcom_model is None:
            _xcom_model = _define_xcom_model()
        return _xcom_model


def _define_xcom_model() -> type[XComModel]:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    class XComModel(_XComOperations, Base):
        """Read both stores without sharing writable ORM identity."""

        __table__ = _rows().subquery("xcom")
        __mapper_args__ = {"primary_key": [__table__.c.task_instance_id, __table__.c.key]}

        dag_run = relationship(
            DagRun,
            primaryjoin=foreign(__table__.c.dag_run_id) == DagRun.id,
            viewonly=True,
            uselist=False,
            lazy="raise",
        )
        task = relationship(
            TaskInstance,
            primaryjoin=foreign(__table__.c.task_instance_id) == TaskInstance.id,
            viewonly=True,
            uselist=False,
            lazy="raise",
        )

    XComModel.__qualname__ = "XComModel"
    for mutation in ("before_insert", "before_update", "before_delete"):
        event.listen(XComModel, mutation, _reject_projection_write)
    return XComModel


def _reject_projection_write(mapper, connection, target):
    raise TypeError("XCom read projection is read-only")


def build_xcom_read_query(*, producer_ids: Select, key: str | None = None) -> Select[tuple[XComModel]]:
    model = get_xcom_model()
    entity = aliased(model, _filter_rows(model.__table__, producer_ids=producer_ids, key=key))
    return select(entity).execution_options(populate_existing=True)


def xcom_entity(statement: Select) -> AliasedClass:
    """Return the mapped XCom alias selected by a dual-store read."""
    return statement.column_descriptions[0]["entity"]


def select_producers(
    *,
    run_id=None,
    dag_ids=None,
    task_ids=None,
    map_indexes=None,
    region_id=SENTINEL_REGION_ID,
    include_prior_dates=False,
    try_number=None,
):
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    query = select(TaskInstance.id)
    if region_id is not None:
        query = query.where(TaskInstance.region_id == region_id)
    if try_number is not None:
        query = query.where(TaskInstance.try_number == try_number)
    for column, value in ((TaskInstance.dag_id, dag_ids), (TaskInstance.task_id, task_ids)):
        if is_container(value):
            query = query.where(column.in_(value))
        elif value is not None:
            query = query.where(column == value)
    if isinstance(map_indexes, range) and map_indexes.step == 1:
        query = query.where(
            TaskInstance.map_index >= map_indexes.start, TaskInstance.map_index < map_indexes.stop
        )
    elif is_container(map_indexes):
        query = query.where(TaskInstance.map_index.in_(map_indexes))
    elif map_indexes is not None:
        query = query.where(TaskInstance.map_index == map_indexes)
    if include_prior_dates:
        requested_run = aliased(DagRun)
        cutoff = (
            select(func.coalesce(requested_run.logical_date, requested_run.run_after))
            .where(
                requested_run.dag_id == TaskInstance.dag_id,
                requested_run.run_id == run_id,
            )
            .correlate(TaskInstance)
            .scalar_subquery()
        )
        query = query.join(
            DagRun, and_(TaskInstance.dag_id == DagRun.dag_id, TaskInstance.run_id == DagRun.run_id)
        ).where(func.coalesce(DagRun.logical_date, DagRun.run_after) <= cutoff)
    elif run_id is not None:
        query = query.where(TaskInstance.run_id == run_id)
    return query


class LazyXComSelectSequence(LazySelectSequence[Any]):
    """
    List-like interface to lazily access XCom values.

    :meta private:
    """

    @staticmethod
    def _rebuild_select(stmt: TextClause) -> Select[tuple[Any]]:
        return cast("Select[tuple[Any]]", select(get_xcom_model().value).from_statement(stmt))

    @staticmethod
    def _process_row(row: Row) -> Any:
        return get_xcom_model().deserialize_value(row)


__compat_imports = {
    "BaseXCom": "airflow.sdk.bases.xcom",
    "XCom": "airflow.sdk.execution_time.xcom",
    "XComArg": "airflow.sdk",
}


def __getattr__(name: str):
    import importlib
    import warnings

    from airflow.utils.deprecation_tools import DeprecatedImportWarning

    if name == "XComModel":
        return get_xcom_model()

    try:
        modpath = __compat_imports[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    warnings.warn(
        f"Importing {name} from 'airflow.models.xcom' is deprecated and will be removed in a future version. "
        f"Please import from '{modpath}' instead.",
        DeprecatedImportWarning,
        stacklevel=2,
    )

    mod = importlib.import_module(modpath)
    value = getattr(mod, name)
    globals()[name] = value
    return value
