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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import (
    JSON,
    Column,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    delete,
    func,
    select,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query, relationship

from airflow.models.base import COLLATION_ARGS, ID_LEN, TaskInstanceDependencies
from airflow.utils import timezone
from airflow.utils.db import LazySelectSequence
from airflow.utils.helpers import is_container
from airflow.utils.json import XComDecoder, XComEncoder
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

# XCom constants below are needed for providers backward compatibility,
# which should import the constants directly after apache-airflow>=2.6.0
from airflow.utils.xcom import (
    MAX_XCOM_SIZE,  # noqa: F401
    XCOM_RETURN_KEY,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.expression import Select, TextClause


class XComModel(TaskInstanceDependencies):
    """XCom model class. Contains table and some utilities."""

    __tablename__ = "xcom"

    dag_run_id = Column(Integer(), nullable=False, primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False, primary_key=True)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    key = Column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)

    # Denormalized for easier lookup.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)

    value = Column(JSON().with_variant(postgresql.JSONB, "postgresql"))
    timestamp = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

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
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="xcom_task_instance_fkey",
            ondelete="CASCADE",
        ),
    )

    dag_run = relationship(
        "DagRun",
        primaryjoin="XComModel.dag_run_id == foreign(DagRun.id)",
        uselist=False,
        lazy="joined",
        passive_deletes="all",
    )
    logical_date = association_proxy("dag_run", "logical_date")

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

        query = session.query(cls).filter_by(dag_id=dag_id, task_id=task_id, run_id=run_id)
        if map_index is not None:
            query = query.filter_by(map_index=map_index)

        for xcom in query:
            # print(f"Clearing XCOM {xcom} with value {xcom.value}")
            session.delete(xcom)

        session.commit()

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
            The default is ``-1`` (set for a non-mapped task).
        :param session: Database session. If not given, a new session will be
            created for this function.
        """
        from airflow.models.dagrun import DagRun

        if not run_id:
            raise ValueError(f"run_id must be passed. Passed run_id={run_id}")

        dag_run_id = session.query(DagRun.id).filter_by(dag_id=dag_id, run_id=run_id).scalar()
        if dag_run_id is None:
            raise ValueError(f"DAG run not found on DAG {dag_id!r} with ID {run_id!r}")

        # Seamlessly resolve LazySelectSequence to a list. This intends to work
        # as a "lazy list" to avoid pulling a ton of XComs unnecessarily, but if
        # it's pushed into XCom, the user should be aware of the performance
        # implications, and this avoids leaking the implementation detail.
        if isinstance(value, LazySelectSequence):
            warning_message = (
                "Coercing mapped lazy proxy %s from task %s (DAG %s, run %s) "
                "to list, which may degrade performance. Review resource "
                "requirements for this operation, and call list() to suppress "
                "this message. See Dynamic Task Mapping documentation for "
                "more information about lazy proxy objects."
            )
            log.warning(
                warning_message,
                "return value" if key == XCOM_RETURN_KEY else f"value {key}",
                task_id,
                dag_id,
                run_id,
            )
            value = list(value)

        value = cls.serialize_value(
            value=value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index,
        )

        # Remove duplicate XComs and insert a new one.
        session.execute(
            delete(cls).where(
                cls.key == key,
                cls.run_id == run_id,
                cls.task_id == task_id,
                cls.dag_id == dag_id,
                cls.map_index == map_index,
            )
        )

        new = cast("Any", cls)(  # Work around Mypy complaining model not defining '__init__'.
            dag_run_id=dag_run_id,
            key=key,
            value=value,
            run_id=run_id,
            task_id=task_id,
            dag_id=dag_id,
            map_index=map_index,
        )
        session.add(new)
        session.flush()

    @classmethod
    @provide_session
    def get_many(
        cls,
        *,
        run_id: str,
        key: str | None = None,
        task_ids: str | Iterable[str] | None = None,
        dag_ids: str | Iterable[str] | None = None,
        map_indexes: int | Iterable[int] | None = None,
        include_prior_dates: bool = False,
        limit: int | None = None,
        session: Session = NEW_SESSION,
    ) -> Query:
        """
        Composes a query to get one or more XCom entries.

        This function returns an SQLAlchemy query of full XCom objects. If you
        just want one stored value, use :meth:`get_one` instead.

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
        :param session: Database session. If not given, a new session will be
            created for this function.
        :param limit: Limiting returning XComs
        """
        from airflow.models.dagrun import DagRun

        if not run_id:
            raise ValueError(f"run_id must be passed. Passed run_id={run_id}")

        query = session.query(cls).join(XComModel.dag_run)

        if key:
            query = query.filter(XComModel.key == key)

        if is_container(task_ids):
            query = query.filter(cls.task_id.in_(task_ids))
        elif task_ids is not None:
            query = query.filter(cls.task_id == task_ids)

        if is_container(dag_ids):
            query = query.filter(cls.dag_id.in_(dag_ids))
        elif dag_ids is not None:
            query = query.filter(cls.dag_id == dag_ids)

        if isinstance(map_indexes, range) and map_indexes.step == 1:
            query = query.filter(cls.map_index >= map_indexes.start, cls.map_index < map_indexes.stop)
        elif is_container(map_indexes):
            query = query.filter(cls.map_index.in_(map_indexes))
        elif map_indexes is not None:
            query = query.filter(cls.map_index == map_indexes)

        if include_prior_dates:
            dr = (
                session.query(
                    func.coalesce(DagRun.logical_date, DagRun.run_after).label("logical_date_or_run_after")
                )
                .filter(DagRun.run_id == run_id)
                .subquery()
            )

            query = query.filter(
                func.coalesce(DagRun.logical_date, DagRun.run_after) <= dr.c.logical_date_or_run_after
            )
        else:
            query = query.filter(cls.run_id == run_id)

        query = query.order_by(DagRun.logical_date.desc(), cls.timestamp.desc())
        if limit:
            return query.limit(limit)
        return query

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
    def deserialize_value(result) -> Any:
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


class LazyXComSelectSequence(LazySelectSequence[Any]):
    """
    List-like interface to lazily access XCom values.

    :meta private:
    """

    @staticmethod
    def _rebuild_select(stmt: TextClause) -> Select:
        return select(XComModel.value).from_statement(stmt)

    @staticmethod
    def _process_row(row: Row) -> Any:
        return XComModel.deserialize_value(row)


def __getattr__(name: str):
    if name == "BaseXCom":
        from airflow.sdk.bases.xcom import BaseXCom

        globals()[name] = BaseXCom
        return BaseXCom

    if name == "XCom":
        from airflow.sdk.execution_time.xcom import XCom

        globals()[name] = XCom
        return XCom

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
