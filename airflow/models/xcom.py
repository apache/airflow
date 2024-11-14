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

import inspect
import json
import logging
import pickle
from typing import TYPE_CHECKING, Any, Iterable, cast

from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    PrimaryKeyConstraint,
    String,
    delete,
    select,
    text,
)
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query, reconstructor, relationship

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.configuration import conf
from airflow.models.base import COLLATION_ARGS, ID_LEN, TaskInstanceDependencies
from airflow.utils import timezone
from airflow.utils.db import LazySelectSequence
from airflow.utils.helpers import is_container
from airflow.utils.json import XComDecoder, XComEncoder
from airflow.utils.log.logging_mixin import LoggingMixin
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

    from airflow.models.taskinstancekey import TaskInstanceKey


class BaseXCom(TaskInstanceDependencies, LoggingMixin):
    """Base class for XCom objects."""

    __tablename__ = "xcom"

    dag_run_id = Column(Integer(), nullable=False, primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False, primary_key=True)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    key = Column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)

    # Denormalized for easier lookup.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)

    value = Column(LargeBinary().with_variant(LONGBLOB, "mysql"))
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
        primaryjoin="BaseXCom.dag_run_id == foreign(DagRun.id)",
        uselist=False,
        lazy="joined",
        passive_deletes="all",
    )
    execution_date = association_proxy("dag_run", "execution_date")

    @reconstructor
    def init_on_load(self):
        """
        Execute after the instance has been loaded from the DB or otherwise reconstituted; called by the ORM.

        i.e automatically deserialize Xcom value when loading from DB.
        """
        self.value = self.orm_deserialize_value()

    def __repr__(self):
        if self.map_index < 0:
            return f'<XCom "{self.key}" ({self.task_id} @ {self.run_id})>'
        return f'<XCom "{self.key}" ({self.task_id}[{self.map_index}] @ {self.run_id})>'

    @classmethod
    @internal_api_call
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
        new = cast(Any, cls)(  # Work around Mypy complaining model not defining '__init__'.
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

    @staticmethod
    @provide_session
    @internal_api_call
    def get_value(
        *,
        ti_key: TaskInstanceKey,
        key: str | None = None,
        session: Session = NEW_SESSION,
    ) -> Any:
        """
        Retrieve an XCom value for a task instance.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        :param ti_key: The TaskInstanceKey to look up the XCom for.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param session: Database session. If not given, a new session will be
            created for this function.
        """
        return BaseXCom.get_one(
            key=key,
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            map_index=ti_key.map_index,
            session=session,
        )

    @staticmethod
    @provide_session
    @internal_api_call
    def get_one(
        *,
        key: str | None = None,
        dag_id: str | None = None,
        task_id: str | None = None,
        run_id: str,
        map_index: int | None = None,
        session: Session = NEW_SESSION,
        include_prior_dates: bool = False,
    ) -> Any | None:
        """
        Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        .. seealso:: ``get_value()`` is a convenience function if you already
            have a structured TaskInstance or TaskInstanceKey object available.

        :param run_id: DAG run ID for the task.
        :param dag_id: Only pull XCom from this DAG. Pass *None* (default) to
            remove the filter.
        :param task_id: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param map_index: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param include_prior_dates: If *False* (default), only XCom from the
            specified DAG run is returned. If *True*, the latest matching XCom is
            returned regardless of the run it belongs to.
        :param session: Database session. If not given, a new session will be
            created for this function.
        """
        query = BaseXCom.get_many(
            run_id=run_id,
            key=key,
            task_ids=task_id,
            dag_ids=dag_id,
            map_indexes=map_index,
            include_prior_dates=include_prior_dates,
            limit=1,
            session=session,
        )

        result = query.with_entities(BaseXCom.value).first()
        if result:
            return XCom.deserialize_value(result)
        return None

    # The 'get_many` is not supported via database isolation mode. Attempting to use it in DB isolation
    # mode will result in a crash - Resulting Query object cannot be **really** serialized
    # TODO(potiuk) - document it in AIP-44 docs
    @staticmethod
    @provide_session
    def get_many(
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

        query = session.query(BaseXCom).join(BaseXCom.dag_run)

        if key:
            query = query.filter(BaseXCom.key == key)

        if is_container(task_ids):
            query = query.filter(BaseXCom.task_id.in_(task_ids))
        elif task_ids is not None:
            query = query.filter(BaseXCom.task_id == task_ids)

        if is_container(dag_ids):
            query = query.filter(BaseXCom.dag_id.in_(dag_ids))
        elif dag_ids is not None:
            query = query.filter(BaseXCom.dag_id == dag_ids)

        if isinstance(map_indexes, range) and map_indexes.step == 1:
            query = query.filter(
                BaseXCom.map_index >= map_indexes.start, BaseXCom.map_index < map_indexes.stop
            )
        elif is_container(map_indexes):
            query = query.filter(BaseXCom.map_index.in_(map_indexes))
        elif map_indexes is not None:
            query = query.filter(BaseXCom.map_index == map_indexes)

        if include_prior_dates:
            dr = session.query(DagRun.execution_date).filter(DagRun.run_id == run_id).subquery()
            query = query.filter(BaseXCom.execution_date <= dr.c.execution_date)
        else:
            query = query.filter(BaseXCom.run_id == run_id)

        query = query.order_by(DagRun.execution_date.desc(), BaseXCom.timestamp.desc())
        if limit:
            return query.limit(limit)
        return query

    @classmethod
    @provide_session
    def delete(cls, xcoms: XCom | Iterable[XCom], session: Session) -> None:
        """Delete one or multiple XCom entries."""
        if isinstance(xcoms, XCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, XCom):
                raise TypeError(f"Expected XCom; received {xcom.__class__.__name__}")
            XCom.purge(xcom, session)
            session.delete(xcom)
        session.commit()

    @staticmethod
    def purge(xcom: XCom, session: Session) -> None:
        """Purge an XCom entry from underlying storage implementations."""
        pass

    @staticmethod
    @provide_session
    @internal_api_call
    def clear(
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Clear all XCom data from the database for the given task instance.

        :param dag_id: ID of DAG to clear the XCom for.
        :param task_id: ID of task to clear the XCom for.
        :param run_id: ID of DAG run to clear the XCom for.
        :param map_index: If given, only clear XCom from this particular mapped
            task. The default ``None`` clears *all* XComs from the task.
        :param session: Database session. If not given, a new session will be
            created for this function.
        """
        # Given the historic order of this function (execution_date was first argument) to add a new optional
        # param we need to add default values for everything :(
        if dag_id is None:
            raise TypeError("clear() missing required argument: dag_id")
        if task_id is None:
            raise TypeError("clear() missing required argument: task_id")

        if not run_id:
            raise ValueError(f"run_id must be passed. Passed run_id={run_id}")

        query = session.query(BaseXCom).filter_by(dag_id=dag_id, task_id=task_id, run_id=run_id)
        if map_index is not None:
            query = query.filter_by(map_index=map_index)

        for xcom in query:
            # print(f"Clearing XCOM {xcom} with value {xcom.value}")
            XCom.purge(xcom, session)
            session.delete(xcom)

        session.commit()

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> Any:
        """Serialize XCom value to str or pickled object."""
        if conf.getboolean("core", "enable_xcom_pickling"):
            return pickle.dumps(value)
        try:
            return json.dumps(value, cls=XComEncoder).encode("UTF-8")
        except (ValueError, TypeError) as ex:
            log.error(
                "%s."
                " If you are using pickle instead of JSON for XCom,"
                " then you need to enable pickle support for XCom"
                " in your airflow config or make sure to decorate your"
                " object with attr.",
                ex,
            )
            raise

    @staticmethod
    def _deserialize_value(result: XCom, orm: bool) -> Any:
        object_hook = None
        if orm:
            object_hook = XComDecoder.orm_object_hook

        if result.value is None:
            return None
        if conf.getboolean("core", "enable_xcom_pickling"):
            try:
                return pickle.loads(result.value)
            except pickle.UnpicklingError:
                return json.loads(result.value.decode("UTF-8"), cls=XComDecoder, object_hook=object_hook)
        else:
            # Since xcom_pickling is disabled, we should only try to deserialize with JSON
            return json.loads(result.value.decode("UTF-8"), cls=XComDecoder, object_hook=object_hook)

    @staticmethod
    def deserialize_value(result: XCom) -> Any:
        """Deserialize XCom value from str or pickle object."""
        return BaseXCom._deserialize_value(result, False)

    def orm_deserialize_value(self) -> Any:
        """
        Deserialize method which is used to reconstruct ORM XCom object.

        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom orm model. This is used when viewing XCom listing
        in the webserver, for example.
        """
        return BaseXCom._deserialize_value(self, True)


class LazyXComSelectSequence(LazySelectSequence[Any]):
    """
    List-like interface to lazily access XCom values.

    :meta private:
    """

    @staticmethod
    def _rebuild_select(stmt: TextClause) -> Select:
        return select(XCom.value).from_statement(stmt)

    @staticmethod
    def _process_row(row: Row) -> Any:
        return XCom.deserialize_value(row)


def _get_function_params(function) -> list[str]:
    """
    Return the list of variables names of a function.

    :param function: The function to inspect
    """
    parameters = inspect.signature(function).parameters
    bound_arguments = [
        name for name, p in parameters.items() if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    ]
    return bound_arguments


def resolve_xcom_backend() -> type[BaseXCom]:
    """
    Resolve custom XCom class.

    Confirm that custom XCom class extends the BaseXCom.
    Compare the function signature of the custom XCom serialize_value to the base XCom serialize_value.
    """
    clazz = conf.getimport("core", "xcom_backend", fallback=f"airflow.models.xcom.{BaseXCom.__name__}")
    if not clazz:
        return BaseXCom
    if not issubclass(clazz, BaseXCom):
        raise TypeError(
            f"Your custom XCom class `{clazz.__name__}` is not a subclass of `{BaseXCom.__name__}`."
        )
    return clazz


if TYPE_CHECKING:
    XCom = BaseXCom  # Hack to avoid Mypy "Variable 'XCom' is not valid as a type".
else:
    XCom = resolve_xcom_backend()
