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

import collections.abc
import contextlib
import datetime
import inspect
import itertools
import json
import logging
import pickle
import warnings
from functools import wraps
from typing import TYPE_CHECKING, Any, Generator, Iterable, cast, overload

import attr
import pendulum
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    PrimaryKeyConstraint,
    String,
    text,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query, Session, reconstructor, relationship
from sqlalchemy.orm.exc import NoResultFound

from airflow import settings
from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.helpers import exactly_one, is_container
from airflow.utils.json import XComDecoder, XComEncoder
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)

# MAX XCOM Size is 48KB
# https://github.com/apache/airflow/pull/1618#discussion_r68249677
MAX_XCOM_SIZE = 49344
XCOM_RETURN_KEY = "return_value"

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


class BaseXCom(Base, LoggingMixin):
    """Base class for XCom objects."""

    __tablename__ = "xcom"

    dag_run_id = Column(Integer(), nullable=False, primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False, primary_key=True)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    key = Column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)

    # Denormalized for easier lookup.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)

    value = Column(LargeBinary)
    timestamp = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __table_args__ = (
        # Ideally we should create a unique index over (key, dag_id, task_id, run_id),
        # but it goes over MySQL's index length limit. So we instead index 'key'
        # separately, and enforce uniqueness with DagRun.id instead.
        Index("idx_xcom_key", key),
        Index("idx_xcom_task_instance", dag_id, task_id, run_id, map_index),
        PrimaryKeyConstraint(
            "dag_run_id", "task_id", "map_index", "key", name="xcom_pkey", mssql_clustered=True
        ),
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
        Called by the ORM after the instance has been loaded from the DB or otherwise reconstituted
        i.e automatically deserialize Xcom value when loading from DB.
        """
        self.value = self.orm_deserialize_value()

    def __repr__(self):
        if self.map_index < 0:
            return f'<XCom "{self.key}" ({self.task_id} @ {self.run_id})>'
        return f'<XCom "{self.key}" ({self.task_id}[{self.map_index}] @ {self.run_id})>'

    @overload
    @classmethod
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
        """Store an XCom value.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

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

    @overload
    @classmethod
    def set(
        cls,
        key: str,
        value: Any,
        task_id: str,
        dag_id: str,
        execution_date: datetime.datetime,
        session: Session = NEW_SESSION,
    ) -> None:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def set(
        cls,
        key: str,
        value: Any,
        task_id: str,
        dag_id: str,
        execution_date: datetime.datetime | None = None,
        session: Session = NEW_SESSION,
        *,
        run_id: str | None = None,
        map_index: int = -1,
    ) -> None:
        """:sphinx-autoapi-skip:"""
        from airflow.models.dagrun import DagRun

        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError(
                f"Exactly one of run_id or execution_date must be passed. "
                f"Passed execution_date={execution_date}, run_id={run_id}"
            )

        if run_id is None:
            message = "Passing 'execution_date' to 'XCom.set()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, RemovedInAirflow3Warning, stacklevel=3)
            try:
                dag_run_id, run_id = (
                    session.query(DagRun.id, DagRun.run_id)
                    .filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date)
                    .one()
                )
            except NoResultFound:
                raise ValueError(f"DAG run not found on DAG {dag_id!r} at {execution_date}") from None
        else:
            dag_run_id = session.query(DagRun.id).filter_by(dag_id=dag_id, run_id=run_id).scalar()
            if dag_run_id is None:
                raise ValueError(f"DAG run not found on DAG {dag_id!r} with ID {run_id!r}")

        # Seamlessly resolve LazyXComAccess to a list. This is intended to work
        # as a "lazy list" to avoid pulling a ton of XComs unnecessarily, but if
        # it's pushed into XCom, the user should be aware of the performance
        # implications, and this avoids leaking the implementation detail.
        if isinstance(value, LazyXComAccess):
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
                run_id or execution_date,
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
        session.query(cls).filter(
            cls.key == key,
            cls.run_id == run_id,
            cls.task_id == task_id,
            cls.dag_id == dag_id,
            cls.map_index == map_index,
        ).delete()
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

    @classmethod
    @provide_session
    def get_value(
        cls,
        *,
        ti_key: TaskInstanceKey,
        key: str | None = None,
        session: Session = NEW_SESSION,
    ) -> Any:
        """Retrieve an XCom value for a task instance.

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
        return cls.get_one(
            key=key,
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            map_index=ti_key.map_index,
            session=session,
        )

    @overload
    @classmethod
    def get_one(
        cls,
        *,
        key: str | None = None,
        dag_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
        session: Session = NEW_SESSION,
    ) -> Any | None:
        """Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

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

    @overload
    @classmethod
    def get_one(
        cls,
        execution_date: datetime.datetime,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
    ) -> Any | None:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def get_one(
        cls,
        execution_date: datetime.datetime | None = None,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> Any | None:
        """:sphinx-autoapi-skip:"""
        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError("Exactly one of ti_key, run_id, or execution_date must be passed")

        if run_id:
            query = cls.get_many(
                run_id=run_id,
                key=key,
                task_ids=task_id,
                dag_ids=dag_id,
                map_indexes=map_index,
                include_prior_dates=include_prior_dates,
                limit=1,
                session=session,
            )
        elif execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.get_one()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, RemovedInAirflow3Warning, stacklevel=3)

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RemovedInAirflow3Warning)
                query = cls.get_many(
                    execution_date=execution_date,
                    key=key,
                    task_ids=task_id,
                    dag_ids=dag_id,
                    map_indexes=map_index,
                    include_prior_dates=include_prior_dates,
                    limit=1,
                    session=session,
                )
        else:
            raise RuntimeError("Should not happen?")

        result = query.with_entities(cls.value).first()
        if result:
            return cls.deserialize_value(result)
        return None

    @overload
    @classmethod
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
        """Composes a query to get one or more XCom entries.

        This function returns an SQLAlchemy query of full XCom objects. If you
        just want one stored value, use :meth:`get_one` instead.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

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
        """

    @overload
    @classmethod
    def get_many(
        cls,
        execution_date: datetime.datetime,
        key: str | None = None,
        task_ids: str | Iterable[str] | None = None,
        dag_ids: str | Iterable[str] | None = None,
        map_indexes: int | Iterable[int] | None = None,
        include_prior_dates: bool = False,
        limit: int | None = None,
        session: Session = NEW_SESSION,
    ) -> Query:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def get_many(
        cls,
        execution_date: datetime.datetime | None = None,
        key: str | None = None,
        task_ids: str | Iterable[str] | None = None,
        dag_ids: str | Iterable[str] | None = None,
        map_indexes: int | Iterable[int] | None = None,
        include_prior_dates: bool = False,
        limit: int | None = None,
        session: Session = NEW_SESSION,
        *,
        run_id: str | None = None,
    ) -> Query:
        """:sphinx-autoapi-skip:"""
        from airflow.models.dagrun import DagRun

        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError(
                f"Exactly one of run_id or execution_date must be passed. "
                f"Passed execution_date={execution_date}, run_id={run_id}"
            )
        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.get_many()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, RemovedInAirflow3Warning, stacklevel=3)

        query = session.query(cls).join(cls.dag_run)

        if key:
            query = query.filter(cls.key == key)

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
            if execution_date is not None:
                query = query.filter(DagRun.execution_date <= execution_date)
            else:
                dr = session.query(DagRun.execution_date).filter(DagRun.run_id == run_id).subquery()
                query = query.filter(cls.execution_date <= dr.c.execution_date)
        elif execution_date is not None:
            query = query.filter(DagRun.execution_date == execution_date)
        else:
            query = query.filter(cls.run_id == run_id)

        query = query.order_by(DagRun.execution_date.desc(), cls.timestamp.desc())
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
            session.delete(xcom)
        session.commit()

    @overload
    @classmethod
    def clear(
        cls,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """Clear all XCom data from the database for the given task instance.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

        :param dag_id: ID of DAG to clear the XCom for.
        :param task_id: ID of task to clear the XCom for.
        :param run_id: ID of DAG run to clear the XCom for.
        :param map_index: If given, only clear XCom from this particular mapped
            task. The default ``None`` clears *all* XComs from the task.
        :param session: Database session. If not given, a new session will be
            created for this function.
        """

    @overload
    @classmethod
    def clear(
        cls,
        execution_date: pendulum.DateTime,
        dag_id: str,
        task_id: str,
        session: Session = NEW_SESSION,
    ) -> None:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def clear(
        cls,
        execution_date: pendulum.DateTime | None = None,
        dag_id: str | None = None,
        task_id: str | None = None,
        session: Session = NEW_SESSION,
        *,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> None:
        """:sphinx-autoapi-skip:"""
        from airflow.models import DagRun

        # Given the historic order of this function (execution_date was first argument) to add a new optional
        # param we need to add default values for everything :(
        if dag_id is None:
            raise TypeError("clear() missing required argument: dag_id")
        if task_id is None:
            raise TypeError("clear() missing required argument: task_id")

        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError(
                f"Exactly one of run_id or execution_date must be passed. "
                f"Passed execution_date={execution_date}, run_id={run_id}"
            )

        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.clear()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, RemovedInAirflow3Warning, stacklevel=3)
            run_id = (
                session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date)
                .scalar()
            )

        query = session.query(cls).filter_by(dag_id=dag_id, task_id=task_id, run_id=run_id)
        if map_index is not None:
            query = query.filter_by(map_index=map_index)
        query.delete()

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
            try:
                return json.loads(result.value.decode("UTF-8"), cls=XComDecoder, object_hook=object_hook)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return pickle.loads(result.value)

    @staticmethod
    def deserialize_value(result: XCom) -> Any:
        """Deserialize XCom value from str or pickle object"""
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


class _LazyXComAccessIterator(collections.abc.Iterator):
    def __init__(self, cm: contextlib.AbstractContextManager[Query]) -> None:
        self._cm = cm
        self._entered = False

    def __del__(self) -> None:
        if self._entered:
            self._cm.__exit__(None, None, None)

    def __iter__(self) -> collections.abc.Iterator:
        return self

    def __next__(self) -> Any:
        return XCom.deserialize_value(next(self._it))

    @cached_property
    def _it(self) -> collections.abc.Iterator:
        self._entered = True
        return iter(self._cm.__enter__())


@attr.define(slots=True)
class LazyXComAccess(collections.abc.Sequence):
    """Wrapper to lazily pull XCom with a sequence-like interface.

    Note that since the session bound to the parent query may have died when we
    actually access the sequence's content, we must create a new session
    for every function call with ``with_session()``.

    :meta private:
    """

    _query: Query
    _len: int | None = attr.ib(init=False, default=None)

    @classmethod
    def build_from_xcom_query(cls, query: Query) -> LazyXComAccess:
        return cls(query=query.with_entities(XCom.value))

    def __repr__(self) -> str:
        return f"LazyXComAccess([{len(self)} items])"

    def __str__(self) -> str:
        return str(list(self))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, (list, LazyXComAccess)):
            z = itertools.zip_longest(iter(self), iter(other), fillvalue=object())
            return all(x == y for x, y in z)
        return NotImplemented

    def __getstate__(self) -> Any:
        # We don't want to go to the trouble of serializing the entire Query
        # object, including its filters, hints, etc. (plus SQLAlchemy does not
        # provide a public API to inspect a query's contents). Converting the
        # query into a SQL string is the best we can get. Theoratically we can
        # do the same for count(), but I think it should be performant enough to
        # calculate only that eagerly.
        with self._get_bound_query() as query:
            statement = query.statement.compile(query.session.get_bind())
            return (str(statement), query.count())

    def __setstate__(self, state: Any) -> None:
        statement, self._len = state
        self._query = Query(XCom.value).from_statement(text(statement))

    def __len__(self):
        if self._len is None:
            with self._get_bound_query() as query:
                self._len = query.count()
        return self._len

    def __iter__(self):
        return _LazyXComAccessIterator(self._get_bound_query())

    def __getitem__(self, key):
        if not isinstance(key, int):
            raise ValueError("only support index access for now")
        try:
            with self._get_bound_query() as query:
                r = query.offset(key).limit(1).one()
        except NoResultFound:
            raise IndexError(key) from None
        return XCom.deserialize_value(r)

    @contextlib.contextmanager
    def _get_bound_query(self) -> Generator[Query, None, None]:
        # Do we have a valid session already?
        if self._query.session and self._query.session.is_active:
            yield self._query
            return

        session = settings.Session()
        try:
            yield self._query.with_session(session)
        finally:
            session.close()


def _patch_outdated_serializer(clazz: type[BaseXCom], params: Iterable[str]) -> None:
    """Patch a custom ``serialize_value`` to accept the modern signature.

    To give custom XCom backends more flexibility with how they store values, we
    now forward all params passed to ``XCom.set`` to ``XCom.serialize_value``.
    In order to maintain compatibility with custom XCom backends written with
    the old signature, we check the signature and, if necessary, patch with a
    method that ignores kwargs the backend does not accept.
    """
    old_serializer = clazz.serialize_value

    @wraps(old_serializer)
    def _shim(**kwargs):
        kwargs = {k: kwargs.get(k) for k in params}
        warnings.warn(
            f"Method `serialize_value` in XCom backend {XCom.__name__} is using outdated signature and"
            f"must be updated to accept all params in `BaseXCom.set` except `session`. Support will be "
            f"removed in a future release.",
            RemovedInAirflow3Warning,
        )
        return old_serializer(**kwargs)

    clazz.serialize_value = _shim  # type: ignore[assignment]


def _get_function_params(function) -> list[str]:
    """
    Returns the list of variables names of a function

    :param function: The function to inspect
    """
    parameters = inspect.signature(function).parameters
    bound_arguments = [
        name for name, p in parameters.items() if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    ]
    return bound_arguments


def resolve_xcom_backend() -> type[BaseXCom]:
    """Resolves custom XCom class

    Confirms that custom XCom class extends the BaseXCom.
    Compares the function signature of the custom XCom serialize_value to the base XCom serialize_value.
    """
    clazz = conf.getimport("core", "xcom_backend", fallback=f"airflow.models.xcom.{BaseXCom.__name__}")
    if not clazz:
        return BaseXCom
    if not issubclass(clazz, BaseXCom):
        raise TypeError(
            f"Your custom XCom class `{clazz.__name__}` is not a subclass of `{BaseXCom.__name__}`."
        )
    base_xcom_params = _get_function_params(BaseXCom.serialize_value)
    xcom_params = _get_function_params(clazz.serialize_value)
    if not set(base_xcom_params) == set(xcom_params):
        _patch_outdated_serializer(clazz=clazz, params=xcom_params)
    return clazz


if TYPE_CHECKING:
    XCom = BaseXCom  # Hack to avoid Mypy "Variable 'XCom' is not valid as a type".
else:
    XCom = resolve_xcom_backend()
