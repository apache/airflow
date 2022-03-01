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

import datetime
import inspect
import json
import logging
import pickle
import warnings
from functools import wraps
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Type, Union, cast, overload

import pendulum
from sqlalchemy import Column, Index, Integer, LargeBinary, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query, Session, reconstructor, relationship
from sqlalchemy.orm.exc import NoResultFound

from airflow.configuration import conf
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.helpers import exactly_one, is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)

# MAX XCOM Size is 48KB
# https://github.com/apache/airflow/pull/1618#discussion_r68249677
MAX_XCOM_SIZE = 49344
XCOM_RETURN_KEY = 'return_value'

# Stand-in value for 'airflow task test' generating a temporary in-memory DAG
# run without storing it in the database.
IN_MEMORY_RUN_ID = "__airflow_in_memory_dagrun__"

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


class BaseXCom(Base, LoggingMixin):
    """Base class for XCom objects."""

    __tablename__ = "xcom"

    dag_run_id = Column(Integer(), nullable=False, primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False, primary_key=True)
    key = Column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)

    # Denormalized for easier lookup.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)

    value = Column(LargeBinary)
    timestamp = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    dag_run = relationship(
        "DagRun",
        primaryjoin="""and_(
            BaseXCom.dag_id == foreign(DagRun.dag_id),
            BaseXCom.run_id == foreign(DagRun.run_id),
        )""",
        uselist=False,
        lazy="joined",
        passive_deletes="all",
    )
    execution_date = association_proxy("dag_run", "execution_date")

    __table_args__ = (
        # Ideally we should create a unique index over (key, dag_id, task_id, run_id),
        # but it goes over MySQL's index length limit. So we instead create indexes
        # separately, and enforce uniqueness with DagRun.id instead.
        Index("idx_xcom_key", key),
        Index("idx_xcom_ti_id", dag_id, task_id, run_id),
    )

    @reconstructor
    def init_on_load(self):
        """
        Called by the ORM after the instance has been loaded from the DB or otherwise reconstituted
        i.e automatically deserialize Xcom value when loading from DB.
        """
        self.value = self.orm_deserialize_value()

    def __repr__(self):
        return f'<XCom "{self.key}" ({self.task_id} @ {self.run_id})>'

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
        execution_date: Optional[datetime.datetime] = None,
        session: Session = NEW_SESSION,
        *,
        run_id: Optional[str] = None,
    ) -> None:
        """:sphinx-autoapi-skip:"""
        from airflow.models.dagrun import DagRun

        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError("Exactly one of run_id or execution_date must be passed")

        if run_id is None:
            message = "Passing 'execution_date' to 'XCom.set()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, DeprecationWarning, stacklevel=3)
            try:
                dag_run_id, run_id = (
                    session.query(DagRun.id, DagRun.run_id)
                    .filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date)
                    .one()
                )
            except NoResultFound:
                raise ValueError(f"DAG run not found on DAG {dag_id!r} at {execution_date}") from None
        elif run_id == IN_MEMORY_RUN_ID:
            dag_run_id = -1
        else:
            dag_run_id = session.query(DagRun.id).filter_by(dag_id=dag_id, run_id=run_id).scalar()
            if dag_run_id is None:
                raise ValueError(f"DAG run not found on DAG {dag_id!r} with ID {run_id!r}")

        value = cls.serialize_value(
            value=value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
        )

        # Remove duplicate XComs and insert a new one.
        session.query(cls).filter(
            cls.key == key,
            cls.run_id == run_id,
            cls.task_id == task_id,
            cls.dag_id == dag_id,
        ).delete()
        new = cast(Any, cls)(  # Work around Mypy complaining model not defining '__init__'.
            dag_run_id=dag_run_id,
            key=key,
            value=value,
            run_id=run_id,
            task_id=task_id,
            dag_id=dag_id,
        )
        session.add(new)
        session.flush()

    @overload
    @classmethod
    def get_one(
        cls,
        *,
        key: Optional[str] = None,
        ti_key: "TaskInstanceKey",
        session: Session = NEW_SESSION,
    ) -> Optional[Any]:
        """Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        If there are no results, *None* is returned.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

        :param ti_key: The TaskInstanceKey to look up the XCom for
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
        *,
        key: Optional[str] = None,
        task_id: str,
        dag_id: str,
        run_id: str,
        session: Session = NEW_SESSION,
    ) -> Optional[Any]:
        ...

    @overload
    @classmethod
    def get_one(
        cls,
        execution_date: datetime.datetime,
        key: Optional[str] = None,
        task_id: Optional[str] = None,
        dag_id: Optional[str] = None,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
    ) -> Optional[Any]:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def get_one(
        cls,
        execution_date: Optional[datetime.datetime] = None,
        key: Optional[str] = None,
        task_id: Optional[str] = None,
        dag_id: Optional[str] = None,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        run_id: Optional[str] = None,
        ti_key: Optional["TaskInstanceKey"] = None,
    ) -> Optional[Any]:
        """:sphinx-autoapi-skip:"""
        if not exactly_one(execution_date is not None, ti_key is not None, run_id is not None):
            raise ValueError("Exactly one of ti_key, run_id, or execution_date must be passed")

        if ti_key is not None:
            query = session.query(cls).filter_by(
                dag_id=ti_key.dag_id,
                run_id=ti_key.run_id,
                task_id=ti_key.task_id,
            )
            if key:
                query = query.filter_by(key=key)
            query = query.limit(1)
        elif run_id:
            query = cls.get_many(
                run_id=run_id,
                key=key,
                task_ids=task_id,
                dag_ids=dag_id,
                include_prior_dates=include_prior_dates,
                limit=1,
                session=session,
            )
        elif execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.get_one()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, PendingDeprecationWarning, stacklevel=3)

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                query = cls.get_many(
                    execution_date=execution_date,
                    key=key,
                    task_ids=task_id,
                    dag_ids=dag_id,
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
        key: Optional[str] = None,
        task_ids: Union[str, Iterable[str], None] = None,
        dag_ids: Union[str, Iterable[str], None] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
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
        :param dag_id: Only pulls XComs from this DAG. If *None* (default), the
            DAG of the calling task is used.
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
        key: Optional[str] = None,
        task_ids: Union[str, Iterable[str], None] = None,
        dag_ids: Union[str, Iterable[str], None] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
        session: Session = NEW_SESSION,
    ) -> Query:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def get_many(
        cls,
        execution_date: Optional[datetime.datetime] = None,
        key: Optional[str] = None,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_ids: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
        session: Session = NEW_SESSION,
        *,
        run_id: Optional[str] = None,
    ) -> Query:
        """:sphinx-autoapi-skip:"""
        from airflow.models.dagrun import DagRun

        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError("Exactly one of run_id or execution_date must be passed")
        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.get_many()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, PendingDeprecationWarning, stacklevel=3)

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

        if include_prior_dates:
            if execution_date is not None:
                query = query.filter(DagRun.execution_date <= execution_date)
            else:
                # This returns an empty query result for IN_MEMORY_RUN_ID,
                # but that is impossible to implement. Sorry?
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
    def delete(cls, xcoms: Union["XCom", Iterable["XCom"]], session: Session) -> None:
        """Delete one or multiple XCom entries."""
        if isinstance(xcoms, XCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, XCom):
                raise TypeError(f'Expected XCom; received {xcom.__class__.__name__}')
            session.delete(xcom)
        session.commit()

    @overload
    @classmethod
    def clear(cls, *, dag_id: str, task_id: str, run_id: str, session: Optional[Session] = None) -> None:
        """Clear all XCom data from the database for the given task instance.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

        :param dag_id: ID of DAG to clear the XCom for.
        :param task_id: ID of task to clear the XCom for.
        :param run_id: ID of DAG run to clear the XCom for.
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
        execution_date: Optional[pendulum.DateTime] = None,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
        session: Session = NEW_SESSION,
        *,
        run_id: Optional[str] = None,
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
            raise ValueError("Exactly one of run_id or execution_date must be passed")

        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.clear()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, DeprecationWarning, stacklevel=3)
            run_id = (
                session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date)
                .scalar()
            )

        return session.query(cls).filter_by(dag_id=dag_id, task_id=task_id, run_id=run_id).delete()

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key=None,
        task_id=None,
        dag_id=None,
        run_id=None,
        mapping_index: int = -1,
    ):
        """Serialize XCom value to str or pickled object"""
        if conf.getboolean('core', 'enable_xcom_pickling'):
            return pickle.dumps(value)
        try:
            return json.dumps(value).encode('UTF-8')
        except (ValueError, TypeError):
            log.error(
                "Could not serialize the XCom value into JSON."
                " If you are using pickle instead of JSON for XCom,"
                " then you need to enable pickle support for XCom"
                " in your airflow config."
            )
            raise

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        """Deserialize XCom value from str or pickle object"""
        if result.value is None:
            return None
        if conf.getboolean('core', 'enable_xcom_pickling'):
            try:
                return pickle.loads(result.value)
            except pickle.UnpicklingError:
                return json.loads(result.value.decode('UTF-8'))
        else:
            try:
                return json.loads(result.value.decode('UTF-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return pickle.loads(result.value)

    def orm_deserialize_value(self) -> Any:
        """
        Deserialize method which is used to reconstruct ORM XCom object.

        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom orm model. This is used when viewing XCom listing
        in the webserver, for example.
        """
        return BaseXCom.deserialize_value(self)


def _patch_outdated_serializer(clazz, params):
    """
    Previously XCom.serialize_value only accepted one argument ``value``.  In order to give
    custom XCom backends more flexibility with how they store values we now forward to
    ``XCom.serialize_value`` all params passed to ``XCom.set``.  In order to maintain
    compatibility with XCom backends written with the old signature we check the signature
    and if necessary we patch with a method that ignores kwargs the backend does not accept.
    """
    old_serializer = clazz.serialize_value

    @wraps(old_serializer)
    def _shim(**kwargs):
        kwargs = {k: kwargs.get(k) for k in params}
        warnings.warn(
            f"Method `serialize_value` in XCom backend {XCom.__name__} is using outdated signature and"
            f"must be updated to accept all params in `BaseXCom.set` except `session`. Support will be "
            f"removed in a future release.",
            DeprecationWarning,
        )
        return old_serializer(**kwargs)

    clazz.serialize_value = _shim


def _get_function_params(function) -> List[str]:
    """
    Returns the list of variables names of a function

    :param function: The function to inspect
    :rtype: List[str]
    """
    parameters = inspect.signature(function).parameters
    bound_arguments = [
        name for name, p in parameters.items() if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    ]
    return bound_arguments


def resolve_xcom_backend() -> Type[BaseXCom]:
    """Resolves custom XCom class"""
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
