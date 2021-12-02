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
import json
import logging
import pickle
import warnings
from typing import TYPE_CHECKING, Any, Iterable, Optional, Type, Union, overload

import pendulum
from sqlalchemy import Column, LargeBinary, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query, Session, reconstructor, relationship

from airflow.configuration import conf
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)

# MAX XCOM Size is 48KB
# https://github.com/apache/airflow/pull/1618#discussion_r68249677
MAX_XCOM_SIZE = 49344
XCOM_RETURN_KEY = 'return_value'

# Work around 'airflow task test' generating a temporary in-memory DAG run
# without storing it in the database. To avoid interfering with actual XCom
# entries but still behave _somewhat_ consistently, we store XCom to a distant
# time in the future. Eventually we want to migrate XCom's primary to use run_id
# instead, so execution_date can just be None for this case.
IN_MEMORY_DAGRUN_ID = "__airflow_in_memory_dagrun__"

# This is the largest possible value we can store in MySQL.
# https://dev.mysql.com/doc/refman/5.7/en/datetime.html
_DISTANT_FUTURE = datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc)


class BaseXCom(Base, LoggingMixin):
    """Base class for XCom objects."""

    __tablename__ = "xcom"

    key = Column(String(512, **COLLATION_ARGS), primary_key=True)
    value = Column(LargeBinary)
    timestamp = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    execution_date = Column(UtcDateTime, primary_key=True)

    # source information
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)

    # For _now_, we link this via execution_date, in 2.3 we will migrate this table to use run_id too
    dag_run = relationship(
        "DagRun",
        primaryjoin="""and_(
            BaseXCom.dag_id == foreign(DagRun.dag_id),
            BaseXCom.execution_date == foreign(DagRun.execution_date)
        )""",
        uselist=False,
        passive_deletes="all",
    )
    run_id = association_proxy("dag_run", "run_id")

    @reconstructor
    def init_on_load(self):
        """
        Called by the ORM after the instance has been loaded from the DB or otherwise reconstituted
        i.e automatically deserialize Xcom value when loading from DB.
        """
        self.value = self.orm_deserialize_value()

    def __repr__(self):
        return f'<XCom "{self.key}" ({self.task_id} @ {self.execution_date})>'

    @overload
    @classmethod
    def set(
        cls,
        key: str,
        value: Any,
        *,
        task_id: str,
        dag_id: str,
        run_id: str,
        session: Optional[Session] = None,
    ) -> None:
        ...

    @overload
    @classmethod
    def set(
        cls,
        key: str,
        value: Any,
        task_id: str,
        dag_id: str,
        execution_date: datetime.datetime,
        session: Optional[Session] = None,
    ) -> None:
        ...

    @classmethod
    @provide_session
    def set(
        cls,
        key: str,
        value: Any,
        task_id: str,
        dag_id: str,
        execution_date: Optional[datetime.datetime] = None,
        *,
        run_id: Optional[str] = None,
        session: Session,
    ) -> None:
        """Store an XCom value.

        :param key: Key to store the value under.
        :param value: Value to store. What types are possible depends on whether
            ``enable_xcom_pickling`` is true or not. If so, this can be any
            picklable object; only be JSON-serializable may be used otherwise.
        :param task_id: Task ID of the task.
        :param dag_id: DAG ID of the task.
        :param run_id: DAG run ID of the task.
        :param execution_date: Execution date of the task. Deprecated; use
            ``run_id`` instead.
        :type execution_date: datetime
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")

        if run_id == IN_MEMORY_DAGRUN_ID:
            execution_date = _DISTANT_FUTURE
        elif run_id is not None:
            from airflow.models.dagrun import DagRun

            execution_date = (
                session.query(DagRun.execution_date)
                .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
                .scalar()
            )
        else:  # Guarantees execution_date is not None.
            message = "Passing 'execution_date' to 'XCom.set()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, DeprecationWarning, stacklevel=3)

        # Remove duplicate XComs and insert a new one.
        session.query(cls).filter(
            cls.key == key,
            cls.execution_date == execution_date,
            cls.task_id == task_id,
            cls.dag_id == dag_id,
        ).delete()
        klass: Any = cls  # Work around Mypy complaining SQLAlchemy model init kawrgs.
        new = klass(
            key=key,
            value=cls.serialize_value(value),
            execution_date=execution_date,
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
        run_id: str,
        key: Optional[str] = None,
        task_id: Optional[str] = None,
        dag_id: Optional[str] = None,
        include_prior_dates: bool = False,
        session: Optional[Session] = None,
    ) -> Optional[Any]:
        ...

    @overload
    @classmethod
    def get_one(
        cls,
        execution_date: pendulum.DateTime,
        key: Optional[str] = None,
        task_id: Optional[str] = None,
        dag_id: Optional[str] = None,
        include_prior_dates: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> Optional[Any]:
        ...

    @classmethod
    @provide_session
    def get_one(
        cls,
        execution_date: Optional[pendulum.DateTime] = None,
        run_id: Optional[str] = None,
        key: Optional[str] = None,
        task_id: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        session: Session = None,
    ) -> Optional[Any]:
        """Retrieve an XCom value, optionally meeting certain criteria.

        If there are no matching results, *None* is returned.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        :param run_id: DAG run ID of the task.
        :type run_id: str
        :param execution_date: Execution date of the task. Deprecated; use
            ``run_id`` instead.
        :type execution_date: pendulum.DateTime
        :param key: If provided, only XComs matching the key will be returned.
            Pass *None* to remove the filter.
        :type key: str | None
        :param task_id: If provided, only pulls XComs from task(s) with matching
            ID. Pass *None* to remove the filter.
        :type task_id: str | None
        :param dag_id: If provided, only pulls XCom from this DAG.
            If *None* (default), the DAG of the calling task is used.
        :type dag_id: str | None
        :param include_prior_dates: If *False* (default), only XCom values from
            the current DAG run are returned. If *True*, XCom values from
            previous DAG runs are returned as well.
        :type include_prior_dates: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")

        if run_id is not None:
            query = cls.get_many(
                run_id=run_id,
                key=key,
                task_ids=task_id,
                dag_ids=dag_id,
                include_prior_dates=include_prior_dates,
                session=session,
            )
        elif execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.get_one()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, PendingDeprecationWarning, stacklevel=3)

            query = cls.get_many(
                execution_date=execution_date,
                key=key,
                task_ids=task_id,
                dag_ids=dag_id,
                include_prior_dates=include_prior_dates,
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
        session: Optional[Session] = None,
    ) -> Query:
        ...

    @overload
    @classmethod
    def get_many(
        cls,
        execution_date: pendulum.DateTime,
        key: Optional[str] = None,
        task_ids: Union[str, Iterable[str], None] = None,
        dag_ids: Union[str, Iterable[str], None] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
        *,
        session: Optional[Session] = None,
    ) -> Query:
        ...

    @classmethod
    @provide_session
    def get_many(
        cls,
        execution_date: Optional[pendulum.DateTime] = None,
        run_id: Optional[str] = None,
        key: Optional[str] = None,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_ids: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
        *,
        session: Session,
    ) -> Query:
        """Composes a query to get XCom values.

        An iterable of full XCom objects are returned. If you just want one
        stored value, use :meth:`get_one` instead. The built-in XCom backend
        implements this to return a SQLAlchemy query.

        :param run_id: DAG run ID of the task.
        :type run_id: str
        :param execution_date: Execution date of the task. Deprecated; use
            ``run_id`` instead.
        :type execution_date: pendulum.DateTime
        :param key: If provided, only XComs with matching keys will be returned.
            Pass *None* to remove the filter.
        :type key: str | list[str] | None
        :param task_ids: If provided, only pulls XComs from task(s) with matching
            ID. Pass *None* to remove the filter.
        :type task_ids: str | list[str] | None
        :param dag_ids: If provided, only pulls XCom from specified DAG(s).
            If *None* (default), the DAG of the calling task is used.
        :type dag_ids: str | list[str] | None
        :param include_prior_dates: If *False*, only XCom values from the
            current DAG run are returned. If True, XCom values from previous
            DAG runs are returned as well.
        :type include_prior_dates: bool
        :param limit: If provided, limit the number of returned objects.
            XCom objects can be quite big and you might want to limit the
            number of rows.
        :type limit: int
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        from airflow.models.dagrun import DagRun

        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")
        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.get_many()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, PendingDeprecationWarning, stacklevel=3)

        query = session.query(cls)

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
                query = query.filter(cls.execution_date <= execution_date)
            else:
                # This returns an empty query result for IN_MEMORY_DAGRUN_ID,
                # but that is impossible to implement. Sorry?
                dr = session.query(DagRun.execution_date).filter(DagRun.run_id == run_id).subquery()
                query = query.filter(cls.execution_date <= dr.c.execution_date)
        elif execution_date is not None:
            query = query.filter(cls.execution_date == execution_date)
        elif run_id == IN_MEMORY_DAGRUN_ID:
            query = query.filter(cls.execution_date == _DISTANT_FUTURE)
        else:
            query = query.join(cls.dag_run).filter(DagRun.run_id == run_id)

        query = query.order_by(cls.execution_date.desc(), cls.timestamp.desc())
        if limit:
            return query.limit(limit)
        return query

    @classmethod
    @provide_session
    def delete(cls, xcoms, session=None):
        """Delete an XCom."""
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
        ...

    @overload
    @classmethod
    def clear(
        cls,
        execution_date: pendulum.DateTime,
        dag_id: str,
        task_id: str,
        session: Optional[Session] = None,
    ) -> None:
        ...

    @classmethod
    @provide_session
    def clear(
        cls,
        execution_date: Optional[pendulum.DateTime] = None,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        session: Session,
    ) -> None:
        """Clears XCom data for the task instance.

        :param run_id: DAG run ID of the task.
        :type run_id: str
        :param execution_date: Execution date of the task. Deprecated; use
            ``run_id`` instead.
        :type execution_date: pendulum.DateTime
        :param task_id: Clears XComs from task with matching ID.
        :type task_ids: str
        :param dag_id: Clears XCom from this DAG.
        :type dag_id: str
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        # Given the historic order of this function (execution_date was first
        # argument), we need to add default values for everything to add a new
        # optional parameter :(
        if not dag_id:
            raise TypeError("clear() missing required argument: dag_id")
        if not task_id:
            raise TypeError("clear() missing required argument: task_id")

        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")

        query = session.query(cls).filter(
            cls.dag_id == dag_id,
            cls.task_id == task_id,
        )

        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.clear()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, DeprecationWarning, stacklevel=3)
            query = query.filter(cls.execution_date == execution_date)
        else:
            from airflow.models.dagrun import DagRun

            query = query.join(cls.dag_run).filter(DagRun.run_id == run_id)

        return query.delete()

    @staticmethod
    def serialize_value(value: Any):
        """Serialize Xcom value to str or pickled object"""
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


def resolve_xcom_backend() -> Type[BaseXCom]:
    """Resolves custom XCom class"""
    clazz = conf.getimport("core", "xcom_backend", fallback=f"airflow.models.xcom.{BaseXCom.__name__}")
    if clazz:
        if not issubclass(clazz, BaseXCom):
            raise TypeError(
                f"Your custom XCom class `{clazz.__name__}` is not a subclass of `{BaseXCom.__name__}`."
            )
        return clazz
    return BaseXCom


if TYPE_CHECKING:
    XCom = BaseXCom  # Hack to avoid Mypy "Variable is not valid as a type".
else:
    XCom = resolve_xcom_backend()
