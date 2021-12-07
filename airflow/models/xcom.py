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
from typing import TYPE_CHECKING, Any, Iterable, Optional, Type, Union, cast, overload

import pendulum
from sqlalchemy import Column, LargeBinary, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query, Session, reconstructor, relationship

from airflow.configuration import conf
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)

# MAX XCOM Size is 48KB
# https://github.com/apache/airflow/pull/1618#discussion_r68249677
MAX_XCOM_SIZE = 49344
XCOM_RETURN_KEY = 'return_value'


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
        :type session: sqlalchemy.orm.session.Session
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
        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")

        if run_id:
            from airflow.models.dagrun import DagRun

            dag_run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one()
            execution_date = dag_run.execution_date

        # Remove duplicate XComs and insert a new one.
        session.query(cls).filter(
            cls.key == key,
            cls.execution_date == execution_date,
            cls.task_id == task_id,
            cls.dag_id == dag_id,
        ).delete()
        new = cast(Any, cls)(  # Work around Mypy complaining model not defining '__init__'.
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
        session: Session = NEW_SESSION,
    ) -> Optional[Any]:
        """Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        If there are no results, *None* is returned.

        A deprecated form of this function accepts ``execution_date`` instead of
        ``run_id``. The two arguments are mutually exclusive.

        :param run_id: DAG run ID for the task.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param task_id: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param dag_id: Only pull XCom from this DAG. If *None* (default), the
            DAG of the calling task is used.
        :param include_prior_dates: If *False* (default), only XCom from the
            specified DAG run is returned. If *True*, the latest matching XCom is
            returned regardless of the run it belongs to.
        :param session: Database session. If not given, a new session will be
            created for this function.
        :type session: sqlalchemy.orm.session.Session
        """

    @overload
    @classmethod
    def get_one(
        cls,
        execution_date: pendulum.DateTime,
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
        execution_date: Optional[pendulum.DateTime] = None,
        key: Optional[str] = None,
        task_id: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        run_id: Optional[str] = None,
    ) -> Optional[Any]:
        """:sphinx-autoapi-skip:"""
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
        :type session: sqlalchemy.orm.session.Session
        """

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
        session: Session = NEW_SESSION,
    ) -> Query:
        """:sphinx-autoapi-skip:"""

    @classmethod
    @provide_session
    def get_many(
        cls,
        execution_date: Optional[pendulum.DateTime] = None,
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
        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")

        filters = []

        if key:
            filters.append(cls.key == key)

        if task_ids:
            if is_container(task_ids):
                filters.append(cls.task_id.in_(task_ids))
            else:
                filters.append(cls.task_id == task_ids)

        if dag_ids:
            if is_container(dag_ids):
                filters.append(cls.dag_id.in_(dag_ids))
            else:
                filters.append(cls.dag_id == dag_ids)

        if include_prior_dates:
            if execution_date is None:
                # In theory it would be possible to build a subquery that joins to DagRun and then gets the
                # execution dates. Lets do that for 2.3
                raise ValueError("Using include_prior_dates needs an execution_date to be passed")
            filters.append(cls.execution_date <= execution_date)
        elif execution_date is not None:
            filters.append(cls.execution_date == execution_date)

        query = session.query(cls).filter(*filters)

        if run_id:
            from airflow.models.dagrun import DagRun

            query = query.join(cls.dag_run).filter(DagRun.run_id == run_id)

        query = query.order_by(cls.execution_date.desc(), cls.timestamp.desc())

        if limit:
            return query.limit(limit)
        else:
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
        :type session: sqlalchemy.orm.session.Session
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
        run_id: Optional[str] = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """:sphinx-autoapi-skip:"""
        # Given the historic order of this function (execution_date was first argument) to add a new optional
        # param we need to add default values for everything :(
        if dag_id is None:
            raise TypeError("clear() missing required argument: dag_id")
        if task_id is None:
            raise TypeError("clear() missing required argument: task_id")

        if not (execution_date is None) ^ (run_id is None):
            raise ValueError("Exactly one of execution_date or run_id must be passed")

        query = session.query(cls).filter(
            cls.dag_id == dag_id,
            cls.task_id == task_id,
        )

        if execution_date is not None:
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
    XCom = BaseXCom  # Hack to avoid Mypy "Variable 'XCom' is not valid as a type".
else:
    XCom = resolve_xcom_backend()
