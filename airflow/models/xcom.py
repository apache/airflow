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

import json
import logging
import pickle
from json import JSONDecodeError
from typing import Any, Iterable, Optional, Union

import pendulum
from sqlalchemy import Column, LargeBinary, String, and_
from sqlalchemy.orm import Query, Session, reconstructor

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

    @reconstructor
    def init_on_load(self):
        """
        Called by the ORM after the instance has been loaded from the DB or otherwise reconstituted
        i.e automatically deserialize Xcom value when loading from DB.
        """
        try:
            self.value = XCom.deserialize_value(self)
        except (UnicodeEncodeError, ValueError):
            # For backward-compatibility.
            # Preventing errors in webserver
            # due to XComs mixed with pickled and unpickled.
            self.value = pickle.loads(self.value)

    def __repr__(self):
        return '<XCom "{key}" ({task_id} @ {execution_date})>'.format(
            key=self.key, task_id=self.task_id, execution_date=self.execution_date
        )

    @classmethod
    @provide_session
    def set(cls, key, value, execution_date, task_id, dag_id, session=None):
        """
        Store an XCom value.

        :return: None
        """
        session.expunge_all()

        value = XCom.serialize_value(value)

        # remove any duplicate XComs
        session.query(cls).filter(
            cls.key == key, cls.execution_date == execution_date, cls.task_id == task_id, cls.dag_id == dag_id
        ).delete()

        session.commit()

        # insert new XCom
        session.add(XCom(key=key, value=value, execution_date=execution_date, task_id=task_id, dag_id=dag_id))

        session.commit()

    @classmethod
    @provide_session
    def get_one(
        cls,
        execution_date: pendulum.DateTime,
        key: Optional[str] = None,
        task_id: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        session: Session = None,
    ) -> Optional[Any]:
        """
        Retrieve an XCom value, optionally meeting certain criteria. Returns None
        of there are no results.

        :param execution_date: Execution date for the task
        :type execution_date: pendulum.datetime
        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. To remove the filter, pass key=None.
        :type key: str
        :param task_id: Only XComs from task with matching id will be
            pulled. Can pass None to remove the filter.
        :type task_id: str
        :param dag_id: If provided, only pulls XCom from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: str
        :param include_prior_dates: If False, only XCom from the current
            execution_date are returned. If True, XCom from previous dates
            are returned as well.
        :type include_prior_dates: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        result = cls.get_many(
            execution_date=execution_date,
            key=key,
            task_ids=task_id,
            dag_ids=dag_id,
            include_prior_dates=include_prior_dates,
            session=session,
        ).first()
        if result:
            return result.value
        return None

    @classmethod
    @provide_session
    def get_many(
        cls,
        execution_date: pendulum.DateTime,
        key: Optional[str] = None,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_ids: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
        session: Session = None,
    ) -> Query:
        """
        Composes a query to get one or more values from the xcom table.

        :param execution_date: Execution date for the task
        :type execution_date: pendulum.datetime
        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. To remove the filter, pass key=None.
        :type key: str
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_ids: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_ids: str
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        :param limit: If required, limit the number of returned objects.
            XCom objects can be quite big and you might want to limit the
            number of rows.
        :type limit: int
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
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
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
        )

        if limit:
            return query.limit(limit)
        else:
            return query

    @classmethod
    @provide_session
    def delete(cls, xcoms, session=None):
        """Delete Xcom"""
        if isinstance(xcoms, XCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, XCom):
                raise TypeError(f'Expected XCom; received {xcom.__class__.__name__}')
            session.delete(xcom)
        session.commit()

    @staticmethod
    def serialize_value(value: Any):
        """Serialize Xcom value to str or pickled object"""
        if conf.getboolean('core', 'enable_xcom_pickling'):
            return pickle.dumps(value)
        try:
            return json.dumps(value).encode('UTF-8')
        except (ValueError, TypeError):
            log.error(
                "Could not serialize the XCOM value into JSON. "
                "If you are using pickles instead of JSON "
                "for XCOM, then you need to enable pickle "
                "support for XCOM in your airflow config."
            )
            raise

    @staticmethod
    def deserialize_value(result) -> Any:
        """Deserialize Xcom value from str or pickle object"""
        enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
        if enable_pickling:
            return pickle.loads(result.value)
        try:
            return json.loads(result.value.decode('UTF-8'))
        except JSONDecodeError:
            log.error(
                "Could not deserialize the XCOM value from JSON. "
                "If you are using pickles instead of JSON "
                "for XCOM, then you need to enable pickle "
                "support for XCOM in your airflow config."
            )
            raise


def resolve_xcom_backend():
    """Resolves custom XCom class"""
    clazz = conf.getimport("core", "xcom_backend", fallback=f"airflow.models.xcom.{BaseXCom.__name__}")
    if clazz:
        if not issubclass(clazz, BaseXCom):
            raise TypeError(
                f"Your custom XCom class `{clazz.__name__}` is not a subclass of `{BaseXCom.__name__}`."
            )
        return clazz
    return BaseXCom


XCom = resolve_xcom_backend()
