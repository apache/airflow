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
import logging
from typing import Any, Iterable, Optional, Union

import pendulum
from sqlalchemy import Column, Integer, String, Text, and_
from sqlalchemy.orm import Query, Session

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils.helpers import is_container
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)


class EventNote(Base):
    """Model that stores a note for an event"""

    __tablename__ = "event_note"

    id = Column(Integer, primary_key=True)

    dag_id = Column(String(ID_LEN, **COLLATION_ARGS))
    task_id = Column(String(ID_LEN), nullable=True)
    execution_date = Column(UtcDateTime, nullable=True)
    timestamp = Column(UtcDateTime, nullable=True)
    event = Column(String(30), nullable=True)
    owner = Column(String(30), nullable=True)
    event_note = Column(Text)

    def __repr__(self):
        return str(self.__key())

    def __key(self):
        return (
            self.dag_id,
            self.task_id,
            self.execution_date,
            self.timestamp,
            self.owner,
            self.event,
            self.event_note,
        )

    @classmethod
    @provide_session
    def get_one(
        cls,
        execution_date: pendulum.DateTime = None,
        timestamp: pendulum.DateTime = None,
        owners: Optional[Union[str, Iterable[str]]] = None,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        events: Optional[Union[str, Iterable[str]]] = None,
        dag_ids: Optional[Union[str, Iterable[str]]] = None,
        session: Session = None,
    ) -> Optional[Any]:
        """
        Retrieve a TaskNote value, optionally meeting certain criteria. Returns None
        of there are no results.

        :param execution_date: Execution date for the task
        :type execution_date: pendulum.datetime
        :param task_ids: Only TaskNotes from task with matching id will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str
        :param dag_ids: If provided, only pulls TaskNote from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_ids str or iterable of strings (representing dag ids)
        :param timestamp: Timestamp of the TaskNote creation
        :type timestamp: pendulum.datetime
        :param events: If provided, only pulls notes from these events.
            Can pass None to remove the filter.
        :type events: str or iterable of strings
        :param owners: If provided, only pulls TaskNote from these users
        :type owners: str or iterable of strings (representing usernames)
        :type dag_ids: str
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        return cls.get_many(
            execution_date=execution_date,
            timestamp=timestamp,
            owners=owners,
            task_ids=task_ids,
            events=events,
            dag_ids=dag_ids,
            session=session,
        ).first()

    @classmethod
    @provide_session
    def get_many(
        cls,
        execution_date: pendulum.DateTime = None,
        timestamp: pendulum.DateTime = None,
        owners: Optional[Union[str, Iterable[str]]] = None,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        events: Optional[Union[str, Iterable[str]]] = None,
        dag_ids: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
        session: Session = None,
    ) -> Query:
        """
        Composes a query to get one or more values from the TaskNote table.

        :param execution_date: Execution date for the task
        :type execution_date: pendulum.datetime
        :param timestamp: Timestamp of the TaskNote creation
        :type timestamp: pendulum.datetime
        :param owners: If provided, only pulls TaskNote from these users
        :type owners: str or iterable of strings (representing usernames)
        :param task_ids: Only notes from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_ids: If provided, only pulls TaskNote from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_ids: str or iterable of strings (representing dag ids)
        :param events: If provided, only pulls notes from these events.
            Can pass None to remove the filter.
        :type events: str or iterable of strings
        :param include_prior_dates: If False, only TaskNote from the current
            execution_date are returned. If True, TaskNote from previous dates
            are returned as well.
        :type include_prior_dates: bool
        :param limit: If required, limit the number of returned objects.
            TaskNote objects can be quite big and you might want to limit the
            number of rows.
        :type limit: int
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        filters = []

        if timestamp:
            filters.append(cls.timestamp == timestamp)

        if owners:
            if is_container(owners):
                filters.append(cls.owner.in_(owners))
            else:
                filters.append(cls.owner == owners)

        if events:
            if is_container(events):
                filters.append(cls.event.in_(events))
            else:
                filters.append(cls.event == events)

        if task_ids:
            if is_container(task_ids):
                filters.append(cls.task_id.in_(task_ids))
            else:
                filters.append(cls.task_id == task_ids)

        if execution_date:
            if include_prior_dates:
                filters.append(cls.execution_date <= execution_date)
            else:
                filters.append(cls.execution_date == execution_date)

        if dag_ids:
            if is_container(dag_ids):
                filters.append(cls.dag_id.in_(dag_ids))
            else:
                filters.append(cls.dag_id == dag_ids)

        query = session.query(cls).filter(and_(*filters))

        if limit:
            return query.limit(limit)
        else:
            return query
