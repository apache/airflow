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

import datetime
from traceback import format_exception
from typing import Any, Iterable

from sqlalchemy import Column, Integer, String, func, or_
from sqlalchemy.orm import joinedload, relationship

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.triggers.base import BaseTrigger
from airflow.utils import timezone
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime, with_row_locks
from airflow.utils.state import State


class Trigger(Base):
    """
    Triggers are a workload that run in an asynchronous event loop shared with
    other Triggers, and fire off events that will unpause deferred Tasks,
    start linked DAGs, etc.

    They are persisted into the database and then re-hydrated into a
    "triggerer" process, where many are run at once. We model it so that
    there is a many-to-one relationship between Task and Trigger, for future
    deduplication logic to use.

    Rows will be evicted from the database when the triggerer detects no
    active Tasks/DAGs using them. Events are not stored in the database;
    when an Event is fired, the triggerer will directly push its data to the
    appropriate Task/DAG.
    """

    __tablename__ = "trigger"

    id = Column(Integer, primary_key=True)
    classpath = Column(String(1000), nullable=False)
    kwargs = Column(ExtendedJSON, nullable=False)
    created_date = Column(UtcDateTime, nullable=False)
    triggerer_id = Column(Integer, nullable=True)

    triggerer_job = relationship(
        "BaseJob",
        primaryjoin="BaseJob.id == Trigger.triggerer_id",
        foreign_keys=triggerer_id,
        uselist=False,
    )

    task_instance = relationship("TaskInstance", back_populates="trigger", lazy="joined", uselist=False)

    def __init__(self, classpath: str, kwargs: dict[str, Any], created_date: datetime.datetime | None = None):
        super().__init__()
        self.classpath = classpath
        self.kwargs = kwargs
        self.created_date = created_date or timezone.utcnow()

    @classmethod
    @internal_api_call
    def from_object(cls, trigger: BaseTrigger):
        """
        Alternative constructor that creates a trigger row based directly
        off of a Trigger object.
        """
        classpath, kwargs = trigger.serialize()
        return cls(classpath=classpath, kwargs=kwargs)

    @classmethod
    @internal_api_call
    @provide_session
    def bulk_fetch(cls, ids: Iterable[int], session=None) -> dict[int, Trigger]:
        """
        Fetches all the Triggers by ID and returns a dict mapping
        ID -> Trigger instance
        """
        query = (
            session.query(cls)
            .filter(cls.id.in_(ids))
            .options(
                joinedload("task_instance"),
                joinedload("task_instance.trigger"),
                joinedload("task_instance.trigger.triggerer_job"),
            )
        )
        return {obj.id: obj for obj in query}

    @classmethod
    @internal_api_call
    @provide_session
    def clean_unused(cls, session=None):
        """
        Deletes all triggers that have no tasks/DAGs dependent on them
        (triggers have a one-to-many relationship to both)
        """
        # Update all task instances with trigger IDs that are not DEFERRED to remove them
        for attempt in run_with_db_retries():
            with attempt:
                session.query(TaskInstance).filter(
                    TaskInstance.state != State.DEFERRED, TaskInstance.trigger_id.isnot(None)
                ).update({TaskInstance.trigger_id: None})
        # Get all triggers that have no task instances depending on them...
        ids = [
            trigger_id
            for (trigger_id,) in (
                session.query(cls.id)
                .join(TaskInstance, cls.id == TaskInstance.trigger_id, isouter=True)
                .group_by(cls.id)
                .having(func.count(TaskInstance.trigger_id) == 0)
            )
        ]
        # ...and delete them (we can't do this in one query due to MySQL)
        session.query(Trigger).filter(Trigger.id.in_(ids)).delete(synchronize_session=False)

    @classmethod
    @internal_api_call
    @provide_session
    def submit_event(cls, trigger_id, event, session=None):
        """
        Takes an event from an instance of itself, and triggers all dependent
        tasks to resume.
        """
        for task_instance in session.query(TaskInstance).filter(
            TaskInstance.trigger_id == trigger_id, TaskInstance.state == State.DEFERRED
        ):
            # Add the event's payload into the kwargs for the task
            next_kwargs = task_instance.next_kwargs or {}
            next_kwargs["event"] = event.payload
            task_instance.next_kwargs = next_kwargs
            # Remove ourselves as its trigger
            task_instance.trigger_id = None
            # Finally, mark it as scheduled so it gets re-queued
            task_instance.state = State.SCHEDULED

    @classmethod
    @internal_api_call
    @provide_session
    def submit_failure(cls, trigger_id, exc=None, session=None):
        """
        Called when a trigger has failed unexpectedly, and we need to mark
        everything that depended on it as failed. Notably, we have to actually
        run the failure code from a worker as it may have linked callbacks, so
        hilariously we have to re-schedule the task instances to a worker just
        so they can then fail.

        We use a special __fail__ value for next_method to achieve this that
        the runtime code understands as immediate-fail, and pack the error into
        next_kwargs.

        TODO: Once we have shifted callback (and email) handling to run on
        workers as first-class concepts, we can run the failure code here
        in-process, but we can't do that right now.
        """
        for task_instance in session.query(TaskInstance).filter(
            TaskInstance.trigger_id == trigger_id, TaskInstance.state == State.DEFERRED
        ):
            # Add the error and set the next_method to the fail state
            traceback = format_exception(type(exc), exc, exc.__traceback__) if exc else None
            task_instance.next_method = "__fail__"
            task_instance.next_kwargs = {"error": "Trigger failure", "traceback": traceback}
            # Remove ourselves as its trigger
            task_instance.trigger_id = None
            # Finally, mark it as scheduled so it gets re-queued
            task_instance.state = State.SCHEDULED

    @classmethod
    @internal_api_call
    @provide_session
    def ids_for_triggerer(cls, triggerer_id, session=None):
        """Retrieves a list of triggerer_ids."""
        return [row[0] for row in session.query(cls.id).filter(cls.triggerer_id == triggerer_id)]

    @classmethod
    @internal_api_call
    @provide_session
    def assign_unassigned(cls, triggerer_id, capacity, session=None):
        """
        Takes a triggerer_id and the capacity for that triggerer and assigns unassigned
        triggers until that capacity is reached, or there are no more unassigned triggers.
        """
        from airflow.jobs.base_job import BaseJob  # To avoid circular import

        count = session.query(func.count(cls.id)).filter(cls.triggerer_id == triggerer_id).scalar()
        capacity -= count

        if capacity <= 0:
            return

        alive_triggerer_ids = [
            row[0]
            for row in session.query(BaseJob.id).filter(
                BaseJob.end_date.is_(None),
                BaseJob.latest_heartbeat > timezone.utcnow() - datetime.timedelta(seconds=30),
                BaseJob.job_type == "TriggererJob",
            )
        ]

        # Find triggers who do NOT have an alive triggerer_id, and then assign
        # up to `capacity` of those to us.
        trigger_ids_query = with_row_locks(
            session.query(cls.id)
            .filter(or_(cls.triggerer_id.is_(None), cls.triggerer_id.notin_(alive_triggerer_ids)))
            .limit(capacity),
            session,
            skip_locked=True,
        ).all()
        if trigger_ids_query:
            session.query(cls).filter(cls.id.in_([i.id for i in trigger_ids_query])).update(
                {cls.triggerer_id: triggerer_id},
                synchronize_session=False,
            )
        session.commit()
