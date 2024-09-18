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
from typing import TYPE_CHECKING, Any, Iterable

from sqlalchemy import Column, Integer, String, Text, delete, func, or_, select, update
from sqlalchemy.orm import relationship, selectinload
from sqlalchemy.sql.functions import coalesce

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.serialization.pydantic.trigger import TriggerPydantic
    from airflow.triggers.base import BaseTrigger


class Trigger(Base):
    """
    Base Trigger class.

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
    encrypted_kwargs = Column("kwargs", Text, nullable=False)
    created_date = Column(UtcDateTime, nullable=False)
    triggerer_id = Column(Integer, nullable=True)

    triggerer_job = relationship(
        "Job",
        primaryjoin="Job.id == Trigger.triggerer_id",
        foreign_keys=triggerer_id,
        uselist=False,
    )

    task_instance = relationship("TaskInstance", back_populates="trigger", lazy="selectin", uselist=False)

    def __init__(
        self,
        classpath: str,
        kwargs: dict[str, Any],
        created_date: datetime.datetime | None = None,
    ) -> None:
        super().__init__()
        self.classpath = classpath
        self.encrypted_kwargs = self._encrypt_kwargs(kwargs)
        self.created_date = created_date or timezone.utcnow()

    @property
    def kwargs(self) -> dict[str, Any]:
        """Return the decrypted kwargs of the trigger."""
        return self._decrypt_kwargs(self.encrypted_kwargs)

    @kwargs.setter
    def kwargs(self, kwargs: dict[str, Any]) -> None:
        """Set the encrypted kwargs of the trigger."""
        self.encrypted_kwargs = self._encrypt_kwargs(kwargs)

    @staticmethod
    def _encrypt_kwargs(kwargs: dict[str, Any]) -> str:
        """Encrypt the kwargs of the trigger."""
        import json

        from airflow.models.crypto import get_fernet
        from airflow.serialization.serialized_objects import BaseSerialization

        serialized_kwargs = BaseSerialization.serialize(kwargs)
        return get_fernet().encrypt(json.dumps(serialized_kwargs).encode("utf-8")).decode("utf-8")

    @staticmethod
    def _decrypt_kwargs(encrypted_kwargs: str) -> dict[str, Any]:
        """Decrypt the kwargs of the trigger."""
        import json

        from airflow.models.crypto import get_fernet
        from airflow.serialization.serialized_objects import BaseSerialization

        # We weren't able to encrypt the kwargs in all migration paths,
        # so we need to handle the case where they are not encrypted.
        # Triggers aren't long lasting, so we can skip encrypting them now.
        if encrypted_kwargs.startswith("{"):
            decrypted_kwargs = json.loads(encrypted_kwargs)
        else:
            decrypted_kwargs = json.loads(
                get_fernet().decrypt(encrypted_kwargs.encode("utf-8")).decode("utf-8")
            )

        return BaseSerialization.deserialize(decrypted_kwargs)

    def rotate_fernet_key(self):
        """Encrypts data with a new key. See: :ref:`security/fernet`."""
        from airflow.models.crypto import get_fernet

        self.encrypted_kwargs = get_fernet().rotate(self.encrypted_kwargs.encode("utf-8")).decode("utf-8")

    @classmethod
    @internal_api_call
    @provide_session
    def from_object(cls, trigger: BaseTrigger, session=NEW_SESSION) -> Trigger | TriggerPydantic:
        """Alternative constructor that creates a trigger row based directly off of a Trigger object."""
        classpath, kwargs = trigger.serialize()
        return cls(classpath=classpath, kwargs=kwargs)

    @classmethod
    @internal_api_call
    @provide_session
    def bulk_fetch(cls, ids: Iterable[int], session: Session = NEW_SESSION) -> dict[int, Trigger]:
        """Fetch all the Triggers by ID and return a dict mapping ID -> Trigger instance."""
        stmt = (
            select(cls)
            .where(cls.id.in_(ids))
            .options(
                selectinload(cls.task_instance)
                .joinedload(TaskInstance.trigger)
                .joinedload(Trigger.triggerer_job)
            )
        )
        return {obj.id: obj for obj in session.scalars(stmt)}

    @classmethod
    @internal_api_call
    @provide_session
    def clean_unused(cls, session: Session = NEW_SESSION) -> None:
        """
        Delete all triggers that have no tasks dependent on them.

        Triggers have a one-to-many relationship to task instances, so we need
        to clean those up first. Afterwards we can drop the triggers not
        referenced by anyone.
        """
        # Update all task instances with trigger IDs that are not DEFERRED to remove them
        for attempt in run_with_db_retries():
            with attempt:
                session.execute(
                    update(TaskInstance)
                    .where(
                        TaskInstance.state != TaskInstanceState.DEFERRED, TaskInstance.trigger_id.is_not(None)
                    )
                    .values(trigger_id=None)
                )

        # Get all triggers that have no task instances depending on them and delete them
        ids = (
            select(cls.id)
            .join(TaskInstance, cls.id == TaskInstance.trigger_id, isouter=True)
            .group_by(cls.id)
            .having(func.count(TaskInstance.trigger_id) == 0)
        )
        if session.bind.dialect.name == "mysql":
            # MySQL doesn't support DELETE with JOIN, so we need to do it in two steps
            ids = session.scalars(ids).all()
        session.execute(
            delete(Trigger).where(Trigger.id.in_(ids)).execution_options(synchronize_session=False)
        )

    @classmethod
    @internal_api_call
    @provide_session
    def submit_event(cls, trigger_id, event, session: Session = NEW_SESSION) -> None:
        """Take an event from an instance of itself, and trigger all dependent tasks to resume."""
        for task_instance in session.scalars(
            select(TaskInstance).where(
                TaskInstance.trigger_id == trigger_id, TaskInstance.state == TaskInstanceState.DEFERRED
            )
        ):
            event.handle_submit(task_instance=task_instance)

    @classmethod
    @internal_api_call
    @provide_session
    def submit_failure(cls, trigger_id, exc=None, session: Session = NEW_SESSION) -> None:
        """
        When a trigger has failed unexpectedly, mark everything that depended on it as failed.

        Notably, we have to actually run the failure code from a worker as it may
        have linked callbacks, so hilariously we have to re-schedule the task
        instances to a worker just so they can then fail.

        We use a special __fail__ value for next_method to achieve this that
        the runtime code understands as immediate-fail, and pack the error into
        next_kwargs.

        TODO: Once we have shifted callback (and email) handling to run on
        workers as first-class concepts, we can run the failure code here
        in-process, but we can't do that right now.
        """
        for task_instance in session.scalars(
            select(TaskInstance).where(
                TaskInstance.trigger_id == trigger_id, TaskInstance.state == TaskInstanceState.DEFERRED
            )
        ):
            # Add the error and set the next_method to the fail state
            traceback = format_exception(type(exc), exc, exc.__traceback__) if exc else None
            task_instance.next_method = "__fail__"
            task_instance.next_kwargs = {"error": "Trigger failure", "traceback": traceback}
            # Remove ourselves as its trigger
            task_instance.trigger_id = None
            # Finally, mark it as scheduled so it gets re-queued
            task_instance.state = TaskInstanceState.SCHEDULED

    @classmethod
    @internal_api_call
    @provide_session
    def ids_for_triggerer(cls, triggerer_id, session: Session = NEW_SESSION) -> list[int]:
        """Retrieve a list of triggerer_ids."""
        return session.scalars(select(cls.id).where(cls.triggerer_id == triggerer_id)).all()

    @classmethod
    @internal_api_call
    @provide_session
    def assign_unassigned(
        cls, triggerer_id, capacity, health_check_threshold, session: Session = NEW_SESSION
    ) -> None:
        """
        Assign unassigned triggers based on a number of conditions.

        Takes a triggerer_id, the capacity for that triggerer and the Triggerer job heartrate
        health check threshold, and assigns unassigned triggers until that capacity is reached,
        or there are no more unassigned triggers.
        """
        from airflow.jobs.job import Job  # To avoid circular import

        count = session.scalar(select(func.count(cls.id)).filter(cls.triggerer_id == triggerer_id))
        capacity -= count

        if capacity <= 0:
            return

        alive_triggerer_ids = select(Job.id).where(
            Job.end_date.is_(None),
            Job.latest_heartbeat > timezone.utcnow() - datetime.timedelta(seconds=health_check_threshold),
            Job.job_type == "TriggererJob",
        )

        # Find triggers who do NOT have an alive triggerer_id, and then assign
        # up to `capacity` of those to us.
        trigger_ids_query = cls.get_sorted_triggers(
            capacity=capacity, alive_triggerer_ids=alive_triggerer_ids, session=session
        )
        if trigger_ids_query:
            session.execute(
                update(cls)
                .where(cls.id.in_([i.id for i in trigger_ids_query]))
                .values(triggerer_id=triggerer_id)
                .execution_options(synchronize_session=False)
            )

        session.commit()

    @classmethod
    def get_sorted_triggers(cls, capacity: int, alive_triggerer_ids: list[int] | Select, session: Session):
        """
        Get sorted triggers based on capacity and alive triggerer ids.

        :param capacity: The capacity of the triggerer.
        :param alive_triggerer_ids: The alive triggerer ids as a list or a select query.
        :param session: The database session.
        """
        query = with_row_locks(
            select(cls.id)
            .join(TaskInstance, cls.id == TaskInstance.trigger_id, isouter=False)
            .where(or_(cls.triggerer_id.is_(None), cls.triggerer_id.not_in(alive_triggerer_ids)))
            .order_by(coalesce(TaskInstance.priority_weight, 0).desc(), cls.created_date)
            .limit(capacity),
            session,
            skip_locked=True,
        )
        return session.execute(query).all()
