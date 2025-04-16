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
import logging
from collections.abc import Iterable
from enum import Enum
from functools import singledispatch
from traceback import format_exception
from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, Integer, String, Text, delete, func, or_, select, update
from sqlalchemy.orm import relationship, selectinload
from sqlalchemy.sql.functions import coalesce

from airflow.assets.manager import AssetManager
from airflow.models.asset import asset_trigger_association_table
from airflow.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.triggers import base as events
from airflow.utils import timezone
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.triggers.base import BaseTrigger

TRIGGER_FAIL_REPR = "__fail__"
"""String value to represent trigger failure.

Internal use only.

:meta private:
"""

log = logging.getLogger(__name__)


class TriggerFailureReason(str, Enum):
    """
    Reasons for trigger failures.

    Internal use only.

    :meta private:
    """

    TRIGGER_TIMEOUT = "Trigger timeout"
    TRIGGER_FAILURE = "Trigger failure"


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

    assets = relationship("AssetModel", secondary=asset_trigger_association_table, back_populates="triggers")

    def __init__(
        self,
        classpath: str,
        kwargs: dict[str, Any],
        created_date: datetime.datetime | None = None,
    ) -> None:
        super().__init__()
        self.classpath = classpath
        self.encrypted_kwargs = self.encrypt_kwargs(kwargs)
        self.created_date = created_date or timezone.utcnow()

    @property
    def kwargs(self) -> dict[str, Any]:
        """Return the decrypted kwargs of the trigger."""
        return self._decrypt_kwargs(self.encrypted_kwargs)

    @kwargs.setter
    def kwargs(self, kwargs: dict[str, Any]) -> None:
        """Set the encrypted kwargs of the trigger."""
        self.encrypted_kwargs = self.encrypt_kwargs(kwargs)

    @staticmethod
    def encrypt_kwargs(kwargs: dict[str, Any]) -> str:
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
    def from_object(cls, trigger: BaseTrigger) -> Trigger:
        """Alternative constructor that creates a trigger row based directly off of a Trigger object."""
        classpath, kwargs = trigger.serialize()
        return cls(classpath=classpath, kwargs=kwargs)

    @classmethod
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
    @provide_session
    def fetch_trigger_ids_with_asset(cls, session: Session = NEW_SESSION) -> set[str]:
        """Fetch all the trigger IDs associated with at least one asset."""
        query = select(asset_trigger_association_table.columns.trigger_id)
        return {trigger_id for trigger_id in session.scalars(query)}

    @classmethod
    @provide_session
    def clean_unused(cls, session: Session = NEW_SESSION) -> None:
        """
        Delete all triggers that have no tasks dependent on them and are not associated to an asset.

        Triggers have a one-to-many relationship to task instances, so we need to clean those up first.
        Afterward we can drop the triggers not referenced by anyone.
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

        # Get all triggers that have no task instances and assets depending on them and delete them
        ids = (
            select(cls.id)
            .where(~cls.assets.any())
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
    @provide_session
    def submit_event(cls, trigger_id, event: events.TriggerEvent, session: Session = NEW_SESSION) -> None:
        """
        Fire an event.

        Resume all tasks that were in deferred state.
        Send an event to all assets associated to the trigger.
        """
        # Resume deferred tasks
        for task_instance in session.scalars(
            select(TaskInstance).where(
                TaskInstance.trigger_id == trigger_id, TaskInstance.state == TaskInstanceState.DEFERRED
            )
        ):
            handle_event_submit(event, task_instance=task_instance, session=session)

        # Send an event to assets
        trigger = session.scalars(select(cls).where(cls.id == trigger_id)).one_or_none()
        if trigger is None:
            # Already deleted for some reason
            return
        for asset in trigger.assets:
            AssetManager.register_asset_change(
                asset=asset.to_public(),
                extra={"from_trigger": True, "payload": event.payload},
                session=session,
            )

    @classmethod
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
        """
        for task_instance in session.scalars(
            select(TaskInstance).where(
                TaskInstance.trigger_id == trigger_id, TaskInstance.state == TaskInstanceState.DEFERRED
            )
        ):
            # Add the error and set the next_method to the fail state
            if isinstance(exc, BaseException):
                traceback = format_exception(type(exc), exc, exc.__traceback__)
            else:
                traceback = exc
            task_instance.next_method = TRIGGER_FAIL_REPR
            task_instance.next_kwargs = {
                "error": TriggerFailureReason.TRIGGER_FAILURE,
                "traceback": traceback,
            }
            # Remove ourselves as its trigger
            task_instance.trigger_id = None
            # Finally, mark it as scheduled so it gets re-queued
            task_instance.state = TaskInstanceState.SCHEDULED
            task_instance.scheduled_dttm = timezone.utcnow()

    @classmethod
    @provide_session
    def ids_for_triggerer(cls, triggerer_id, session: Session = NEW_SESSION) -> list[int]:
        """Retrieve a list of trigger ids."""
        return session.scalars(select(cls.id).where(cls.triggerer_id == triggerer_id)).all()

    @classmethod
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
        ti_triggers = session.execute(query).all()

        query = with_row_locks(
            select(cls.id).where(cls.assets.any()).order_by(cls.created_date).limit(capacity),
            session,
            skip_locked=True,
        )
        asset_triggers = session.execute(query).all()

        # Add triggers associated to assets after triggers associated to tasks
        # It prioritizes DAGs over event driven scheduling which is fair
        return ti_triggers + asset_triggers


@singledispatch
def handle_event_submit(event: events.TriggerEvent, *, task_instance: TaskInstance, session: Session) -> None:
    """
    Handle the submit event for a given task instance.

    This function sets the next method and next kwargs of the task instance,
    as well as its state to scheduled. It also adds the event's payload
    into the kwargs for the task.

    :param task_instance: The task instance to handle the submit event for.
    :param session: The session to be used for the database callback sink.
    """
    from airflow.utils.state import TaskInstanceState

    # Get the next kwargs of the task instance, or an empty dictionary if it doesn't exist
    next_kwargs = task_instance.next_kwargs or {}

    # Add the event's payload into the kwargs for the task
    next_kwargs["event"] = event.payload

    # Update the next kwargs of the task instance
    task_instance.next_kwargs = next_kwargs

    # Remove ourselves as its trigger
    task_instance.trigger_id = None

    # Set the state of the task instance to scheduled
    task_instance.state = TaskInstanceState.SCHEDULED
    task_instance.scheduled_dttm = timezone.utcnow()
    session.flush()


@handle_event_submit.register(events.BaseTaskEndEvent)
def _process_BaseTaskEndEvent(
    event: events.BaseTaskEndEvent, *, task_instance: TaskInstance, session: Session
) -> None:
    """
    Submit event for the given task instance.

    Marks the task with the state `task_instance_state` and optionally pushes xcom if applicable.

    :param task_instance: The task instance to be submitted.
    :param session: The session to be used for the database callback sink.
    """
    from airflow.callbacks.callback_requests import TaskCallbackRequest
    from airflow.callbacks.database_callback_sink import DatabaseCallbackSink
    from airflow.utils.state import TaskInstanceState

    # Mark the task with terminal state and prevent it from resuming on worker
    task_instance.trigger_id = None
    task_instance.set_state(event.task_instance_state, session=session)

    def _submit_callback_if_necessary() -> None:
        """Submit a callback request if the task state is SUCCESS or FAILED."""
        if event.task_instance_state in (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED):
            request = TaskCallbackRequest(
                filepath=task_instance.dag_model.relative_fileloc,
                ti=task_instance,
                task_callback_type=event.task_instance_state,
                bundle_name=task_instance.dag_model.bundle_name,
                bundle_version=task_instance.dag_run.bundle_version,
            )
            log.info("Sending callback: %s", request)
            try:
                DatabaseCallbackSink().send(callback=request, session=session)
            except Exception:
                log.exception("Failed to send callback.")

    def _push_xcoms_if_necessary() -> None:
        """Pushes XComs to the database if they are provided."""
        if event.xcoms:
            for key, value in event.xcoms.items():
                task_instance.xcom_push(key=key, value=value)

    _submit_callback_if_necessary()
    _push_xcoms_if_necessary()
    session.flush()
