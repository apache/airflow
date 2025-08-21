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

import logging
from enum import Enum
from typing import TYPE_CHECKING

import sqlalchemy_jsonfield
import uuid6
from sqlalchemy import Column, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow.models import Base, Trigger
from airflow.settings import json
from airflow.triggers.deadline import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY, DeadlineCallbackTrigger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk.definitions.deadline import AsyncCallback, Callback as TaskSdkCallback, SyncCallback
    from airflow.triggers.base import TriggerEvent

log = logging.getLogger(__name__)


class CallbackState(str, Enum):
    """All possible states of callbacks."""

    PENDING = "pending"
    QUEUED = "queued"
    SUCCESS = "success"
    FAILED = "failed"


class CallbackType(str, Enum):
    """
    Types of Callbacks.

    Used:
        - for figuring out what class to instantiate while deserialization
        - by the executor (once implemented) to figure out how to interpret `Callback.data`
    """

    TRIGGERER = "triggerer"
    EXECUTOR = "executor"


class Callback(Base):
    """A generic callback."""

    __tablename__ = "callback"

    # This is used by SQLAlchemy to be able to deserialize DB rows to subclasses
    __mapper_args__ = {
        "polymorphic_identity": "callback",
        "polymorphic_on": "callback_type",
    }
    callback_type = Column(String(30), nullable=False)

    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)

    # Used by subclasses to store information about how to run the callback
    data = Column(sqlalchemy_jsonfield.JSONField(json=json))

    # State of the Callback of type: CallbackType
    state = Column(String(20))

    # Return value of the callback if successful, otherwise exception details
    output = Column(Text)

    def __init__(self, state: str | None = None, data: dict | None = None):
        self.state = state if state else CallbackState.PENDING
        self.data = data

    def queue(self, session):
        self.state = CallbackState.QUEUED
        session.add(self)


class TriggererCallback(Callback):
    """Callbacks that run on the Triggerer (must be async)."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.TRIGGERER}

    trigger = relationship("Trigger", back_populates="callback", uselist=False)

    def __init__(self, callback_def: AsyncCallback):
        super().__init__(data=callback_def.serialize())

    def queue(self, session: Session):
        self.trigger = Trigger.from_object(
            DeadlineCallbackTrigger(
                callback_path=self.data["path"],
                callback_kwargs=self.data["kwargs"],
            )
        )
        super().queue(session)

    def handle_callback_event(self, event: TriggerEvent, session: Session):
        status = event.payload.get(PAYLOAD_STATUS_KEY)
        if status in {CallbackState.SUCCESS, CallbackState.FAILED, CallbackState.QUEUED}:
            self.state = status
            if status != CallbackState.QUEUED:
                self.trigger = None
                self.output = event.payload.get(PAYLOAD_BODY_KEY)
            session.add(self)
        else:
            log.error("Unexpected event received: %s", event.payload)


class ExecutorCallback(Callback):
    """
    Callbacks that run on the executor.

    Not currently supported.
    """

    __mapper_args__ = {"polymorphic_identity": CallbackType.EXECUTOR}

    def __init__(self, callback_def: SyncCallback):
        super().__init__(data=callback_def.serialize())


def create_callback_from_sdk_def(callback_def: TaskSdkCallback) -> Callback:
    from airflow.sdk.definitions.deadline import AsyncCallback, SyncCallback

    if isinstance(callback_def, AsyncCallback):
        return TriggererCallback(callback_def)
    if isinstance(callback_def, SyncCallback):
        return ExecutorCallback(callback_def)
    raise RuntimeError(f"Cannot handle Callback of type {type(callback_def)}")
