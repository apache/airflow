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

from sqlalchemy import event
from sqlalchemy.orm import Session

from airflow.listeners.listener import get_listener_manager
from airflow.models import TaskInstance
from airflow.utils.state import State

_is_listening = False


def on_task_instance_state_session_flush(session, flush_context):
    """
    Listens for session.flush() events that modify TaskInstance's state, and notify listeners that listen
    for that event. Doing it this way enable us to be stateless in the SQLAlchemy event listener.
    """
    logger = logging.getLogger(__name__)
    if not get_listener_manager().has_listeners:
        return
    for state in flush_context.states:
        if isinstance(state.object, TaskInstance) and session.is_modified(
            state.object, include_collections=False
        ):
            added, unchanged, deleted = flush_context.get_attribute_history(state, 'state')

            logger.debug(
                "session flush listener: added %s unchanged %s deleted %s - %s",
                added,
                unchanged,
                deleted,
                state.object,
            )
            if not added:
                continue

            previous_state = deleted[0] if deleted else State.NONE

            if State.RUNNING in added:
                get_listener_manager().hook.on_task_instance_running(
                    previous_state=previous_state, task_instance=state.object, session=session
                )
            elif State.FAILED in added:
                get_listener_manager().hook.on_task_instance_failed(
                    previous_state=previous_state, task_instance=state.object, session=session
                )
            elif State.SUCCESS in added:
                get_listener_manager().hook.on_task_instance_success(
                    previous_state=previous_state, task_instance=state.object, session=session
                )


def register_task_instance_state_events():
    global _is_listening
    if not _is_listening:
        event.listen(Session, 'after_flush', on_task_instance_state_session_flush)
        _is_listening = True


def unregister_task_instance_state_events():
    global _is_listening
    event.remove(Session, 'after_flush', on_task_instance_state_session_flush)
    _is_listening = False
