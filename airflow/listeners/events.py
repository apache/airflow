import logging

from sqlalchemy import event
from sqlalchemy.orm import Session

from airflow.listeners.listener import get_listener_manager
from airflow.models import TaskInstance
from airflow.utils.state import State


logger = logging.getLogger()


@event.listens_for(Session, 'after_flush', propagate=True)
def on_task_instance_session_flush(session, flush_context):
    """
    Listens for session.flush() events that modify TaskInstance's state, and notify listeners that listen
    for that event. Doing it this way enable us to be stateless in the SQLAlchemy event listener.
    """
    for state in flush_context.states:
        if isinstance(state.object, TaskInstance) and session.is_modified(
            state.object, include_collections=False
        ):
            added, unchanged, deleted = flush_context.get_attribute_history(state, 'state')

            logger.debug(f"session flush listener: added {added} unchanged {unchanged} deleted {deleted}")
            if not added:
                continue

            if State.RUNNING in added:
                get_listener_manager().hook.on_task_instance_running(
                    previous_state=deleted[0],
                    task_instance=state.object,
                    session=session
                )
            elif State.FAILED in added:
                get_listener_manager().hook.on_task_instance_failed(
                    previous_state=deleted[0],
                    task_instance=state.object,
                    session=session
                )
            elif State.SUCCESS in added:
                get_listener_manager().hook.on_task_instance_success(
                    previous_state=deleted[0],
                    task_instance=state.object,
                    session=session
                )
