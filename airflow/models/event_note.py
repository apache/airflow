import logging
from sqlalchemy import Column, Integer, String, Text
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils import timezone
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
    note = Column(Text)

    def __repr__(self):
        return str(self.__key())

    def __init__(self, event, note,  task_instance=None, owner=None,  **kwargs):
        self.note = note
        self.event = event
        self.timestamp = timezone.utcnow()
        task_owner = None

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            task_owner = task_instance.task.owner

        if 'task_id' in kwargs:
            self.task_id = kwargs['task_id']
        if 'dag_id' in kwargs:
            self.dag_id = kwargs['dag_id']
        if 'execution_date' in kwargs:
            if kwargs['execution_date']:
                self.execution_date = kwargs['execution_date']

        self.owner = owner or task_owner
