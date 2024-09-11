from sqlalchemy import Column, Integer, String
from sqlalchemy.orm.session import Session

from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.session import provide_session


class CrossDagComm(Base, LoggingMixin):
    """Used to actively log events to the database"""

    __tablename__ = "cross_dag_communication"

    id = Column(Integer, primary_key=True)
    type = Column(String(10))
    key = Column(String(250))
    source_dag_id = Column(String(250))
    source_task_id = Column(String(250))
    target_dag_id = Column(String(250))
    target_task_id = Column(String(250))
    timestamp = Column(UtcDateTime, default=timezone.utcnow)

    def __init__(
        self, 
        type: str, 
        key: str=None, 
        source_dag_id: str=None, 
        source_task_id: str=None, 
        target_dag_id: str=None, 
        target_task_id: str=None, 
    ):
        self.type = type
        self.key = key
        self.source_dag_id = source_dag_id
        self.source_task_id = source_task_id
        self.target_dag_id = target_dag_id
        self.target_task_id = target_task_id
    
    @classmethod
    @provide_session
    def add(
        cls, 
        type: str, 
        key: str=None, 
        source_dag_id: str=None, 
        source_task_id: str=None, 
        target_dag_id: str=None, 
        target_task_id: str=None, 
        session: Session=None
    ):
        """
        Add a new entry to the cross_dag_communication table
        :param key: key of the entry
        :param value: value of the entry
        """
        session.add(cls(type, key, source_dag_id, source_task_id, target_dag_id, target_task_id))
        session.flush()
