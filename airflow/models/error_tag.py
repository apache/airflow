from sqlalchemy import Column, Integer, String

from airflow.models.base import Base
from airflow.utils.db import provide_session


class ErrorTag(Base):
    """
    curve error tags.
    """

    __tablename__ = "error_tag"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    label = Column(String(1000), nullable=False)
    value = Column(String(1000), nullable=False)

    @classmethod
    @provide_session
    def get_all(cls, session=None):
        objs = session.query(cls)
        return objs.all()
