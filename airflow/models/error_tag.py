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
    value = Column(String(1000), nullable=False, unique=True)

    def __init__(
        self, lable=None, value=None):
        self.label = lable
        self.value = value

    @staticmethod
    def _error_tag_data(error_tag):
        return {
            'id': error_tag.id,
            'label': error_tag.label,
            'value': error_tag.value
        }

    @staticmethod
    def _error_tag_label(error_tag):
        return error_tag.label

    @classmethod
    @provide_session
    def get_all_dict(cls, session=None):
        objs = session.query(cls).all()
        ret = {}
        for o in objs:
            ret.update({o.value: cls._error_tag_label(o)})
        return ret

    @classmethod
    @provide_session
    def get_all(cls, session=None):
        objs = session.query(cls).all()
        dataArr = list(map(cls._error_tag_data, objs))
        return dataArr
