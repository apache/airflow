from sqlalchemy import Column, Integer, String
from airflow.models.base import Base
from airflow.utils.db import provide_session
from flask_babel import lazy_gettext


class TighteningController(Base):
    """
    tightening controllers.
    """

    __tablename__ = "tightening_controller"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    controller_name = Column(String(1000), nullable=False, unique=True)
    line_code = Column(String(1000), nullable=False)
    line_name = Column(String(1000), nullable=True)
    work_center_code = Column(String(1000), nullable=False)
    work_center_name = Column(String(1000), nullable=True)

    def __init__(self, controller_name=None, line_code=None, line_name=None, work_center_code=None,
                 work_center_name=None):
        self.controller_name = controller_name
        self.line_code = line_code
        self.line_name = line_name
        self.work_center_code = work_center_code
        self.work_center_name = work_center_name

    @classmethod
    @provide_session
    def find_controller(cls, controller_name, session=None):
        obj = session.query(cls).filter(cls.controller_name == controller_name).first()
        if obj is None:
            return {}
        return {
            'controller_name': obj.controller_name,
            'line_code': obj.line_code,
            'line_name': obj.line_name,
            'work_center_code': obj.work_center_code,
            'work_center_name': obj.work_center_name
        }
