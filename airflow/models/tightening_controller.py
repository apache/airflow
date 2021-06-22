from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from airflow.models.base import Base, ID_LEN


class DeviceTypeModel(Base):
    __tablename__ = "device_type"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)

    name = Column(String(100), nullable=False)

    view_config = Column(String(1000), nullable=True)

    def __repr__(self):
        return self.name


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
    device_type = relationship('DeviceTypeModel')
    device_type_id = Column(Integer, ForeignKey('device_type.id'), nullable=False)

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

    @classmethod
    @provide_session
    def list_controllers(cls, session=None):
        controllers = list(session.query(cls).all())
        return controllers

    @classmethod
    @provide_session
    def add_controller(cls, controller_name, line_code, work_center_code, line_name=None, work_center_name=None,
                       session=None):
        session.add(TighteningController(
            controller_name=controller_name,
            line_code=line_code,
            work_center_code=work_center_code,
            line_name=line_name,
            work_center_name=work_center_name
        ))
