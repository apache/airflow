from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from plugins.models.base import Base


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
    device_type = relationship('models.device_type.DeviceTypeModel')
    device_type_id = Column(Integer, ForeignKey('device_type.id'), nullable=False)

    def __init__(self, *args, controller_name=None, line_code=None, line_name=None, work_center_code=None,
                 work_center_name=None, device_type_id=None, **kwargs):
        super(TighteningController, self).__init__(*args, **kwargs)
        self.controller_name = controller_name
        self.line_code = line_code
        self.line_name = line_name
        self.work_center_code = work_center_code
        self.work_center_name = work_center_name
        self.device_type_id = device_type_id

    def as_dict(self):
        v: dict = self.__dict__
        if v:
            v.pop('id')
            v.pop('_sa_instance_state')
        return v

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
            'work_center_name': obj.work_center_name,
            'device_type_id': obj.device_type_id,
        }

    @classmethod
    @provide_session
    def list_controllers(cls, session=None):
        controllers = list(session.query(cls).all())
        return controllers

    @classmethod
    @provide_session
    def add_controller(cls, controller_name, line_code, work_center_code, line_name=None, work_center_name=None,
                       device_type_id=None,
                       session=None):
        session.add(TighteningController(
            controller_name=controller_name,
            line_code=line_code,
            work_center_code=work_center_code,
            line_name=line_name,
            work_center_name=work_center_name,
            device_type_id=device_type_id
        ))
