from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from sqlalchemy import Column, Float, Integer, String, Text

from plugins.result_storage.base import Base


class ResultModel(Base):
    """
    result
    """

    def __repr__(self):
        return self.entity_id

    __tablename__ = "result"

    pk = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(Integer)
    entity_id = Column(String(256), unique=True)
    tool_sn = Column(String(256))
    angle_max = Column(Integer)
    angle_min = Column(Integer)
    angle_target = Column(Integer)
    batch = Column(String(32))
    batch_count = Column(Integer)
    channel_id = Column(Integer)
    controller_name = Column(String(256))
    controller_sn = Column(String(256))
    count = Column(Integer)
    device_type = Column(String(32))
    error_code = Column(String(64))
    group_seq = Column(Integer)
    job = Column(Integer)
    measure_angle = Column(Float)
    measure_result = Column(String(32))
    measure_time = Column(Float)
    measure_torque = Column(Float)
    nut_no = Column(String(256))
    pset = Column(Integer)
    seq = Column(Integer)
    step_results = Column(Text)
    strategy = Column(String(16))
    tightening_id = Column(String(128))
    torque_max = Column(Integer)
    torque_min = Column(Integer)
    torque_target = Column(Integer)
    torque_threshold = Column(Integer)
    update_time = Column(UtcDateTime, default=timezone.utcnow())
    user_id = Column(Integer)
    workorder_id = Column(Integer)
    vin = Column(String(256))

    def as_dict(self):
        v: dict = self.__dict__
        if v:
            v.pop('_sa_instance_state')
            return v
        else:
            return dict()


