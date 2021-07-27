from sqlalchemy import Boolean, Column, Float, Integer, String, Text

from plugins.result_storage.base import Base
from airflow.utils.sqlalchemy import UtcDateTime


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
    update_time = Column(String(256))
    user_id = Column(Integer)
    workorder_id = Column(Integer)
    vin = Column(String(256))
    task_id = Column(String(250))
    dag_id = Column(String(250))
    execution_date = Column(UtcDateTime)
    line_code = Column(String(100))  # 产线代码
    factory_code = Column(String(100))  # 工厂代码
    error_tag = Column(String(1000))
    result = Column(String(20))  # 分析结果, OK/NOK
    verify_error = Column(Integer)
    final_state = Column(String(20))  # 最终状态牵涉2次检验
    # controller_name = Column(String(100))  # 控制器名称@工位编号/工位名称
    bolt_number = Column(String(1000))  # 螺栓编号
    craft_type = Column(Integer)  # 工艺类型
    car_code = Column(String(1000))  # 车辆编号
    type = Column(String(100), default="normal")  # 任务实例类型，normal/rework,正常/返修
    should_analyze = Column(Boolean(), default=True)

    def as_dict(self):
        v: dict = self.__dict__
        if v:
            if v.get('_sa_instance_state'):
                v.pop('_sa_instance_state')
            return v
        else:
            return dict()


