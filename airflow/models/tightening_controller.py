from sqlalchemy import Column, Integer, String
from airflow.models.base import Base


class TighteningController(Base):
    """
    tightening controllers.
    """

    __tablename__ = "tightening_controller"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    controller_name = Column(String(1000), nullable=False, unique=True)
    line_code = Column(String(1000), nullable=False)
    work_center_code = Column(String(1000), nullable=False)
    work_center_name = Column(String(1000), nullable=True)

    def __init__(self, controller_name, line_code, work_center_code, work_center_name=None):
        self.controller_name = controller_name
        self.line_code = line_code
        self.work_center_code = work_center_code
        self.work_center_name = work_center_name
