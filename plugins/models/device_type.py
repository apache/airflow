from sqlalchemy import Column, String, Integer
from airflow.plugins_manager import AirflowPlugin
from plugins.models.base import Base
from airflow import settings


class DeviceTypeModel(Base):
    __tablename__ = "device_type"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(100), nullable=False)
    view_config = Column(String(1000), nullable=True)

    def __repr__(self):
        return self.name

    def __init__(self, *args, name=None, view_config=None, **kwargs):
        super(DeviceTypeModel, self).__init__(*args, **kwargs)
        self.name = name
        self.view_config = view_config


# Defining the plugin class
class DeviceTypeModelPlugin(AirflowPlugin):
    name = "device_type_model_plugin"

    @classmethod
    def on_load(cls):
        engine = settings.engine
        if not engine.dialect.has_table(engine, DeviceTypeModel.__tablename__):
            Base.metadata.create_all(engine)
