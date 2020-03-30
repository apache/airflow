from airflow import models
from airflow.utils.db import create_session
import os

CAS_BASE_URL = os.environ.get("CAS_BASE_URL", "http://localhost:9095")


def get_cas_base_url():
    connection_model = models.connection.Connection
    with create_session() as session:
        cas_base_url = session.query(connection_model).filter(
            connection_model.conn_id == 'cas_base_url').first()
    if not cas_base_url:
        cas_base_url = CAS_BASE_URL  # 从环境变量中获取URL配置
    return 'http://{}:{}'.format(cas_base_url.host,cas_base_url.port)
