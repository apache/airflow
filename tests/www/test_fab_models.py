from airflow.models.base import Base
from airflow.www.fab_security.sqla.models import User


def test_fab_models_use_airflow_base_meta():
    user = User()
    assert user.metadata == Base.metadata