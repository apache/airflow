from datetime import datetime

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import DagRunState


class DAGRunLightResponse(BaseModel):
    """DAG Run serializer for responses."""

    run_id: str
    dag_id: str
    logical_date: datetime | None
    run_after: datetime
    start_date: datetime | None
    end_date: datetime | None
    state: DagRunState
