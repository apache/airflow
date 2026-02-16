from __future__ import annotations

from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun
from airflow.callbacks.callback_requests import DagRunContext
from airflow.sdk import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType


def test_dagrun_model_accepts_null_start_date():
    """Ensure DagRun Pydantic model allows start_date=None."""
    data = {
        "dag_id": "test_dag",
        "run_id": "test_run",
        "logical_date": timezone.utcnow(),
        "data_interval_start": None,
        "data_interval_end": None,
        "run_after": timezone.utcnow(),
        "start_date": None,
        "end_date": None,
        "run_type": DagRunType.MANUAL,
        "state": DagRunState.QUEUED,
        "consumed_asset_events": [],
    }

    dr = DagRun.model_validate(data)
    assert dr.start_date is None


def test_dagrun_context_accepts_null_start_date():
    """Ensure DagRunContext (used by scheduler) accepts DagRun with null start_date."""
    data = {
        "dag_id": "test_dag",
        "run_id": "test_run",
        "logical_date": timezone.utcnow(),
        "data_interval_start": None,
        "data_interval_end": None,
        "run_after": timezone.utcnow(),
        "start_date": None,
        "end_date": None,
        "run_type": DagRunType.MANUAL,
        "state": DagRunState.QUEUED,
        "consumed_asset_events": [],
    }

    dr = DagRun.model_validate(data)
    ctx = DagRunContext(dag_run=dr)

    assert ctx.dag_run.start_date is None
