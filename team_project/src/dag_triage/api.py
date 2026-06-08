"""FastAPI routes for the dag_triage Task Instance panel."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from dag_triage.triage_service import TriageSummary, build_triage_summary


class TriageCategoryResponse(BaseModel):
    name: str
    confidence: float = Field(ge=0.0, le=1.0)


class TriageRemediationResponse(BaseModel):
    title: str
    steps: list[str]
    doc_links: list[str]


class TriageSummaryResponse(BaseModel):
    categories: list[TriageCategoryResponse]
    root_cause_summary: str
    remediations: list[TriageRemediationResponse]
    log_available: bool
    error: str | None = None


def _summary_to_response(summary: TriageSummary) -> TriageSummaryResponse:
    return TriageSummaryResponse(
        categories=[
            TriageCategoryResponse(name=category.name, confidence=category.confidence)
            for category in summary.categories
        ],
        root_cause_summary=summary.root_cause_summary,
        remediations=[
            TriageRemediationResponse(
                title=remediation.title,
                steps=remediation.steps,
                doc_links=remediation.doc_links,
            )
            for remediation in summary.remediations
        ],
        log_available=summary.log_available,
        error=summary.error,
    )


def _get_session():
    with create_session(scoped=False) as session:
        yield session


SessionDep = Annotated[Session, Depends(_get_session)]

triage_app = FastAPI(
    title="DAG Triage Panel",
    description="Failure triage summary for Task Instance view.",
)


@triage_app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@triage_app.get(
    "/summary",
    response_model=TriageSummaryResponse,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_LOGS))],
)
async def get_triage_summary(
    session: SessionDep,
    dag_id: str = Query(...),
    run_id: str = Query(...),
    task_id: str = Query(...),
    map_index: int = Query(-1),
) -> TriageSummaryResponse:
    """Return triage summary for a failed task instance."""
    task_instance = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == task_id,
            TaskInstance.map_index == map_index,
        )
    )
    if task_instance is None:
        task_instance = session.scalar(
            select(TaskInstanceHistory).where(
                TaskInstanceHistory.dag_id == dag_id,
                TaskInstanceHistory.run_id == run_id,
                TaskInstanceHistory.task_id == task_id,
                TaskInstanceHistory.map_index == map_index,
            )
        )

    if task_instance is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task instance not found")

    if task_instance.state != TaskInstanceState.FAILED:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=f"Triage summary is only available for failed tasks (state={task_instance.state}).",
        )

    return _summary_to_response(build_triage_summary(task_instance))
