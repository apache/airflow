from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.api_fastapi.datamodel.interactive import InteractiveResponse
from airflow.providers.standard.models import InteractiveResponseModel

if TYPE_CHECKING:
    from airflow.api_fastapi.common.db.common import SessionDep

interactive_router = AirflowRouter(tags=["InteractiveResponse"], prefix="/interactive")


@interactive_router.post(
    "/{task_instance_id}/response",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def write_response(
    task_instance_id: UUID,
    session: SessionDep,
) -> None:
    """Generate a new API token."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)

    ti = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not ti:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    existing_response = select(InteractiveResponseModel).where(ti_id=ti_id_str)
    if existing_response:
        raise HTTPException(status.HTTP_409_CONFLICT, f"Response exists for task task_instance id {task_id}")

    interactive_response = InteractiveResponseModel(
        content=content, task_instance=task_instance, ti_id=ti_id_str
    )
    session.add(interactive_response)


@interactive_router.get(
    "/{task_instance_id}/response",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def get_response(
    task_instance_id: UUID,
    session: SessionDep,
) -> InteractiveResponse:
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)

    ti = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not ti:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    interactive_response = session.scalar(select(InteractiveResponseModel).where(ti_id=ti_id_str))
    return InteractiveResponse(content=interactive_response.content)
