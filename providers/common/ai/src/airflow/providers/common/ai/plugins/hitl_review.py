# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import logging
import mimetypes
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.xcom import XComModel
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_ACTION,
    XCOM_HUMAN_FEEDBACK_PREFIX,
    AgentSessionData,
    HITLReviewResponse,
    HumanActionData,
    HumanFeedbackRequest,
    SessionStatus,
)
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)

_PLUGIN_PREFIX = "/hitl-review"


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix for non-root deployments."""
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        base_path = urlparse(base_url).path
    else:
        base_path = base_url
    base_path = base_path.rstrip("/")
    return base_path + path


def _get_bundle_url() -> str:
    """
    Return bundle URL for the React plugin.

    Uses an absolute URL when api.base_url is a full URL so the bundle loads
    correctly in Vite dev mode, where import() resolves relative to the script
    origin (5173) rather than the document origin (28080).
    """
    path = _get_base_url_path(f"{_PLUGIN_PREFIX}/static/main.umd.cjs")
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}" + path
    return path


def _get_session():
    with create_session(scoped=False) as session:
        yield session


SessionDep = Annotated[Session, Depends(_get_session)]


def _get_map_index(q: str = Query("-1", alias="map_index")) -> int:
    """Parse map_index query; use -1 when placeholder unreplaced (e.g. ``{MAP_INDEX}``) or invalid."""
    try:
        return int(q)
    except (ValueError, TypeError):
        return -1


MapIndexDep = Annotated[int, Depends(_get_map_index)]


def _read_xcom(session: Session, *, dag_id: str, run_id: str, task_id: str, map_index: int = -1, key: str):
    """Read a single XCom value from the database."""
    row = session.scalars(
        XComModel.get_many(
            run_id=run_id,
            key=key,
            dag_ids=dag_id,
            task_ids=task_id,
            map_indexes=map_index,
            limit=1,
        )
    ).first()
    if row is None:
        return None
    return XComModel.deserialize_value(row)


def _read_xcom_by_prefix(
    session: Session, *, dag_id: str, run_id: str, task_id: str, map_index: int = -1, prefix: str
) -> dict[int, Any]:
    """Read all iteration-keyed XCom entries matching *prefix* (e.g. ``airflow_hitl_review_agent_output_``)."""
    query = select(XComModel.key, XComModel.value).where(
        XComModel.dag_id == dag_id,
        XComModel.run_id == run_id,
        XComModel.task_id == task_id,
        XComModel.map_index == map_index,
        XComModel.key.like(f"{prefix}%"),
    )
    result: dict[int, Any] = {}
    for key, value in session.execute(query).all():
        suffix = key[len(prefix) :]
        if suffix.isdigit():
            # deserialize_value expects an object with a .value attribute;
            # wrap the raw column value so we can reuse the standard deserialization path.
            row = SimpleNamespace(value=value)
            result[int(suffix)] = XComModel.deserialize_value(row)
    return result


def _write_xcom(
    session: Session, *, dag_id: str, run_id: str, task_id: str, map_index: int = -1, key: str, value
):
    """Write data to db."""
    XComModel.set(
        key=key,
        value=value,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        session=session,
    )


_RUNNING_TI_STATES = frozenset(
    {
        TaskInstanceState.RUNNING,
        TaskInstanceState.DEFERRED,
        TaskInstanceState.UP_FOR_RETRY,
        TaskInstanceState.QUEUED,
        TaskInstanceState.SCHEDULED,
    }
)


def _is_task_completed(
    session: Session, *, dag_id: str, run_id: str, task_id: str, map_index: int = -1
) -> bool:
    """Return True if the task instance is no longer running."""
    state = session.scalar(
        select(TI.state).where(
            TI.dag_id == dag_id,
            TI.run_id == run_id,
            TI.task_id == task_id,
            TI.map_index == map_index,
        )
    )
    if state is None:
        return True
    return state not in _RUNNING_TI_STATES


def _build_session_response(
    session: Session, *, dag_id: str, run_id: str, task_id: str, map_index: int = -1
) -> HITLReviewResponse | None:
    """Build `HITLReviewResponse` from XCom entries."""
    raw = _read_xcom(
        session,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
    )
    if raw is None:
        return None
    sess_data = AgentSessionData.model_validate(raw)
    outputs = _read_xcom_by_prefix(
        session,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        prefix=XCOM_AGENT_OUTPUT_PREFIX,
    )
    human_responses = _read_xcom_by_prefix(
        session,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        prefix=XCOM_HUMAN_FEEDBACK_PREFIX,
    )
    completed = _is_task_completed(
        session,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
    )
    return HITLReviewResponse.from_xcom(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        session=sess_data,
        outputs=outputs,
        human_entries=human_responses,
        task_completed=completed,
    )


hitl_review_app = FastAPI(
    title="HITL Review",
    description=(
        "REST API and chat UI for human-in-the-loop LLM feedback sessions.  "
        "Sessions are stored in XCom entries on the running task instance."
    ),
)


@hitl_review_app.get("/health")
async def health() -> dict[str, str]:
    """Liveness check."""
    return {"status": "ok"}


@hitl_review_app.get(
    "/sessions/find",
    response_model=HITLReviewResponse,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.HITL_DETAIL))],
)
async def find_session(
    db: SessionDep,
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: MapIndexDep,
) -> HITLReviewResponse:
    """Find the feedback session for a specific task instance."""
    resp = _build_session_response(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
    )
    if resp is None:
        task_active = not _is_task_completed(
            db,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
        )
        raise HTTPException(
            status_code=404,
            detail={"message": "No matching session found.", "task_active": task_active},
        )
    return resp


@hitl_review_app.post(
    "/sessions/feedback",
    response_model=HITLReviewResponse,
    dependencies=[Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.HITL_DETAIL))],
)
async def submit_feedback(
    body: HumanFeedbackRequest,
    db: SessionDep,
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: MapIndexDep,
) -> HITLReviewResponse:
    """Request changes — provide human feedback for the LLM."""
    if not (body.feedback and body.feedback.strip()):
        raise HTTPException(
            status_code=400,
            detail="Feedback is required when requesting changes.",
        )
    raw = _read_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
    )
    if raw is None:
        raise HTTPException(status_code=404, detail="No matching session found.")
    sess_data = AgentSessionData.model_validate(raw)
    if sess_data.status != SessionStatus.PENDING_REVIEW:
        raise HTTPException(
            status_code=409,
            detail=f"Session is '{sess_data.status.value}', expected 'pending_review'.",
        )

    iteration = sess_data.iteration
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}{iteration}",
        value=body.feedback,
    )

    action = HumanActionData(action="changes_requested", feedback=body.feedback, iteration=iteration)
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_HUMAN_ACTION,
        value=action.model_dump(mode="json"),
    )

    sess_data.status = SessionStatus.CHANGES_REQUESTED
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
        value=sess_data.model_dump(mode="json"),
    )

    resp = _build_session_response(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
    )
    if resp is None:
        raise HTTPException(status_code=500, detail="Failed to read session after update.")
    return resp


@hitl_review_app.post(
    "/sessions/approve",
    response_model=HITLReviewResponse,
    dependencies=[Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.HITL_DETAIL))],
)
async def approve_session(
    db: SessionDep,
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: MapIndexDep,
) -> HITLReviewResponse:
    """Approve the current output."""
    raw = _read_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
    )
    if raw is None:
        raise HTTPException(status_code=404, detail="No matching session found.")

    sess_data = AgentSessionData.model_validate(raw)
    if sess_data.status != SessionStatus.PENDING_REVIEW:
        raise HTTPException(
            status_code=409,
            detail=f"Session is '{sess_data.status.value}', expected 'pending_review'.",
        )

    action = HumanActionData(action="approve", iteration=sess_data.iteration)
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_HUMAN_ACTION,
        value=action.model_dump(mode="json"),
    )

    sess_data.status = SessionStatus.APPROVED
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
        value=sess_data.model_dump(mode="json"),
    )

    resp = _build_session_response(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
    )
    if resp is None:
        raise HTTPException(status_code=500, detail="Failed to read session after update.")
    return resp


@hitl_review_app.post(
    "/sessions/reject",
    response_model=HITLReviewResponse,
    dependencies=[Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.HITL_DETAIL))],
)
async def reject_session(
    db: SessionDep,
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: MapIndexDep,
) -> HITLReviewResponse:
    """Reject the output."""
    raw = _read_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
    )
    if raw is None:
        raise HTTPException(status_code=404, detail="No matching session found.")
    sess_data = AgentSessionData.model_validate(raw)
    if sess_data.status != SessionStatus.PENDING_REVIEW:
        raise HTTPException(
            status_code=409,
            detail=f"Session is '{sess_data.status.value}', expected 'pending_review'.",
        )

    action = HumanActionData(action="reject", iteration=sess_data.iteration)
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_HUMAN_ACTION,
        value=action.model_dump(mode="json"),
    )

    sess_data.status = SessionStatus.REJECTED
    _write_xcom(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        key=XCOM_AGENT_SESSION,
        value=sess_data.model_dump(mode="json"),
    )

    resp = _build_session_response(
        db,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
    )
    if resp is None:
        raise HTTPException(status_code=500, detail="Failed to read session after update.")
    return resp


# Ensure proper MIME types for plugin bundle (FastAPI serves .cjs as text/plain by default)
mimetypes.add_type("application/javascript", ".cjs")

_WWW_DIR = Path(__file__).parent / "www"
_dist_dir = _WWW_DIR / "dist"
if _dist_dir.is_dir():
    hitl_review_app.mount(
        "/static",
        StaticFiles(directory=str(_dist_dir.absolute()), html=True),
        name="hitl_review_static",
    )


class HITLReviewPlugin(AirflowPlugin):
    """Register the HITL Review REST API + chat UI on the Airflow API server."""

    name = "hitl_review"
    fastapi_apps = [
        {
            "name": "hitl-review",
            "app": hitl_review_app,
            "url_prefix": _PLUGIN_PREFIX,
        }
    ]
    react_apps = [
        {
            "name": "HITL Review",
            "bundle_url": _get_bundle_url(),
            "destination": "task_instance",
            "url_route": "hitl-review",
        }
    ]
