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
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.orm import Session

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

log = logging.getLogger(__name__)

_PLUGIN_PREFIX = "/hitl-review"


def _get_session():
    with create_session(scoped=False) as session:
        yield session


SessionDep = Annotated[Session, Depends(_get_session, scope="function")]


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
) -> dict[int, str]:
    """Read all iteration-keyed XCom entries matching *prefix* (e.g. ``airflow_hitl_review_agent_output_``)."""
    query = select(XComModel.key, XComModel.value).where(
        XComModel.dag_id == dag_id,
        XComModel.run_id == run_id,
        XComModel.task_id == task_id,
        XComModel.map_index == map_index,
        XComModel.key.like(f"{prefix}%"),
    )
    result: dict[int, str] = {}
    for key, value in session.execute(query).all():
        suffix = key[len(prefix) :]
        if suffix.isdigit():
            result[int(suffix)] = value
    return result


def _write_xcom(
    session: Session, *, dag_id: str, run_id: str, task_id: str, map_index: int = -1, key: str, value
):
    """
    Write (upsert) a single XCom value in the database.

    Uses ``serialize=False`` so the value is stored natively in the JSON column
    without double-encoding (``XComModel.set`` with the default ``serialize=True``
    calls ``json.dumps`` which would wrap dicts in a JSON string).
    """
    XComModel.set(
        key=key,
        value=value,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        serialize=False,
        session=session,
    )


def _parse_model(model_cls, raw):
    """Parse a Pydantic model from a value that may be a dict or a JSON string."""
    if isinstance(raw, str):
        return model_cls.model_validate_json(raw)
    return model_cls.model_validate(raw)


_RUNNING_TI_STATES = frozenset({"running", "deferred", "up_for_retry", "queued", "scheduled"})


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
    sess_data = _parse_model(AgentSessionData, raw)
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


@hitl_review_app.get("/sessions/find", response_model=HITLReviewResponse)
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


@hitl_review_app.post("/sessions/feedback", response_model=HITLReviewResponse)
async def submit_feedback(
    body: HumanFeedbackRequest,
    db: SessionDep,
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: MapIndexDep,
) -> HITLReviewResponse:
    """Request changes — provide human feedback for the LLM."""
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
    sess_data = _parse_model(AgentSessionData, raw)
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


@hitl_review_app.post("/sessions/approve", response_model=HITLReviewResponse)
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

    sess_data = _parse_model(AgentSessionData, raw)
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


@hitl_review_app.post("/sessions/reject", response_model=HITLReviewResponse)
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
    sess_data = _parse_model(AgentSessionData, raw)
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


# -- Chat UI ----------------------------------------------------------------

_CHAT_HTML_SHELL = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HITL Review</title>
<style>*{margin:0;padding:0;box-sizing:border-box}</style>
</head>
<body>
<div id="root"></div>
<script src="__STATIC_PREFIX__/main.js"></script>
</body>
</html>
"""


@hitl_review_app.get("/chat", response_class=HTMLResponse)
async def chat_page(
    db: SessionDep,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = -1,
) -> HTMLResponse:
    """Serve the interactive chat window for a feedback session."""
    run_id = run_id.replace(" ", "+")
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
    html = _CHAT_HTML_SHELL.replace("__STATIC_PREFIX__", f"{_PLUGIN_PREFIX}/static")
    return HTMLResponse(html)


@hitl_review_app.get("/chat-by-task", response_class=HTMLResponse)
async def chat_by_task(
    db: SessionDep,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: MapIndexDep,
) -> HTMLResponse:
    """Serve the chat window for the active feedback session of a task instance."""
    html = _CHAT_HTML_SHELL.replace("__STATIC_PREFIX__", f"{_PLUGIN_PREFIX}/static")
    return HTMLResponse(html)


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
    external_views = [
        {
            "name": "HITL Review",
            "href": (
                f"{_PLUGIN_PREFIX}/chat-by-task"
                "?dag_id={DAG_ID}&run_id={RUN_ID}&task_id={TASK_ID}&map_index={MAP_INDEX}"
            ),
            "destination": "task_instance",
            "url_route": "hitl-review",
        }
    ]
