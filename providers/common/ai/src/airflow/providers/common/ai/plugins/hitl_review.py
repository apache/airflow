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

from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.common.ai.plugins._plugin_urls import get_bundle_url
from airflow.providers.common.compat.version_compat import (
    AIRFLOW_V_3_1_PLUS,
    AIRFLOW_V_3_3_PLUS,
    AIRFLOW_V_3_4_PLUS,
)

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
    from airflow.plugins_manager import FastAPIAppDict, ReactAppDict

_PLUGIN_PREFIX = "/hitl-review"


if AIRFLOW_V_3_1_PLUS:
    import mimetypes
    from pathlib import Path

    from fastapi import Depends, FastAPI, HTTPException, Query
    from fastapi.staticfiles import StaticFiles
    from sqlalchemy import delete, select
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
    from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance as TI
    from airflow.models.xcom import XComModel
    from airflow.providers.common.ai.utils.hitl_review import (
        HITL_FEEDBACK_PARAM,
        HITL_OPTION_APPROVE,
        HITL_OPTION_REJECT,
        HITL_OPTION_REQUEST_CHANGES,
        XCOM_AGENT_OUTPUT_PREFIX,
        XCOM_AGENT_SESSION,
        XCOM_HUMAN_ACTION,
        XCOM_HUMAN_FEEDBACK_PREFIX,
        AgentSessionData,
        HITLReviewResponse,
        HumanActionData,
        HumanActionType,
        HumanFeedbackRequest,
        SessionStatus,
    )
    from airflow.providers.common.compat.sdk import timezone
    from airflow.sdk import TaskInstanceState
    from airflow.utils.session import create_session

    if AIRFLOW_V_3_3_PLUS:
        # A parked review is resumed the way the core Human-in-the-loop response route resumes a task.
        from airflow.models.hitl import HITLDetail, HITLUser
        from airflow.models.trigger import handle_event_submit
        from airflow.triggers.base import TriggerEvent

    if AIRFLOW_V_3_4_PLUS:
        from airflow.api_fastapi.core_api.services.public.task_coordinates import resolve_task_scope
        from airflow.models.dagbag import DBDagBag
        from airflow.models.task_coordinates import TaskCoordinateResolver

    def _get_session():
        with create_session(scoped=False) as session:
            yield session

    SessionDep = Annotated[Session, Depends(_get_session)]

    def _read_xcom(
        session: Session,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int = -1,
        region_id: UUID | None,
        key: str,
    ):
        """Read a single XCom value from the database."""
        scope: dict[str, Any] = (
            {"region_id": region_id, "include_node_regions": False} if AIRFLOW_V_3_4_PLUS else {}
        )
        read = XComModel.get_many(
            run_id=run_id,
            key=key,
            dag_ids=dag_id,
            task_ids=task_id,
            map_indexes=map_index,
            limit=1,
            **scope,
        )
        row = session.scalars(read).first()
        if row is None:
            return None
        return row.value

    def _read_xcom_by_prefix(
        session: Session,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int = -1,
        region_id: UUID | None,
        prefix: str,
    ) -> dict[int, Any]:
        """Read all iteration-keyed XCom entries matching *prefix* (e.g. ``airflow_hitl_review_agent_output_``)."""
        if AIRFLOW_V_3_4_PLUS:
            read = XComModel.get_many(
                run_id=run_id,
                dag_ids=dag_id,
                task_ids=task_id,
                map_indexes=map_index,
                region_id=region_id,
                include_node_regions=False,
            )
            entity = read.column_descriptions[0]["entity"]
            query = read.with_only_columns(entity.key, entity.value).where(entity.key.like(f"{prefix}%"))
        else:
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
                result[int(suffix)] = value
        return result

    def _write_xcom(
        session: Session,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int = -1,
        region_id: UUID | None,
        key: str,
        value,
    ):
        """Write data to db."""
        if AIRFLOW_V_3_4_PLUS:
            owner = session.scalar(
                _task_instance_query(
                    TI.id,
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    map_index=map_index,
                    region_id=region_id,
                )
            )
            if owner is None:
                raise HTTPException(404, f"Task instance not found on DAG {dag_id!r} with ID {run_id!r}")
            XComModel.set_for_attempt(
                task_instance_id=owner,
                key=key,
                value=value,
                serialize=False,
                session=session,
            )
            return

        # Stores value natively to match worker-written XComs; use XComModel.set(serialize=False) once min Airflow >= 3.2.
        dag_run_id = session.scalar(select(DagRun.id).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
        if dag_run_id is None:
            raise HTTPException(404, f"DAG run not found on DAG {dag_id!r} with ID {run_id!r}")
        session.execute(
            delete(XComModel).where(
                XComModel.key == key,
                XComModel.run_id == run_id,
                XComModel.task_id == task_id,
                XComModel.dag_id == dag_id,
                XComModel.map_index == map_index,
            )
        )
        session.add(
            XComModel(
                dag_run_id=dag_run_id,
                key=key,
                value=value,
                run_id=run_id,
                task_id=task_id,
                dag_id=dag_id,
                map_index=map_index,
            )
        )
        session.flush()

    _RUNNING_TI_STATES = frozenset(
        {
            TaskInstanceState.RUNNING,
            TaskInstanceState.DEFERRED,
            TaskInstanceState.UP_FOR_RETRY,
            TaskInstanceState.QUEUED,
            TaskInstanceState.SCHEDULED,
            # A task waiting for the reviewer between rounds is parked, not finished.
            *([TaskInstanceState.AWAITING_INPUT] if AIRFLOW_V_3_3_PLUS else []),
        }
    )

    _STATUS_FOR_ACTION: dict[HumanActionType, SessionStatus] = {
        "approve": SessionStatus.APPROVED,
        "changes_requested": SessionStatus.CHANGES_REQUESTED,
        "reject": SessionStatus.REJECTED,
    }
    # The option of the review request each action answers, when the task is parked on one.
    _HITL_OPTION_FOR_ACTION: dict[HumanActionType, str] = {
        "approve": HITL_OPTION_APPROVE,
        "changes_requested": HITL_OPTION_REQUEST_CHANGES,
        "reject": HITL_OPTION_REJECT,
    }

    def _task_instance_query(
        entity: Any,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int = -1,
        region_id: UUID | None,
    ):
        """Select ``entity`` (``TI`` or one of its columns) for the task instance under review."""
        query = select(entity).where(
            TI.dag_id == dag_id,
            TI.run_id == run_id,
            TI.task_id == task_id,
        )
        if AIRFLOW_V_3_4_PLUS:
            return query.where(
                TI.working_set.is_(True),
                TI.region_index == map_index,
                TI.region_id == region_id,
            )
        return query.where(TI.map_index == map_index)

    def _is_task_completed(
        session: Session,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int = -1,
        region_id: UUID | None,
    ) -> bool:
        """Return True if the task instance is no longer running."""
        state = session.scalar(
            _task_instance_query(
                TI.state,
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_index=map_index,
                region_id=region_id,
            )
        )
        if state is None:
            return True
        return state not in _RUNNING_TI_STATES

    def _build_session_response(
        session: Session,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int = -1,
        region_id: UUID | None,
    ) -> HITLReviewResponse | None:
        """Build `HITLReviewResponse` from XCom entries."""
        raw = _read_xcom(
            session,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
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
            region_id=region_id,
            prefix=XCOM_AGENT_OUTPUT_PREFIX,
        )
        human_responses = _read_xcom_by_prefix(
            session,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
            prefix=XCOM_HUMAN_FEEDBACK_PREFIX,
        )
        completed = _is_task_completed(
            session,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
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

    def _get_map_index(q: str = Query("-1", alias="map_index")) -> int:
        """Parse map_index query; use -1 when placeholder unreplaced (e.g. ``{MAP_INDEX}``) or invalid."""
        try:
            return int(q)
        except (ValueError, TypeError):
            return -1

    MapIndexDep = Annotated[int, Depends(_get_map_index)]

    def _get_task_scope(
        db: SessionDep,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: MapIndexDep,
        region_id: UUID | None = None,
        region_index: Annotated[int | None, Query(ge=-1)] = None,
    ) -> tuple[UUID | None, int]:
        if AIRFLOW_V_3_4_PLUS:
            scope = resolve_task_scope(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                resolver=TaskCoordinateResolver(DBDagBag(), db),
                map_index=map_index,
                region_id=region_id,
                region_index=region_index,
            )
            return scope.region_id, scope.region_index
        if region_id is not None or region_index is not None:
            raise HTTPException(400, "Region selectors require Airflow 3.4 or later")
        return None, map_index

    TaskScopeDep = Annotated[tuple[UUID | None, int], Depends(_get_task_scope)]

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
        scope: TaskScopeDep,
    ) -> HITLReviewResponse:
        """Find the feedback session for a specific task instance."""
        region_id, map_index = scope
        resp = _build_session_response(
            db,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
        )
        if resp is None:
            task_active = not _is_task_completed(
                db,
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_index=map_index,
                region_id=region_id,
            )
            raise HTTPException(
                status_code=404,
                detail={"message": "No matching session found.", "task_active": task_active},
            )
        return resp

    def _answer_review_request(
        session: Session,
        ti: TI | None,
        *,
        action: HumanActionType,
        feedback: str,
        user: BaseUser,
    ) -> None:
        """
        Answer the Human-in-the-loop request a parked review waits on, and resume the task if parked.

        A task that has not parked yet finds the answer when it parks, and resumes straight away.
        """
        if ti is None:
            raise HTTPException(status_code=404, detail="Task instance not found.")
        # Running: the task is about to park, and will find the answer when it does. Any other
        # state (cleared, failed, already resumed) would leave the answer unread.
        if ti.state not in (TaskInstanceState.AWAITING_INPUT, TaskInstanceState.RUNNING):
            raise HTTPException(
                status_code=409,
                detail=f"The task is {ti.state}, not waiting for a review answer. Reload the session.",
            )
        detail = session.scalar(
            select(HITLDetail).where(HITLDetail.ti_id == ti.id).with_for_update(of=HITLDetail)
        )
        if detail is None or detail.response_received:
            raise HTTPException(
                status_code=409,
                detail="This review round was already answered, for example on the Required Actions "
                "page. Reload the session.",
            )
        detail.responded_by = dict(HITLUser(id=str(user.get_id()), name=user.get_name()))
        detail.responded_at = timezone.utcnow()
        detail.chosen_options = [_HITL_OPTION_FOR_ACTION[action]]
        detail.params_input = {HITL_FEEDBACK_PARAM: feedback}
        session.add(detail)
        if ti.state == TaskInstanceState.AWAITING_INPUT:
            handle_event_submit(
                TriggerEvent(detail.as_resume_event_payload()), task_instance=ti, session=session
            )

    def _record_action(
        session: Session,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        region_id: UUID | None,
        user: BaseUser,
        action: HumanActionType,
        feedback: str = "",
    ) -> HITLReviewResponse:
        """Record the reviewer's action on the round that is waiting, and resume the task if it is parked."""
        coordinates: dict[str, Any] = {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "map_index": map_index,
            "region_id": region_id,
        }
        ti = None
        if AIRFLOW_V_3_3_PLUS:
            # Lock the task instance before reading the session, in the order the core response route
            # and the park transition lock (task instance, then its HITL request): two clicks cannot
            # both answer one round, and an answer cannot deadlock against the task parking.
            ti = session.scalar(_task_instance_query(TI, **coordinates).with_for_update(of=TI))
        raw = _read_xcom(session, **coordinates, key=XCOM_AGENT_SESSION)
        if raw is None:
            raise HTTPException(status_code=404, detail="No matching session found.")
        sess_data = AgentSessionData.model_validate(raw)
        if sess_data.status != SessionStatus.PENDING_REVIEW:
            raise HTTPException(
                status_code=409,
                detail=f"Session is '{sess_data.status.value}', expected 'pending_review'.",
            )
        if sess_data.parked:
            _answer_review_request(session, ti, action=action, feedback=feedback, user=user)

        iteration = sess_data.iteration
        if action == "changes_requested":
            _write_xcom(
                session, **coordinates, key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}{iteration}", value=feedback
            )
        if not sess_data.parked:
            # A polling task reads the action from this XCom; a parked one gets it on resume.
            human_action = HumanActionData(action=action, feedback=feedback, iteration=iteration)
            _write_xcom(
                session, **coordinates, key=XCOM_HUMAN_ACTION, value=human_action.model_dump(mode="json")
            )
        sess_data.status = _STATUS_FOR_ACTION[action]
        _write_xcom(session, **coordinates, key=XCOM_AGENT_SESSION, value=sess_data.model_dump(mode="json"))

        resp = _build_session_response(session, **coordinates)
        if resp is None:
            raise HTTPException(status_code=500, detail="Failed to read session after update.")
        return resp

    @hitl_review_app.post(
        "/sessions/feedback",
        response_model=HITLReviewResponse,
        dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.HITL_DETAIL))],
    )
    def submit_feedback(
        body: HumanFeedbackRequest,
        db: SessionDep,
        user: GetUserDep,
        dag_id: str,
        task_id: str,
        run_id: str,
        scope: TaskScopeDep,
    ) -> HITLReviewResponse:
        """Request changes — provide human feedback for the LLM."""
        region_id, map_index = scope
        if not (body.feedback and body.feedback.strip()):
            raise HTTPException(
                status_code=400,
                detail="Feedback is required when requesting changes.",
            )
        return _record_action(
            db,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
            user=user,
            action="changes_requested",
            feedback=body.feedback,
        )

    @hitl_review_app.post(
        "/sessions/approve",
        response_model=HITLReviewResponse,
        dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.HITL_DETAIL))],
    )
    def approve_session(
        db: SessionDep,
        user: GetUserDep,
        dag_id: str,
        task_id: str,
        run_id: str,
        scope: TaskScopeDep,
    ) -> HITLReviewResponse:
        """Approve the current output."""
        region_id, map_index = scope
        return _record_action(
            db,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
            user=user,
            action="approve",
        )

    @hitl_review_app.post(
        "/sessions/reject",
        response_model=HITLReviewResponse,
        dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.HITL_DETAIL))],
    )
    def reject_session(
        db: SessionDep,
        user: GetUserDep,
        dag_id: str,
        task_id: str,
        run_id: str,
        scope: TaskScopeDep,
    ) -> HITLReviewResponse:
        """Reject the output."""
        region_id, map_index = scope
        return _record_action(
            db,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id,
            user=user,
            action="reject",
        )

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
    fastapi_apps: list[FastAPIAppDict] = []
    react_apps: list[ReactAppDict] = []
    if AIRFLOW_V_3_1_PLUS:
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
                "bundle_url": get_bundle_url(_PLUGIN_PREFIX, "main.umd.cjs"),
                "destination": "task_instance",
                "url_route": "hitl-review",
            }
        ]
