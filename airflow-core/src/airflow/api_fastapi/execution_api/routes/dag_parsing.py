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

import hashlib
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Request, Security, status
from sqlalchemy import delete, or_, select
from sqlalchemy.orm import selectinload
from starlette.requests import ClientDisconnect
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    MAX_PUBLICATION_BYTES,
    DagBundleInventoryBody,
    DagBundleInventoryResponse,
    DagBundleStateResponse,
    DagParseResultBody,
    DagParseResultResponse,
    ProcessorWorkAckBody,
    ProcessorWorkClaimBody,
    ProcessorWorkItem,
)
from airflow.api_fastapi.execution_api.security import CurrentExecutionToken, ExecutionAPIRoute, require_auth
from airflow.configuration import conf
from airflow.dag_processing.claims import get_available_work_filter
from airflow.dag_processing.collection import (
    DagBundleOwnershipError,
    DagPublicationValidationError,
    _reject_other_teams_plugin_classes,
    is_source_discovery_failed,
    update_dag_parsing_results_in_db,
    validate_dag_bundle_ownership,
)
from airflow.exceptions import DeserializationError
from airflow.jobs.job import Job
from airflow.models.asset import remove_references_to_deleted_dags
from airflow.models.callback import DagProcessorCallback
from airflow.models.dag import DagModel
from airflow.models.dag_parse_checkpoint import DagParseCheckpoint
from airflow.models.dag_processor_work_claim import DagProcessorWorkClaim
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.errors import ParseImportError
from airflow.sdk.importers.base import DagSourceCode  # noqa: SDK001
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import CallbackState

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from pydantic import BaseModel
    from sqlalchemy.orm import Session
    from starlette.responses import Response
    from starlette.types import Message


class DagParsingRoute(ExecutionAPIRoute):
    """Refuse request bodies over the publication limit before they are read into memory."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def handle_bounded_request(request: Request) -> Response:
            too_large = HTTPException(
                status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"Request body exceeds {MAX_PUBLICATION_BYTES // (1024 * 1024)} MiB",
            )
            declared = request.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > MAX_PUBLICATION_BYTES:
                raise too_large
            body = bytearray()
            try:
                async for chunk in request.stream():
                    body += chunk
                    if len(body) > MAX_PUBLICATION_BYTES:
                        raise too_large
            except ClientDisconnect as error:
                # FastAPI reports a body it could not read as a bad request rather than a server error.
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST, detail="The client disconnected while sending the body"
                ) from error
            replayed = False

            async def receive() -> Message:
                nonlocal replayed
                if replayed:
                    return await request.receive()
                replayed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}

            return await handler(Request(request.scope, receive))

        return handle_bounded_request


router = VersionedAPIRouter(route_class=DagParsingRoute)


def _compute_payload_hash(body: BaseModel) -> str:
    """Hash a request body independently of key order; a non-JSON float raises ValueError."""
    payload = json.dumps(body.model_dump(mode="json"), sort_keys=True, allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _validate_job(job_id: int, token, *, session: Session, for_update: bool = True) -> None:
    if job_id != token.claims.job_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail={"reason": "not_found"})
    query = select(Job).where(Job.id == job_id)
    # Publications lock the Job to serialize checkpoint creation and fence completion or replacement.
    job = session.scalar(query.with_for_update() if for_update else query)
    if job is None or job.session_id != token.id or job.end_date is not None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "job_closed"})


@router.post(
    "/{job_id}/parse-results",
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_403_FORBIDDEN, "The bundle or a Dag's plugin is not granted, or the Job has ended"),
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
            status.HTTP_413_CONTENT_TOO_LARGE,
            status.HTTP_422_UNPROCESSABLE_CONTENT,
        ]
    ),
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)
def publish_parse_result(
    job_id: int, body: DagParseResultBody, *, session: SessionDep, token=CurrentExecutionToken
) -> DagParseResultResponse:
    """Atomically publish a file's results and a receipt, without reading the processor's filesystem."""
    if body.bundle_name not in token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    try:
        payload_hash = _compute_payload_hash(body)
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid JSON value")
    source_key = hashlib.sha256(
        json.dumps(["parse", body.bundle_name, body.relative_fileloc]).encode()
    ).hexdigest()

    _validate_job(job_id, token, session=session)
    checkpoint = session.get(DagParseCheckpoint, (job_id, source_key))
    if checkpoint is not None:
        if checkpoint.attempt_id == body.attempt_id:
            if checkpoint.payload_hash != payload_hash:
                raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "publication_conflict"})
            return DagParseResultResponse(
                attempt_id=checkpoint.attempt_id, accepted_at=checkpoint.accepted_at
            )
        if body.dispatch_sequence <= checkpoint.dispatch_sequence:
            raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "publication_superseded"})

    bundle = session.scalar(
        select(DagBundleModel).where(DagBundleModel.name == body.bundle_name).with_for_update(key_share=True)
    )
    if bundle is None or not bundle.active:
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "bundle_inactive"})
    if body.bundle_revision != bundle.parse_revision or (
        bundle.parse_revision is not None and body.bundle_version != bundle.version
    ):
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "source_changed"})
    dags = [LazyDeserializedDAG(data=document) for document in body.serialized_dags]
    dag_ids = [dag.dag_id for dag in dags]
    existing = session.scalars(
        select(DagModel)
        .where(DagModel.dag_id.in_(dag_ids))
        .order_by(DagModel.dag_id)
        .with_for_update(key_share=True)
    ).all()
    try:
        validate_dag_bundle_ownership(existing, body.bundle_name, session=session)
    except DagBundleOwnershipError as error:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail={"reason": "dag_owned_by_another_bundle"}
        ) from error
    if len(_reject_other_teams_plugin_classes(body.bundle_name, dags, {}, session=session)) != len(dags):
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "plugin_not_granted"})
    try:
        for dag in dags:
            DagSerialization.validate_serialized_dag(dag.data)
    except (DeserializationError, ValueError, TypeError, KeyError) as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid serialized Dag") from error

    # A complete accepted import proves absence; an import error only ages out prior definitions.
    cutoff = timezone.utcnow() - timedelta(seconds=conf.getint("dag_processor", "stale_dag_threshold"))
    source_filter = or_(
        DagModel.relative_fileloc == body.relative_fileloc,
        DagModel.relative_fileloc.startswith(body.relative_fileloc + "/", autoescape=True),
    )
    missing = session.scalars(
        select(DagModel)
        .where(DagModel.bundle_name == body.bundle_name, source_filter, DagModel.dag_id.not_in(dag_ids))
        .order_by(DagModel.dag_id)
        .with_for_update(key_share=True)
    )
    for stored_dag in missing:
        failed = any(
            path == body.relative_fileloc or path == stored_dag.relative_fileloc
            for path in body.import_errors
        )
        if not failed or (stored_dag.last_parsed_time is not None and stored_dag.last_parsed_time < cutoff):
            stored_dag.is_stale = True

    import_errors = {(body.bundle_name, path): error for path, error in body.import_errors.items()}
    files_parsed = {(body.bundle_name, path) for path in [body.relative_fileloc, *body.parsed_definitions]}
    files_parsed.update(import_errors)
    try:
        update_dag_parsing_results_in_db(
            bundle_name=body.bundle_name,
            bundle_version=body.bundle_version,
            version_data=body.version_data,
            dags=dags,
            import_errors=import_errors,
            parse_duration=body.parse_duration,
            warnings={DagWarning(**warning.model_dump()) for warning in body.warnings},
            files_parsed=files_parsed,
            dag_source_codes={
                dag_id: DagSourceCode(
                    source_code=source.source_code
                    if source.source_code is not None
                    else "Source unavailable",
                    language=source.language,
                )
                for dag_id, source in body.source_codes.items()
            },
            atomic=True,
            enforce_bundle_ownership=True,
            resolve_bundle_paths=False,
            session=session,
        )
    except DagPublicationValidationError as error:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"reason": "invalid_dag", "message": str(error)},
        ) from error
    except DagBundleOwnershipError as error:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail={"reason": "dag_owned_by_another_bundle"}
        ) from error
    if checkpoint is None:
        checkpoint = DagParseCheckpoint(job_id=job_id, source_key=source_key)
        session.add(checkpoint)
    checkpoint.attempt_id = body.attempt_id
    checkpoint.dispatch_sequence = body.dispatch_sequence
    checkpoint.payload_hash = payload_hash
    checkpoint.accepted_at = timezone.utcnow()
    checkpoint.bundle_revision = body.bundle_revision
    session.flush()
    return DagParseResultResponse(attempt_id=checkpoint.attempt_id, accepted_at=checkpoint.accepted_at)


@router.get(
    "/{job_id}/bundles",
    responses=create_openapi_http_exception_doc(
        [(status.HTTP_403_FORBIDDEN, "The Job has ended or was replaced"), status.HTTP_404_NOT_FOUND]
    ),
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)
def get_bundles(
    job_id: int, *, session: SessionDep, token=CurrentExecutionToken
) -> list[DagBundleStateResponse]:
    """Read the provisioned catalog without constructing providers on the API server."""
    _validate_job(job_id, token, session=session, for_update=False)
    bundles = session.scalars(
        select(DagBundleModel)
        .options(selectinload(DagBundleModel.teams))
        .where(DagBundleModel.name.in_(token.claims.dag_bundles), DagBundleModel.active.is_(True))
        .order_by(DagBundleModel.name)
    )
    return [
        DagBundleStateResponse(
            name=bundle.name,
            version=bundle.version,
            last_refreshed=bundle.last_refreshed,
            revision=bundle.parse_revision,
            team_name=bundle.team_name,
        )
        for bundle in bundles
    ]


@router.post(
    "/{job_id}/bundles/{bundle_name:path}/inventory",
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_403_FORBIDDEN, "The bundle is not granted, or the Job has ended"),
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
            status.HTTP_413_CONTENT_TOO_LARGE,
        ]
    ),
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)
def publish_inventory(
    job_id: int,
    bundle_name: str,
    body: DagBundleInventoryBody,
    *,
    session: SessionDep,
    token=CurrentExecutionToken,
) -> DagBundleInventoryResponse:
    """Reconcile observed sources in one bundle, preserving sources whose discovery failed."""
    if bundle_name not in token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    source_key = hashlib.sha256(json.dumps(["inventory", bundle_name]).encode()).hexdigest()
    payload_hash = _compute_payload_hash(body)
    _validate_job(job_id, token, session=session)
    checkpoint = session.get(DagParseCheckpoint, (job_id, source_key))
    if checkpoint is not None:
        if checkpoint.attempt_id == body.attempt_id:
            if checkpoint.payload_hash != payload_hash or checkpoint.bundle_revision is None:
                raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "publication_conflict"})
            return DagBundleInventoryResponse(
                attempt_id=checkpoint.attempt_id,
                accepted_at=checkpoint.accepted_at,
                revision=checkpoint.bundle_revision,
            )
        if body.dispatch_sequence <= checkpoint.dispatch_sequence:
            raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "publication_superseded"})
    bundle = session.scalar(
        select(DagBundleModel).where(DagBundleModel.name == bundle_name).with_for_update(key_share=True)
    )
    if bundle is None or not bundle.active:
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "bundle_inactive"})
    if body.expected_revision != bundle.parse_revision:
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "source_changed"})
    inventory_hash = hashlib.sha256(
        json.dumps([body.version, sorted(body.files), sorted(body.failed_sources)]).encode()
    ).hexdigest()
    if bundle.inventory_hash != inventory_hash or bundle.parse_revision is None:
        bundle.parse_revision = uuid7()
        bundle.inventory_hash = inventory_hash
    bundle.version = body.version
    bundle.last_refreshed = timezone.utcnow()
    present = set(body.files)
    failed_sources = set(body.failed_sources)

    # Like DagModel.deactivate_deleted_dags, a NULL location (a 2.x row never parsed since) is not present.
    def is_removed(relative_fileloc: str | None) -> bool:
        return relative_fileloc not in present and not is_source_discovery_failed(
            relative_fileloc, failed_sources
        )

    # Lock only the Dags being deactivated, so the rest of the bundle stays available to the scheduler.
    removed = sorted(
        dag_id
        for dag_id, relative_fileloc in session.execute(
            select(DagModel.dag_id, DagModel.relative_fileloc).where(
                DagModel.bundle_name == bundle_name, DagModel.is_stale.is_(False)
            )
        )
        if is_removed(relative_fileloc)
    )
    deactivated = False
    for start in range(0, len(removed), 500):
        for dag in session.scalars(
            select(DagModel)
            .where(
                DagModel.dag_id.in_(removed[start : start + 500]),
                DagModel.bundle_name == bundle_name,
                DagModel.is_stale.is_(False),
            )
            .order_by(DagModel.dag_id)
            .with_for_update(key_share=True)
        ):
            if is_removed(dag.relative_fileloc):
                dag.is_stale = True
                deactivated = True
    if deactivated:
        session.flush()
        remove_references_to_deleted_dags(session=session)
    error_ids = [
        error.id
        for error in session.scalars(
            select(ParseImportError).where(ParseImportError.bundle_name == bundle_name)
        )
        if is_removed(error.filename)
    ]
    for start in range(0, len(error_ids), 500):
        session.execute(
            delete(ParseImportError).where(ParseImportError.id.in_(error_ids[start : start + 500]))
        )
    if checkpoint is None:
        checkpoint = DagParseCheckpoint(job_id=job_id, source_key=source_key)
        session.add(checkpoint)
    checkpoint.attempt_id = body.attempt_id
    checkpoint.dispatch_sequence = body.dispatch_sequence
    checkpoint.payload_hash = payload_hash
    checkpoint.accepted_at = bundle.last_refreshed
    checkpoint.bundle_revision = bundle.parse_revision
    session.flush()
    return DagBundleInventoryResponse(
        attempt_id=checkpoint.attempt_id,
        accepted_at=checkpoint.accepted_at,
        revision=bundle.parse_revision,
    )


@router.post(
    "/{job_id}/requested-work/{kind}/claim",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)
def claim_work(
    job_id: int,
    kind: Literal["callbacks", "priority"],
    body: ProcessorWorkClaimBody,
    *,
    session: SessionDep,
    token=CurrentExecutionToken,
) -> list[ProcessorWorkItem]:
    """Replay outstanding work without letting an old or empty claim take new requests."""
    if not set(body.bundle_names) <= token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    _validate_job(job_id, token, session=session)
    model = DagProcessorCallback if kind == "callbacks" else DagPriorityParsingRequest
    receipt = session.get(DagProcessorWorkClaim, (job_id, kind))
    payload_hash = _compute_payload_hash(body)
    replaying = receipt is not None and body.claim_sequence <= receipt.claim_sequence
    if receipt is not None and replaying:
        if body.claim_sequence < receipt.claim_sequence:
            return []
        if receipt.claim_id != body.claim_id or receipt.payload_hash != payload_hash:
            raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "claim_conflict"})
        query = select(model).where(
            model.processor_job_id == job_id, model.processor_claim_id == body.claim_id
        )
    else:
        if receipt is None:
            receipt = DagProcessorWorkClaim(job_id=job_id, kind=kind)
            session.add(receipt)
        receipt.claim_id = body.claim_id
        receipt.claim_sequence = body.claim_sequence
        receipt.payload_hash = payload_hash
        ready = select(DagBundleModel.name).where(DagBundleModel.active.is_(True))
        query = select(model).where(
            model.bundle_name.in_(body.bundle_names),
            model.bundle_name.in_(ready),
            get_available_work_filter(model.processor_job_id),
        )
        if kind == "callbacks":
            query = query.where(
                or_(DagProcessorCallback.state.is_(None), DagProcessorCallback.state == CallbackState.QUEUED)
            )
    if kind == "callbacks":
        query = query.order_by(DagProcessorCallback.priority_weight.desc(), DagProcessorCallback.id)
    else:
        query = query.order_by(DagPriorityParsingRequest.id)
    if not replaying:
        query = with_row_locks(query.limit(body.limit), session=session, skip_locked=True)
    replay = list(
        cast("list[DagProcessorCallback | DagPriorityParsingRequest]", session.scalars(query).all())
    )
    if not replaying:
        for work in replay:
            work.processor_job_id = job_id
            work.processor_claim_id = body.claim_id
            if isinstance(work, DagProcessorCallback):
                work.state = CallbackState.QUEUED
    result = []
    for work in replay:
        if work.bundle_name is None or work.bundle_name not in token.claims.dag_bundles:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
        if isinstance(work, DagProcessorCallback):
            request = work.get_callback_request()
            path = request.filepath
            callback = request.to_json()
        else:
            path, callback = work.relative_fileloc, None
        result.append(
            ProcessorWorkItem(
                id=str(work.id),
                claim_id=body.claim_id,
                bundle_name=work.bundle_name,
                relative_fileloc=path,
                callback=callback,
            )
        )
    return result


@router.post(
    "/{job_id}/requested-work/{kind}/{work_id}/ack",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
            status.HTTP_422_UNPROCESSABLE_CONTENT,
        ]
    ),
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)
def acknowledge_work(
    job_id: int,
    kind: Literal["callbacks", "priority"],
    work_id: str,
    body: ProcessorWorkAckBody,
    *,
    session: SessionDep,
    token=CurrentExecutionToken,
) -> None:
    """Reject acknowledgments that could consume a replacement owner's claim."""
    _validate_job(job_id, token, session=session)
    model = DagProcessorCallback if kind == "callbacks" else DagPriorityParsingRequest
    try:
        key = UUID(work_id) if kind == "callbacks" else work_id
    except ValueError as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid callback ID") from error
    work = cast(
        "DagProcessorCallback | DagPriorityParsingRequest | None",
        session.scalar(select(model).where(model.id == key).with_for_update()),
    )
    if work is None:
        return
    if work.bundle_name not in token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    if (work.processor_job_id, work.processor_claim_id) != (job_id, body.claim_id):
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "claim_replaced"})
    # Callback runners log individual failures without failing the child process;
    # an acknowledgment confirms delivery, not successful execution of user code.
    session.delete(work)
