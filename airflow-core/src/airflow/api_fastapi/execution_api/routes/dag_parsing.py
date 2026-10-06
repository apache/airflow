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
from typing import Literal, cast
from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Security, status
from sqlalchemy import delete, or_, select
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
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
from airflow.dag_processing.collection import (
    DagBundleOwnershipError,
    _reject_other_teams_plugin_classes,
    update_dag_parsing_results_in_db,
    validate_dag_bundle_ownership,
)
from airflow.exceptions import DeserializationError
from airflow.jobs.job import Job
from airflow.models.callback import DagProcessorCallback
from airflow.models.dag import DagModel
from airflow.models.dag_parse_checkpoint import DagParseCheckpoint
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.errors import ParseImportError
from airflow.sdk.importers.base import DagSourceCode
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import CallbackState

router = VersionedAPIRouter(route_class=ExecutionAPIRoute)
MAX_PARSE_RESULT_BYTES = 16 * 1024 * 1024


@router.post(
    "/{job_id}/parse-results",
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)
def publish_parse_result(
    job_id: int, body: DagParseResultBody, *, session: SessionDep, token=CurrentExecutionToken
) -> DagParseResultResponse:
    """Atomically publish a file's results and a receipt, without reading the processor's filesystem."""
    if job_id != token.claims.job_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail={"reason": "not_found"})
    if body.bundle_name not in token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    try:
        payload = json.dumps(body.model_dump(mode="json"), sort_keys=True, allow_nan=False).encode()
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid JSON value")
    if len(payload) > MAX_PARSE_RESULT_BYTES:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, detail="Parse result exceeds 16 MiB")
    payload_hash = hashlib.sha256(payload).hexdigest()
    source_key = hashlib.sha256(json.dumps([body.bundle_name, body.relative_fileloc]).encode()).hexdigest()

    # The Job lock serializes checkpoint creation and fences completion/replacement in the same transaction.
    job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
    if job is None or job.session_id != token.id or job.end_date is not None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "job_closed"})
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
        select(DagBundleModel).where(DagBundleModel.name == body.bundle_name).with_for_update()
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
        select(DagModel).where(DagModel.dag_id.in_(dag_ids)).order_by(DagModel.dag_id).with_for_update()
    ).all()
    try:
        validate_dag_bundle_ownership(existing, body.bundle_name, session=session)
    except DagBundleOwnershipError as error:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail={"reason": "dag_owned_by_another_bundle"}
        ) from error
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
        .with_for_update()
    )
    for stored_dag in missing:
        failed = any(
            path == body.relative_fileloc or path == stored_dag.relative_fileloc
            for path in body.import_errors
        )
        if not failed or (stored_dag.last_parsed_time is not None and stored_dag.last_parsed_time < cutoff):
            stored_dag.is_stale = True
    if len(_reject_other_teams_plugin_classes(body.bundle_name, dags, {}, session=session)) != len(dags):
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "plugin_not_granted"})
    try:
        for dag in dags:
            DagSerialization.validate_serialized_dag(dag.data)
    except (DeserializationError, ValueError, TypeError, KeyError) as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid serialized Dag") from error

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
                path: DagSourceCode(
                    source_code=source.source_code
                    if source.source_code is not None
                    else "Source unavailable",
                    language=source.language,
                )
                for path, source in body.source_codes.items()
            },
            atomic=True,
            enforce_bundle_ownership=True,
            session=session,
        )
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


@router.get("/{job_id}/bundles", dependencies=[Security(require_auth, scopes=["token:dag_processor"])])
def get_bundles(
    job_id: int, *, session: SessionDep, token=CurrentExecutionToken
) -> list[DagBundleStateResponse]:
    """Read the provisioned catalog without constructing providers on the API server."""
    _validate_job(job_id, token, session=session)
    bundles = session.scalars(
        select(DagBundleModel)
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


def _validate_job(job_id, token, *, session):
    if job_id != token.claims.job_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail={"reason": "not_found"})
    job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
    if job is None or job.session_id != token.id or job.end_date is not None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "job_closed"})


@router.post(
    "/{job_id}/bundles/{bundle_name:path}/inventory",
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
    """Accept a complete inventory and reconcile only that bundle, atomically."""
    if bundle_name not in token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    _validate_job(job_id, token, session=session)
    source_key = hashlib.sha256(json.dumps(["inventory", bundle_name]).encode()).hexdigest()
    payload_hash = hashlib.sha256(body.model_dump_json().encode()).hexdigest()
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
        select(DagBundleModel).where(DagBundleModel.name == bundle_name).with_for_update()
    )
    if bundle is None or not bundle.active:
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "bundle_inactive"})
    if body.expected_revision != bundle.parse_revision:
        raise HTTPException(status.HTTP_409_CONFLICT, detail={"reason": "source_changed"})
    inventory_hash = hashlib.sha256(json.dumps([body.version, sorted(body.files)]).encode()).hexdigest()
    if bundle.inventory_hash != inventory_hash or bundle.parse_revision is None:
        bundle.parse_revision = uuid7()
        bundle.inventory_hash = inventory_hash
    bundle.version = body.version
    bundle.last_refreshed = timezone.utcnow()
    present = set(body.files)
    for dag in session.scalars(
        select(DagModel)
        .where(DagModel.bundle_name == bundle_name, DagModel.is_stale.is_(False))
        .order_by(DagModel.dag_id)
        .with_for_update()
    ):
        if dag.relative_fileloc is not None and dag.relative_fileloc not in present:
            dag.is_stale = True
    error_ids = [
        error.id
        for error in session.scalars(
            select(ParseImportError).where(ParseImportError.bundle_name == bundle_name)
        )
        if error.filename not in present
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
    """Retain requests until acknowledged, and replay a lost claim response."""
    if not set(body.bundle_names) <= token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail={"reason": "bundle_not_granted"})
    _validate_job(job_id, token, session=session)
    model = DagProcessorCallback if kind == "callbacks" else DagPriorityParsingRequest
    replay = list(
        cast(
            "list[DagProcessorCallback | DagPriorityParsingRequest]",
            session.scalars(
                select(model)
                .where(model.processor_job_id == job_id, model.processor_claim_id == body.claim_id)
                .order_by(model.id)
            ).all(),
        )
    )
    if not replay:
        ready = select(DagBundleModel.name).where(DagBundleModel.active.is_(True))
        retired = select(Job.id).where(Job.end_date.is_not(None))
        query = select(model).where(
            model.bundle_name.in_(body.bundle_names),
            model.bundle_name.in_(ready),
            or_(model.processor_job_id.is_(None), model.processor_job_id.in_(retired)),
        )
        if kind == "callbacks":
            query = query.where(
                or_(DagProcessorCallback.state.is_(None), DagProcessorCallback.state == CallbackState.QUEUED)
            ).order_by(DagProcessorCallback.priority_weight.desc(), DagProcessorCallback.id)
        else:
            query = query.order_by(DagPriorityParsingRequest.id)
        replay = list(
            cast(
                "list[DagProcessorCallback | DagPriorityParsingRequest]",
                session.scalars(
                    with_row_locks(query.limit(body.limit), session=session, skip_locked=True)
                ).all(),
            )
        )
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
    # Callback runners log individual failures without failing the child process. This
    # acknowledges delivery, preserving the existing consume-after-processing semantics.
    session.delete(work)
