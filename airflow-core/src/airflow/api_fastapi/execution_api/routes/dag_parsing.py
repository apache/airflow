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

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Security, status
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagParseResultBody,
    DagParseResultResponse,
)
from airflow.api_fastapi.execution_api.security import CurrentExecutionToken, ExecutionAPIRoute, require_auth
from airflow.dag_processing.collection import (
    DagBundleOwnershipError,
    _reject_other_teams_plugin_classes,
    update_dag_parsing_results_in_db,
    validate_dag_bundle_ownership,
)
from airflow.exceptions import DeserializationError
from airflow.jobs.job import Job
from airflow.models.dag import DagModel
from airflow.models.dag_parse_checkpoint import DagParseCheckpoint
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.sdk.importers.base import DagSourceCode
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG

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
    session.flush()
    return DagParseResultResponse(attempt_id=checkpoint.attempt_id, accepted_at=checkpoint.accepted_at)
