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
"""DAG Processing API sub-app (AIP-92): persistence endpoints for the DAG processor."""

from __future__ import annotations

import logging
from datetime import timedelta

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, update
from sqlalchemy.orm import load_only

from airflow._shared.timezones import timezone
from airflow.api_fastapi.dag_processing.datamodels import (
    BundleStateResponse,
    BundleStateUpdateBody,
    CallbackClaimBody,
    JobCompleteBody,
    JobRegisterBody,
    ParsingResultBody,
    PriorityClaimBody,
    ReconcileBody,
    StaleDagsBody,
)
from airflow.api_fastapi.dag_processing.security import require_dag_processing_auth
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.jobs.job import Job
from airflow.models.asset import remove_references_to_deleted_dags
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ParseImportError
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import prohibit_commit, with_row_locks

log = logging.getLogger(__name__)

router = APIRouter()


def health() -> dict:
    return {"status": "healthy"}


@router.post("/parsing-results", status_code=201)
def persist_parsing_results(body: ParsingResultBody) -> dict:
    """Persist one file's parse results. Mirrors ``persist_parsing_result``."""
    try:
        dags = [LazyDeserializedDAG.model_validate(d) for d in body.serialized_dags]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid serialized_dags payload: {e}")

    import_errors: dict[tuple[str, str], str] = {}
    if body.import_errors:
        import_errors = {(body.bundle_name, rel): err for rel, err in body.import_errors.items()}

    files_parsed: set[tuple[str, str]] | None = None
    if body.relative_fileloc is not None:
        files_parsed = {(body.bundle_name, body.relative_fileloc)}
        files_parsed.update(import_errors.keys())

    warnings = [DagWarning(**warn) for warn in (body.warnings or [])]

    with create_session() as session:
        update_dag_parsing_results_in_db(
            bundle_name=body.bundle_name,
            bundle_version=body.bundle_version,
            version_data=body.version_data,
            dags=dags,
            import_errors=import_errors,
            parse_duration=body.run_duration,
            warnings=set(warnings),
            session=session,
            files_parsed=files_parsed,
        )

    return {"bundle_name": body.bundle_name, "relative_fileloc": body.relative_fileloc}


@router.post("/bundles/{bundle_name}/reconcile")
def reconcile_bundle(bundle_name: str, body: ReconcileBody) -> dict:
    """
    Deactivate DAGs/import-errors for files no longer present in the bundle.

    Mirrors ``deactivate_deleted_dags`` + ``clear_orphaned_import_errors``. These run in
    separate transactions, and import-error cleanup swallows its own errors, so a cleanup
    failure cannot roll back the deactivations (matching the original behaviour).
    """
    observed = set(body.observed_filelocs)

    with create_session() as session:
        deactivated = DagModel.deactivate_deleted_dags(
            bundle_name=bundle_name,
            rel_filelocs=observed,
            session=session,
        )
        if deactivated:
            remove_references_to_deleted_dags(session=session)

    try:
        with create_session() as session:
            errors = session.scalars(
                select(ParseImportError)
                .where(ParseImportError.bundle_name == bundle_name)
                .options(load_only(ParseImportError.filename))
            )
            for error in errors:
                if error.filename not in observed:
                    session.delete(error)
    except Exception:
        log.exception("Error removing old import errors for bundle %s", bundle_name)

    return {"bundle_name": bundle_name, "deactivated": bool(deactivated)}


@router.get("/bundles/{bundle_name}/state", response_model=BundleStateResponse)
def get_bundle_state(bundle_name: str) -> BundleStateResponse:
    """Return a bundle's persisted refresh state (last_refreshed + version)."""
    with create_session() as session:
        row = session.scalar(
            select(DagBundleModel)
            .where(DagBundleModel.name == bundle_name)
            .options(load_only(DagBundleModel.last_refreshed, DagBundleModel.version))
        )
        if row is None:
            return BundleStateResponse(found=False)
        return BundleStateResponse(found=True, last_refreshed=row.last_refreshed, version=row.version)


@router.patch("/bundles/{bundle_name}/state")
def update_bundle_state(bundle_name: str, body: BundleStateUpdateBody) -> dict:
    """Persist a bundle's post-refresh state. Updates ``version`` only when provided."""
    values: dict = {"last_refreshed": body.last_refreshed}
    if body.version is not None:
        values["version"] = body.version
    with create_session() as session:
        session.execute(update(DagBundleModel).where(DagBundleModel.name == bundle_name).values(**values))
    return {"bundle_name": bundle_name}


@router.post("/bundles/sync")
def sync_bundles() -> dict:
    """Sync the configured DAG bundles to the metadata database (server-side)."""
    DagBundlesManager().sync_bundles_to_db()
    return {"synced": True}


@router.post("/stale-dags")
def deactivate_stale_dags(body: StaleDagsBody) -> dict:
    """
    Deactivate DAGs whose files have not been re-parsed within the stale threshold.

    Mirrors ``DagFileProcessorManager.deactivate_stale_dags`` server-side.
    """
    last_parsed = {(e.bundle_name, e.relative_fileloc): e.last_finish_time for e in body.last_parsed}
    to_deactivate: set[str] = set()
    deactivated = 0
    with create_session() as session:
        inactive_bundles = set(
            session.scalars(select(DagBundleModel.name).where(DagBundleModel.active.is_(False))).all()
        )
        rows = session.execute(
            select(
                DagModel.dag_id,
                DagModel.bundle_name,
                DagModel.last_parsed_time,
                DagModel.relative_fileloc,
            ).where(~DagModel.is_stale)
        )
        for row in rows:
            if row.bundle_name in inactive_bundles:
                to_deactivate.add(row.dag_id)
                continue
            last_finish_time = last_parsed.get((row.bundle_name, row.relative_fileloc))
            if last_finish_time and (
                row.last_parsed_time + timedelta(seconds=body.stale_dag_threshold) < last_finish_time
            ):
                to_deactivate.add(row.dag_id)
        if to_deactivate:
            result = session.execute(
                update(DagModel)
                .where(DagModel.dag_id.in_(to_deactivate))
                .values(is_stale=True)
                .execution_options(synchronize_session="fetch")
            )
            deactivated = getattr(result, "rowcount", 0)
    return {"deactivated": deactivated}


@router.post("/purge-warnings")
def purge_inactive_dag_warnings() -> dict:
    """Delete warnings for inactive/stale DAGs (server-side)."""
    DagWarning.purge_inactive_dag_warnings()
    return {"purged": True}


@router.post("/priority-parse-requests/claim")
def claim_priority_parse_requests(body: PriorityClaimBody) -> dict:
    """Claim (select + delete, one transaction) priority parse requests for the given bundles."""
    claimed: list[dict] = []
    with create_session() as session:
        requests = session.scalars(
            select(DagPriorityParsingRequest).where(
                DagPriorityParsingRequest.bundle_name.in_(body.bundle_names)
            )
        )
        for request in requests:
            claimed.append({"bundle_name": request.bundle_name, "relative_fileloc": request.relative_fileloc})
            session.delete(request)
    return {"claimed": claimed}


@router.post("/callbacks/claim")
def claim_callbacks(body: CallbackClaimBody) -> dict:
    """
    Claim callbacks for the given bundles using FOR UPDATE SKIP LOCKED, server-side.

    Mirrors ``DagFileProcessorManager._fetch_callbacks_from_db``. Returns each claimed
    callback's raw ``{req_class, req_data}`` payload so the caller can rebuild the typed
    ``CallbackRequest`` exactly as ``DbCallbackRequest.get_callback_request`` does.
    """
    claimed: list[dict] = []
    with create_session() as session:
        with prohibit_commit(session) as guard:
            query = with_row_locks(
                select(DbCallbackRequest)
                .where(DbCallbackRequest.bundle_name.in_(body.bundle_names))
                .order_by(DbCallbackRequest.priority_weight.desc())
                .limit(body.limit),
                of=DbCallbackRequest,
                session=session,
                skip_locked=True,
            )
            callbacks = [cb[0] if isinstance(cb, tuple) else cb for cb in session.scalars(query)]
            for callback in callbacks:
                claimed.append(callback.data)
                session.delete(callback)
            guard.commit()
    return {"callbacks": claimed}


@router.post("/jobs", status_code=201)
def register_job(body: JobRegisterBody) -> dict:
    """Register the processor's liveness Job row (server-side) and return its id."""
    job = Job()
    job.job_type = body.job_type
    # ``Job()`` defaults hostname/unixname/pid to *this* API server's, but the processor runs in a
    # different process (and usually host). Record the identity it reported so its health check
    # (``airflow jobs check --job-type DagProcessorJob --hostname <host>``) matches this row.
    job.hostname = body.hostname
    if body.unixname:
        job.unixname = body.unixname
    if body.pid is not None:
        job.pid = body.pid
    with create_session() as session:
        job.prepare_for_execution(session=session)
    return {"job_id": job.id}


@router.post("/jobs/{job_id}/heartbeat")
def job_heartbeat(job_id: int) -> dict:
    """Update the processor Job's latest_heartbeat so the health check sees it alive."""
    with create_session() as session:
        job = session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        job.latest_heartbeat = timezone.utcnow()
        session.merge(job)
    return {"alive": True}


@router.post("/jobs/{job_id}/complete")
def complete_job(job_id: int, body: JobCompleteBody) -> dict:
    """Record the processor Job's terminal state and end time."""
    with create_session() as session:
        job = session.get(Job, job_id)
        if job is not None:
            job.end_date = timezone.utcnow()
            job.state = body.state
            session.merge(job)
    return {"completed": True}


def create_dag_processing_api_app() -> FastAPI:
    """Create the DAG Processing API sub-app (mounted at ``/dag-processing``)."""
    app = FastAPI(
        title="Airflow DAG Processing API",
        description="Persistence endpoints for the DAG processor (AIP-92).",
    )

    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        # Mounted sub-apps build their own middleware stack, so without this the parent
        # app never logs unhandled exceptions raised here.
        log.exception("Unhandled exception in DAG Processing API: %s %s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

    # /health stays unauthenticated so external readiness probes work; every persistence
    # endpoint requires a valid DAG-processor token.
    app.add_api_route("/health", health, methods=["GET"])
    app.include_router(router, dependencies=[Depends(require_dag_processing_auth)])
    return app
