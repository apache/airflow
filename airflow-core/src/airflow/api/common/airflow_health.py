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
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import _get_bundle_config_source
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import EXECUTOR_JOB_TYPE, HealthCheckResult, Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.executors.executor_utils import ExecutorName

log = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Aggregate health of a component: whether it has at least one live instance."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class DetailedHealthStatus(str, Enum):
    """How much of a component's work has a live instance covering it."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"


@provide_session
def get_jobs_health(job_type: str, *, session: Session = NEW_SESSION) -> list[Job]:
    """
    Return live unfinished jobs and the latest unfinished job of the given job type, ordered by heartbeat.

    Keep the latest job even when dead so health responses retain its heartbeat.
    """
    unfinished = Job.end_date.is_(None)
    # Two separate queries rather than one ``id IN (... UNION ALL ...)``: MySQL cannot semijoin that
    # form and runs it as a dependent subquery per ``job`` row, which scales with job history.
    live_jobs = session.scalars(
        select(Job).where(Job.job_type == job_type, unfinished, Job.is_alive_clause([job_type]))
    )
    latest_job = session.scalar(
        select(Job).where(Job.job_type == job_type, unfinished).order_by(Job.latest_heartbeat.desc()).limit(1)
    )
    jobs = {job.id: job for job in live_jobs}
    if latest_job is not None:
        jobs.setdefault(latest_job.id, latest_job)
    return sorted(
        jobs.values(),
        key=lambda job: job.latest_heartbeat or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


def _job_instance_health(job: Job, heartbeat_field_name: str) -> dict[str, Any]:
    heartbeat = job.latest_heartbeat.isoformat() if job.latest_heartbeat else None
    return {
        "hostname": job.hostname,
        heartbeat_field_name: heartbeat,
    }


def _live_jobs(jobs: list[Job]) -> list[Job]:
    """
    Narrow unfinished job rows down to the replicas that are actually running.

    ``instances`` describes the deployment as it is now, so a row left behind by a replica that
    never got to write its ``end_date`` must not be listed: the host it names is gone, and under an
    orchestrator that assigns a fresh hostname per restart it will never come back. The unfinished
    rows are still what ``status`` and ``latest_*_heartbeat`` are derived from, so how long a dead
    component has been silent stays visible even once it drops out of ``instances``.
    """
    return [job for job in jobs if job.is_alive()]


def _legacy_status(jobs: list[Job]) -> HealthStatus:
    """Top-level status: healthy if any instance is alive."""
    return HealthStatus.HEALTHY if any(job.is_alive() for job in jobs) else HealthStatus.UNHEALTHY


def _triggerer_instance_health(job: Job) -> dict[str, Any]:
    return {
        **_job_instance_health(job, "latest_triggerer_heartbeat"),
        "team_names": job.team_names,
    }


def _dag_processor_instance_health(job: Job) -> dict[str, Any]:
    return {
        **_job_instance_health(job, "latest_dag_processor_heartbeat"),
        "bundle_names": job.bundle_names,
    }


@lru_cache
def _configured_executor_names() -> dict[str, ExecutorName]:
    """
    Map every configured executor's health key to its ``ExecutorName``, for display purposes.

    Cached for the same reason :func:`_configured_executor_keys` is: the configured executors can't
    change while the process runs.
    """
    try:
        return {repr(name): name for name in ExecutorLoader.get_executor_names(validate_teams=False)}
    except Exception:
        # A health probe must not fail on malformed executor config; the row's own key is still shown.
        log.warning("Could not read the executor configuration", exc_info=True)
        return {}


def _executor_instance_health(job: Job) -> dict[str, Any]:
    # job.executor_class is the internal identity key (e.g. ":CeleryExecutor:"), not the name the user
    # configured. Reporting the alias or module path they wrote in [core] executor instead is a better
    # user experience; the key itself is only shown as a fallback, e.g. for a since-reconfigured executor.
    name = _configured_executor_names().get(job.executor_class) if job.executor_class else None
    return {
        **_job_instance_health(job, "latest_executor_heartbeat"),
        "executor": (name.alias or name.module_path) if name else job.executor_class,
        "team_names": job.team_names,
        "health_check_result": job.health_check_result,
    }


def _live_executor_jobs(jobs: list[Job], live_scheduler_ids: set[int]) -> list[Job]:
    """
    Narrow executor rows down to live instances whose owning scheduler is also still alive.

    A row can stay technically alive for a little while after its scheduler process has already
    died (its heartbeat hasn't crossed the staleness threshold yet), so liveness on the row alone
    isn't enough; cross-checking the owner avoids briefly reporting a dead scheduler's executor as
    healthy. ``live_scheduler_ids`` comes from the caller's own scheduler job query (e.g.
    :func:`get_airflow_health`) rather than this re-querying them.
    """
    return [job for job in jobs if job.is_alive() and job.owner_job_id in live_scheduler_ids]


# ``detailed_status`` answers "is every part of this component's work being done", which needs a
# denominator. Counting job rows cannot supply one: ``end_date`` is only written by a cooperative
# shutdown, so a replica lost to SIGKILL, an OOM kill, or a node eviction leaves an unfinished row
# behind forever and a restarted replica adds a second one. The denominator is therefore taken from
# the declared work partition instead, which is unaffected by how replicas come and go:
#
# * Dag processor -- the declared bundles, or the reconciled active bundles when reload is enabled.
# * Triggerer -- the team scopes those bundles declare, since a triggerer only picks up triggers for
#   its own team (see ``Trigger.ids_for_triggerer``).
# * Scheduler -- schedulers are symmetric, so there is no partition and no partial state to report.


def _configured_bundle_teams() -> dict[str, str | None]:
    """Get bundle coverage without running a deployment command or secrets lookup on a health request."""
    try:
        return _get_bundle_config_source().get_bundle_teams()
    except Exception:
        # A health probe must not fail on malformed bundle config; callers fall back to liveness only.
        log.warning("Could not read the Dag bundle configuration", exc_info=True)
        return {}


def _liveness_status(jobs: list[Job]) -> DetailedHealthStatus:
    """Status for a component with no declared work partition: one live replica covers everything."""
    return DetailedHealthStatus.HEALTHY if any(job.is_alive() for job in jobs) else DetailedHealthStatus.DOWN


def _coverage_status(expected: set[Any], covered: set[Any]) -> DetailedHealthStatus:
    """Status from how much of a component's declared work partition its live replicas cover."""
    if not expected - covered:
        return DetailedHealthStatus.HEALTHY
    if expected & covered:
        return DetailedHealthStatus.DEGRADED
    return DetailedHealthStatus.DOWN


def _dag_processor_detailed_status(jobs: list[Job]) -> DetailedHealthStatus:
    """Status from bundle coverage, which partitions processor work with or without multi-team mode."""
    expected = set(_configured_bundle_teams())
    if not expected:
        return _liveness_status(jobs)

    covered: set[str] = set()
    for job in jobs:
        if job.is_alive():
            # A processor started without ``--bundle-name`` parses every configured bundle.
            covered |= expected if not job.bundle_names else expected & set(job.bundle_names)
    return _coverage_status(expected, covered)


@lru_cache
def _configured_executor_keys() -> set[str]:
    """
    Return the key each configured executor that implements a health check would be recorded under.

    Most executors don't override :meth:`~airflow.executors.base_executor.BaseExecutor.get_health`,
    so this deliberately excludes them rather than counting every configured executor: an executor
    that was never going to report anything isn't a gap in coverage, and a deployment where nothing
    implements a health check should see no ``executor`` block at all in ``/monitor/health``, not an
    ``unhealthy`` one.

    Also empty when ``executor_health_check_interval`` is 0: the scheduler records no ``ExecutorJob``
    rows in that case either, so nothing would ever cover an expected key and ``/monitor/health`` would
    report the executor DOWN forever instead of omitting the block.

    Cached: this calls :meth:`~airflow.executors.executor_loader.ExecutorLoader.import_executor_cls`,
    which imports every configured executor class, and the configured executors can't change while the
    process runs. Without caching, an API server would redo those imports -- and reproduce any import
    failure's warning and traceback -- on every single call to ``/monitor/health``. Team validation is
    skipped (``validate_teams=False``), the same as ``connections.py`` does, so a database hiccup on
    the first call can't get cached as "no executors".
    """
    try:
        if conf.getfloat("scheduler", "executor_health_check_interval", fallback=30.0) <= 0:
            return set()
        names = ExecutorLoader.get_executor_names(validate_teams=False)
    except Exception:
        # A health probe must not fail on malformed executor config; callers fall back to liveness only.
        log.warning("Could not read the executor configuration", exc_info=True)
        return set()

    keys = set()
    for name in names:
        try:
            executor_cls, _ = ExecutorLoader.import_executor_cls(name)
        except Exception:
            log.warning("Could not import configured executor %s", name.module_path, exc_info=True)
            continue
        if executor_cls.overrides_get_health():
            keys.add(repr(name))
    return keys


def _executor_detailed_status(jobs: list[Job], live_scheduler_ids: set[int]) -> DetailedHealthStatus:
    """
    Status from configured-executor coverage: each configured executor is its own partition.

    A shared executor backend (e.g. one Celery broker) can be reachable from one scheduler and not
    another, so coverage is decided per live, currently-owned row rather than by any single row for
    that executor key: one scheduler finding it down is real signal, not noise an unrelated healthy
    row should paper over.

    ``live_scheduler_ids`` is forwarded to :func:`_live_executor_jobs`; see its docstring.
    """
    expected = _configured_executor_keys()
    live_jobs = _live_executor_jobs(jobs, live_scheduler_ids)
    if not expected:
        return _liveness_status(live_jobs)

    covered: set[str] = set()
    unhealthy: set[str] = set()
    seen: set[str] = set()
    for job in live_jobs:
        if not job.executor_class or job.executor_class not in expected:
            continue
        seen.add(job.executor_class)
        if job.health_check_result == HealthCheckResult.HEALTHY:
            covered.add(job.executor_class)
        elif job.health_check_result == HealthCheckResult.UNHEALTHY:
            unhealthy.add(job.executor_class)
    expected = expected - (seen - covered - unhealthy)
    if not expected:
        return _liveness_status(live_jobs)
    covered -= unhealthy
    return _coverage_status(expected, covered)


def _triggerer_detailed_status(jobs: list[Job]) -> DetailedHealthStatus:
    """Status from team coverage, since a triggerer only picks up triggers for its own team."""
    if not conf.getboolean("core", "multi_team"):
        # Outside multi-team mode no team filter is applied, so any live triggerer serves every trigger.
        return _liveness_status(jobs)

    # ``None`` is a scope in its own right: triggers from bundles that declare no team are only picked
    # up by a triggerer started without ``--team-name``, so it must stay in the expected set.
    expected = set(_configured_bundle_teams().values())
    if not expected:
        return _liveness_status(jobs)

    covered: set[str | None] = set()
    for job in jobs:
        if job.is_alive():
            # An unscoped triggerer has no ``team_names``, it covers the team-less
            # scope, represented by ``None`` in both ``expected`` and ``covered``.
            covered |= set(job.team_names) or {None}
    return _coverage_status(expected, covered)


def get_airflow_health() -> dict[str, Any]:
    """Get the health for Airflow metadatabase, scheduler, triggerer, dag processor, and executors."""
    metadatabase_status = HealthStatus.HEALTHY

    latest_scheduler_heartbeat = None
    latest_triggerer_heartbeat = None
    latest_dag_processor_heartbeat = None
    latest_executor_heartbeat = None

    scheduler_instances: list[dict[str, Any]] | None = None
    triggerer_instances: list[dict[str, Any]] | None = None
    dag_processor_instances: list[dict[str, Any]] | None = None
    executor_instances: list[dict[str, Any]] | None = None

    scheduler_status = HealthStatus.UNHEALTHY
    triggerer_status = HealthStatus.UNHEALTHY
    dag_processor_status = HealthStatus.UNHEALTHY
    executor_status = HealthStatus.UNHEALTHY

    scheduler_detailed_status = DetailedHealthStatus.DOWN
    triggerer_detailed_status = DetailedHealthStatus.DOWN
    dag_processor_detailed_status = DetailedHealthStatus.DOWN
    executor_detailed_status = DetailedHealthStatus.DOWN

    scheduler_jobs: list[Job] = []
    try:
        scheduler_jobs = get_jobs_health(SchedulerJobRunner.job_type)
        scheduler_status = _legacy_status(scheduler_jobs)
        scheduler_detailed_status = _liveness_status(scheduler_jobs)
        if scheduler_jobs and scheduler_jobs[0].latest_heartbeat:
            latest_scheduler_heartbeat = scheduler_jobs[0].latest_heartbeat.isoformat()
        if live_scheduler_jobs := _live_jobs(scheduler_jobs):
            scheduler_instances = [
                _job_instance_health(job, "latest_scheduler_heartbeat") for job in live_scheduler_jobs
            ]
    except Exception:
        metadatabase_status = HealthStatus.UNHEALTHY

    try:
        triggerer_jobs = get_jobs_health(TriggererJobRunner.job_type)
        triggerer_status = _legacy_status(triggerer_jobs)
        triggerer_detailed_status = _triggerer_detailed_status(triggerer_jobs)
        if triggerer_jobs and triggerer_jobs[0].latest_heartbeat:
            latest_triggerer_heartbeat = triggerer_jobs[0].latest_heartbeat.isoformat()
        if live_triggerer_jobs := _live_jobs(triggerer_jobs):
            triggerer_instances = [_triggerer_instance_health(job) for job in live_triggerer_jobs]
    except Exception:
        metadatabase_status = HealthStatus.UNHEALTHY
        triggerer_status = HealthStatus.UNHEALTHY
        triggerer_detailed_status = DetailedHealthStatus.DOWN

    try:
        dag_processor_jobs = get_jobs_health(DagProcessorJobRunner.job_type)
        dag_processor_status = _legacy_status(dag_processor_jobs)
        dag_processor_detailed_status = _dag_processor_detailed_status(dag_processor_jobs)
        if dag_processor_jobs and dag_processor_jobs[0].latest_heartbeat:
            latest_dag_processor_heartbeat = dag_processor_jobs[0].latest_heartbeat.isoformat()
        if live_dag_processor_jobs := _live_jobs(dag_processor_jobs):
            dag_processor_instances = [_dag_processor_instance_health(job) for job in live_dag_processor_jobs]
    except Exception:
        metadatabase_status = HealthStatus.UNHEALTHY
        dag_processor_status = HealthStatus.UNHEALTHY
        dag_processor_detailed_status = DetailedHealthStatus.DOWN

    # Omitted entirely, not just reported unhealthy, when no configured executor implements a health
    # check: a reporting mechanism nothing has opted into isn't a gap in coverage, and showing it as
    # "unhealthy" on every deployment that hasn't adopted it yet would be a false alarm, not a signal.
    report_executor_health = bool(_configured_executor_keys())
    if report_executor_health:
        try:
            executor_jobs = get_jobs_health(EXECUTOR_JOB_TYPE)
            # Reuse scheduler_jobs fetched above instead of re-querying the same rows.
            live_scheduler_ids = {job.id for job in scheduler_jobs if job.is_alive()}
            executor_detailed_status = _executor_detailed_status(executor_jobs, live_scheduler_ids)
            executor_status = (
                HealthStatus.UNHEALTHY
                if executor_detailed_status == DetailedHealthStatus.DOWN
                else HealthStatus.HEALTHY
            )
            if executor_jobs and executor_jobs[0].latest_heartbeat:
                latest_executor_heartbeat = executor_jobs[0].latest_heartbeat.isoformat()
            if live_executor_jobs := _live_executor_jobs(executor_jobs, live_scheduler_ids):
                executor_instances = [_executor_instance_health(job) for job in live_executor_jobs]
        except Exception:
            metadatabase_status = HealthStatus.UNHEALTHY
            executor_status = HealthStatus.UNHEALTHY
            executor_detailed_status = DetailedHealthStatus.DOWN

    airflow_health_status = {
        "metadatabase": {"status": metadatabase_status},
        "scheduler": {
            "status": scheduler_status,
            "latest_scheduler_heartbeat": latest_scheduler_heartbeat,
            "detailed_status": scheduler_detailed_status,
            "instances": scheduler_instances,
        },
        "triggerer": {
            "status": triggerer_status,
            "latest_triggerer_heartbeat": latest_triggerer_heartbeat,
            "detailed_status": triggerer_detailed_status,
            "instances": triggerer_instances,
        },
        "dag_processor": {
            "status": dag_processor_status,
            "latest_dag_processor_heartbeat": latest_dag_processor_heartbeat,
            "detailed_status": dag_processor_detailed_status,
            "instances": dag_processor_instances,
        },
    }

    if report_executor_health:
        airflow_health_status["executor"] = {
            "status": executor_status,
            "latest_executor_heartbeat": latest_executor_heartbeat,
            "detailed_status": executor_detailed_status,
            "instances": executor_instances,
        }

    return airflow_health_status
