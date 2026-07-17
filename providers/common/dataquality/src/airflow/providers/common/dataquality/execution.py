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
"""Helpers for running data quality rules from operators or custom Python tasks."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.dataquality.assets import DQ_RESULT_EXTRA_KEY
from airflow.providers.common.dataquality.backends import get_backend_from_config
from airflow.providers.common.dataquality.engines.sql import SQLDQEngine
from airflow.providers.common.dataquality.results import (
    ERROR,
    FAIL,
    PASS,
    WARN,
    DQRun,
    RuleResult,
    build_summary,
)
from airflow.providers.common.dataquality.rules import RuleSet, describe_rule

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.providers.common.dataquality.engines.sql import Observation
    from airflow.providers.common.dataquality.rules.rule import Condition, Dimension
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.sdk import Asset, Context

log = logging.getLogger(__name__)

RulesetArg = RuleSet | dict[str, Any] | str


@dataclass(frozen=True)
class DataQualityResult:
    """Evaluated data quality results before Airflow task metadata is attached."""

    ruleset: RuleSet
    table: str
    results: tuple[RuleResult, ...]
    started_at: str
    finished_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ruleset": self.ruleset.name,
            "table": self.table,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "results": [result.to_dict() for result in self.results],
        }


def run_quality_checks(
    *,
    ruleset: RulesetArg,
    table: str,
    conn_id: str | None = None,
    hook: DbApiHook | None = None,
    hook_params: dict[str, Any] | None = None,
    partition_clause: str | None = None,
) -> DataQualityResult:
    """
    Run data quality rules through a ``DbApiHook``-compatible connection.

    Use this from custom Python tasks when the standard
    :class:`~airflow.providers.common.dataquality.operators.dq_check.DQCheckOperator`
    does not fit the task shape.
    """
    resolved_ruleset = _resolve_ruleset(ruleset)
    resolved_hook = hook if hook is not None else _get_hook(conn_id, hook_params)
    started_at = datetime.now(tz=timezone.utc).isoformat()
    observations = SQLDQEngine(resolved_hook).measure(
        ruleset=resolved_ruleset,
        table=table,
        partition_clause=partition_clause,
    )
    finished_at = datetime.now(tz=timezone.utc).isoformat()
    return DataQualityResult(
        ruleset=resolved_ruleset,
        table=table,
        results=tuple(_evaluate_observation(observation) for observation in observations),
        started_at=started_at,
        finished_at=finished_at,
    )


def persist_quality_results(
    result: DataQualityResult,
    *,
    context: Context,
    outlets: Sequence[Asset] | None = None,
) -> dict[str, Any]:
    """
    Persist data quality results for the current task and return the run summary.

    When ``[common.dataquality] results_path`` is not configured, this still returns
    the same summary and skips writing history. When ``outlets`` are supplied, the
    summary is also attached to outlet asset events for ``require_quality()``.
    """
    run = _build_run_from_context(
        context=context,
        ruleset=result.ruleset,
        table=result.table,
        outlets=outlets or (),
        started_at=result.started_at,
        finished_at=result.finished_at,
    )
    results = list(result.results)
    summary = build_summary(run=run, results=results)

    backend = get_backend_from_config()
    if backend is None:
        log.info("No [common.dataquality] results_path configured; skipping results persistence")
    else:
        # Persistence is best-effort: an unreachable results store leaves a gap in
        # history but must not change the outcome of the check itself.
        try:
            backend.write_run(run=run, results=results)
        except Exception:
            log.exception("Failed to persist data quality results; continuing")

    if outlets:
        _attach_to_outlet_events(context, outlets, summary)

    return summary


def _get_hook(conn_id: str | None, hook_params: dict[str, Any] | None) -> DbApiHook:
    if conn_id is None:
        raise ValueError("Either conn_id or hook is required")
    connection = BaseHook.get_connection(conn_id)
    return connection.get_hook(hook_params=hook_params)


def _resolve_ruleset(ruleset: RulesetArg) -> RuleSet:
    if isinstance(ruleset, RuleSet):
        return ruleset
    if isinstance(ruleset, dict):
        return RuleSet.from_dict(ruleset)
    return RuleSet.from_file(ruleset)


def _evaluate_observation(observation: Observation) -> RuleResult:
    rule = observation.rule
    condition = cast("Condition", rule.condition)
    dimension = cast("Dimension", rule.dimension)
    error_message = observation.error_message
    if error_message is None:
        try:
            passed = condition.evaluate(observation.observed_value)
        except (TypeError, ValueError) as e:
            passed = False
            error_message = f"Could not evaluate observed value {observation.observed_value!r}: {e}"
    else:
        passed = False

    if error_message is not None:
        status = ERROR
    elif passed:
        status = PASS
    else:
        status = WARN if rule.severity == "warn" else FAIL
    observed = observation.observed_value
    if observed is not None and not isinstance(observed, int | float | str):
        observed = str(observed)
    return RuleResult(
        rule_uid=rule.rule_uid,
        rule_name=rule.name,
        status=status,
        observed_value=observed,
        condition=condition.to_dict(),
        dimension=dimension.value,
        severity=rule.severity.value,
        duration_ms=observation.duration_ms,
        error_message=error_message,
        description=rule.description or describe_rule(rule),
        sql=observation.sql,
    )


def _build_run_from_context(
    *,
    context: Context,
    ruleset: RuleSet,
    table: str,
    outlets: Sequence[Asset],
    started_at: str,
    finished_at: str,
) -> DQRun:
    ti = context["ti"]
    return DQRun(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        map_index=ti.map_index if ti.map_index is not None else -1,
        ruleset_name=ruleset.name,
        table_ref=table,
        asset_names=tuple(_get_outlet_asset_names(outlets)),
        started_at=started_at,
        finished_at=finished_at,
    )


def _get_outlet_asset_names(outlets: Sequence[Asset]) -> list[str]:
    return [asset.name for asset in outlets if getattr(asset, "name", None)]


def _attach_to_outlet_events(context: Context, outlets: Sequence[Asset], summary: dict[str, Any]) -> None:
    try:
        outlet_events = context["outlet_events"]
    except KeyError:
        return
    for outlet in outlets:
        if getattr(outlet, "name", None):
            try:
                outlet_events[outlet].extra[DQ_RESULT_EXTRA_KEY] = summary
            except Exception:
                log.warning("Could not attach data quality summary to outlet event for %s", outlet)
