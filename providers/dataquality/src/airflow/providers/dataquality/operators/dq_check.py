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
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.providers.dataquality.assets import DQ_RESULT_EXTRA_KEY, get_asset_quality_config
from airflow.providers.dataquality.backends import get_backend_from_config
from airflow.providers.dataquality.engines.sql import SQLDQEngine
from airflow.providers.dataquality.exceptions import DQCheckFailedError
from airflow.providers.dataquality.results import ERROR, FAIL, PASS, WARN, DQRun, RuleResult, build_summary
from airflow.providers.dataquality.rules import RuleSet, describe_rule
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.providers.dataquality.engines.sql import Observation
    from airflow.providers.dataquality.rules.rule import Condition, Dimension
    from airflow.sdk import Asset, Context

log = logging.getLogger(__name__)

FAIL_ON_CHOICES = ("error", "warn", "never")


class DQCheckOperator(BaseSQLOperator):
    """
    Run a ruleset against a table and persist per-rule results to the configured results store.

    Every rule is evaluated and recorded regardless of the task outcome. Execution errors
    (a check query failing) always fail the task; rule failures fail the task according to
    ``fail_on``.

    :param ruleset: A :class:`~airflow.providers.dataquality.rules.RuleSet`, its dict form, or a path
        to a YAML ruleset file. Optional when ``asset`` carries one.
    :param table: The table to run checks against. Optional when ``asset`` carries one
        (falling back to the asset name).
    :param asset: An asset decorated with :func:`~airflow.providers.dataquality.assets.asset_quality`.
        Supplies defaults for ``ruleset``, ``table``, and ``conn_id`` (explicit arguments
        win) and is automatically added to the task's outlets so its asset events carry
        the check summary.
    :param partition_clause: Predicate ANDed into every built-in check's WHERE clause,
        e.g. ``"ds = '{{ ds }}'"``.
    :param fail_on: Severity that fails the task: ``error`` (default — warn-severity rule
        failures are recorded but don't fail), ``warn`` (any rule failure fails), or
        ``never`` (only execution errors fail).
    :param conn_id: Connection to the database to check (any ``DbApiHook``-compatible type).
    :param database: Optional database/schema overriding the connection's default.

    Results are persisted to the backend configured under ``[dataquality] results_path``; when that's
    unset, checks still run but no history is persisted. There is no per-operator override --
    all checks in a deployment share one results store.
    """

    template_fields: Sequence[str] = ("table", "partition_clause", *BaseSQLOperator.template_fields)
    ui_color: str = "#87ceeb"

    def __init__(
        self,
        *,
        ruleset: RuleSet | dict[str, Any] | str | None = None,
        table: str | None = None,
        asset: Asset | None = None,
        partition_clause: str | None = None,
        fail_on: str = "error",
        **kwargs,
    ) -> None:
        if asset is not None:
            config = get_asset_quality_config(asset) or {}
            ruleset = ruleset if ruleset is not None else config.get("ruleset")
            table = table or config.get("table") or asset.name
            kwargs.setdefault("conn_id", config.get("conn_id"))
            outlets = list(kwargs.get("outlets") or [])
            if asset not in outlets:
                outlets.append(asset)
            kwargs["outlets"] = outlets
        super().__init__(**kwargs)
        if fail_on not in FAIL_ON_CHOICES:
            raise ValueError(f"fail_on must be one of {FAIL_ON_CHOICES}, got {fail_on!r}")
        if ruleset is None:
            raise ValueError("ruleset is required, either directly or via an asset_quality() asset")
        if not table:
            raise ValueError("table is required, either directly or via an asset_quality() asset")
        self.ruleset = ruleset
        self.table = table
        self.asset = asset
        self.partition_clause = partition_clause
        self.fail_on = fail_on

    def execute(self, context: Context) -> dict[str, Any]:
        ruleset = self._resolve_ruleset()
        started_at = datetime.now(tz=timezone.utc).isoformat()
        engine = self._get_engine(self.get_db_hook())
        observations = engine.measure(ruleset, self.table, self.partition_clause)
        results = [self._evaluate(observation) for observation in observations]
        finished_at = datetime.now(tz=timezone.utc).isoformat()

        run = self._build_run(context, ruleset, started_at, finished_at)
        summary = build_summary(run, results)
        self._log_results(results, summary)
        self._persist(run, results)
        self._attach_to_outlet_events(context, summary)
        self._raise_for_failures(results, summary)
        return summary

    def _get_engine(self, hook: DbApiHook) -> SQLDQEngine:
        return SQLDQEngine(hook)

    def _resolve_ruleset(self) -> RuleSet:
        if self.ruleset is SET_DURING_EXECUTION:
            raise ValueError("ruleset is required, either directly or returned by @task.dq_check")
        if isinstance(self.ruleset, RuleSet):
            return self.ruleset
        if isinstance(self.ruleset, dict):
            return RuleSet.from_dict(self.ruleset)
        return RuleSet.from_file(self.ruleset)

    @staticmethod
    def _evaluate(observation: Observation) -> RuleResult:
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

    def _build_run(self, context: Context, ruleset: RuleSet, started_at: str, finished_at: str) -> DQRun:
        ti = context["ti"]
        return DQRun(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            try_number=ti.try_number,
            map_index=ti.map_index if ti.map_index is not None else -1,
            ruleset_name=ruleset.name,
            table_ref=self.table,
            asset_names=tuple(self._outlet_asset_names()),
            started_at=started_at,
            finished_at=finished_at,
        )

    def _outlet_asset_names(self) -> list[str]:
        names = []
        for outlet in self.outlets or []:
            name = getattr(outlet, "name", None)
            if name:
                names.append(name)
        return names

    def _persist(self, run: DQRun, results: list[RuleResult]) -> None:
        backend = get_backend_from_config()
        if backend is None:
            self.log.info("No [dataquality] results_path configured; skipping results persistence")
            return
        # Persistence is best-effort: an unreachable results store leaves a gap in
        # history but must not change the outcome of the check itself.
        try:
            backend.write_run(run, results)
        except Exception:
            self.log.exception("Failed to persist data quality results; continuing")

    def _attach_to_outlet_events(self, context: Context, summary: dict[str, Any]) -> None:
        try:
            outlet_events = context["outlet_events"]
        except KeyError:
            return
        for outlet in self.outlets or []:
            if getattr(outlet, "name", None):
                try:
                    outlet_events[outlet].extra[DQ_RESULT_EXTRA_KEY] = summary
                except Exception:
                    self.log.warning("Could not attach dq summary to outlet event for %s", outlet)

    def _log_results(self, results: list[RuleResult], summary: dict[str, Any]) -> None:
        for result in results:
            self.log.info(
                "Rule %s [%s/%s]: %s (observed=%s, condition=%s)",
                result.rule_name,
                result.dimension,
                result.severity,
                result.status.upper(),
                result.observed_value,
                result.condition,
            )
        self.log.info("Data quality score: %s", summary["score"])

    def _raise_for_failures(self, results: list[RuleResult], summary: dict[str, Any]) -> None:
        errored = [r.rule_name for r in results if r.status == ERROR]
        if errored:
            raise DQCheckFailedError(f"Check execution errored for rules: {errored}")
        failed = [r.rule_name for r in results if r.status == FAIL]
        warned = [r.rule_name for r in results if r.status == WARN]
        if self.fail_on == "error" and failed:
            raise DQCheckFailedError(f"Data quality rules failed: {failed} (score={summary['score']})")
        if self.fail_on == "warn" and (failed or warned):
            raise DQCheckFailedError(
                f"Data quality rules failed: {failed + warned} (score={summary['score']})"
            )
        if failed or warned:
            self.log.warning("Rule failures below fail_on=%s threshold: %s", self.fail_on, failed + warned)
