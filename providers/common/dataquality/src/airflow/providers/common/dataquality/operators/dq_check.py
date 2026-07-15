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
from typing import TYPE_CHECKING, Any

from airflow.providers.common.dataquality.assets import get_asset_quality_config
from airflow.providers.common.dataquality.exceptions import DQCheckFailedError
from airflow.providers.common.dataquality.execution import (
    DataQualityResult,
    _resolve_ruleset,
    persist_quality_results,
    run_quality_checks,
)
from airflow.providers.common.dataquality.results import (
    ERROR,
    FAIL,
    WARN,
    RuleResult,
)
from airflow.providers.common.sql.operators.sql import BaseSQLOperator

if TYPE_CHECKING:
    from airflow.providers.common.dataquality.rules import RuleSet
    from airflow.sdk import Asset, Context

log = logging.getLogger(__name__)

FAIL_ON_CHOICES = ("error", "warn", "never")


class DQCheckOperator(BaseSQLOperator):
    """
    Run a ruleset against a table and persist per-rule results to the configured results store.

    Every rule is evaluated and recorded regardless of the task outcome. Execution errors
    (a check query failing) always fail the task; rule failures fail the task according to
    ``fail_on``.

    :param ruleset: A :class:`~airflow.providers.common.dataquality.rules.RuleSet`, its dict form, or a path
        to a YAML ruleset file. Optional when ``asset`` carries one.
    :param table: The table to run checks against. Optional when ``asset`` carries one
        (falling back to the asset name).
    :param asset: An asset decorated with :func:`~airflow.providers.common.dataquality.assets.asset_quality`.
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

    Results are persisted to the backend configured under ``[common.dataquality] results_path``; when that's
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
            asset_conn_id = config.get("conn_id")
            if asset_conn_id is not None:
                kwargs.setdefault("conn_id", asset_conn_id)
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
        if not self.conn_id:
            raise ValueError("conn_id is required, either directly or via an asset_quality() asset")
        self.ruleset = ruleset
        self.table = table
        self.asset = asset
        self.partition_clause = partition_clause
        self.fail_on = fail_on

    def execute(self, context: Context) -> dict[str, Any]:
        result = run_quality_checks(
            hook=self.get_db_hook(),
            ruleset=_resolve_ruleset(self.ruleset),
            table=self.table,
            partition_clause=self.partition_clause,
        )
        results = list(result.results)
        summary = self._persist_result(result, context)
        self._log_results(results, summary)
        self._raise_for_failures(results, summary)
        return summary

    def _persist_result(self, result: DataQualityResult, context: Context) -> dict[str, Any]:
        return persist_quality_results(result, context=context, outlets=list(self.outlets or ()))

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
