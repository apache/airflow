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
"""
Asset-level data quality declarations.

Quality configuration lives inside ``Asset.extra`` under the ``airflow.dq`` key, so it is
serialized with the Dag and needs no Airflow core changes. The rules travel with the asset
definition instead of being scattered across the Dags that check it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airflow.providers.dataquality.exceptions import DQRuleValidationError
from airflow.providers.dataquality.rules import RuleSet
from airflow.sdk import task

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from airflow.sdk import Asset
    from airflow.sdk.execution_time.comms import AssetEventDagRunReferenceResult

log = logging.getLogger(__name__)

DQ_EXTRA_KEY = "airflow.dq"
DQ_RESULT_EXTRA_KEY = "airflow.dq.result"


def asset_quality(
    asset: Asset,
    *,
    ruleset: RuleSet | dict[str, Any] | str,
    conn_id: str | None = None,
    table: str | None = None,
) -> Asset:
    """
    Attach data quality configuration to an asset, returning the same asset.

    :param asset: The asset the rules describe.
    :param ruleset: A :class:`~airflow.providers.dataquality.rules.RuleSet`, its dict form, or a path
        to a YAML ruleset file (resolved eagerly, at Dag-parse time).
    :param conn_id: Default connection a check operator should use for this asset.
    :param table: Default table to check; falls back to the asset name when unset.

    Usage::

        orders = asset_quality(
            Asset("orders", uri="postgres://warehouse/analytics/orders"),
            ruleset=rules,
            conn_id="warehouse",
            table="analytics.orders",
        )
        check = DQCheckOperator(task_id="dq", asset=orders)
    """
    if isinstance(ruleset, str):
        ruleset = RuleSet.from_file(ruleset)
    elif isinstance(ruleset, dict):
        ruleset = RuleSet.from_dict(ruleset)
    config: dict[str, Any] = {"ruleset": ruleset.to_dict()}
    if conn_id:
        config["conn_id"] = conn_id
    if table:
        config["table"] = table
    asset.extra[DQ_EXTRA_KEY] = config
    return asset


def get_asset_quality_config(asset: Asset) -> dict[str, Any] | None:
    """Return the raw ``airflow.dq`` config attached to an asset, if any."""
    config = asset.extra.get(DQ_EXTRA_KEY)
    if not isinstance(config, dict):
        return None
    return config


def get_asset_ruleset(asset: Asset) -> RuleSet:
    """Return the ruleset attached to an asset, raising when none is attached."""
    config = get_asset_quality_config(asset)
    if not config or "ruleset" not in config:
        raise DQRuleValidationError(
            f"Asset {asset.name!r} has no data quality config; attach one with asset_quality()"
        )
    return RuleSet.from_dict(config["ruleset"])


def _quality_score_passes(
    asset: Asset,
    min_score: float,
    triggering_asset_events: Mapping[Asset, Sequence[AssetEventDagRunReferenceResult]],
) -> bool:
    """Pure decision logic behind :func:`require_quality`, kept separate so it is testable without a Dag."""
    events = triggering_asset_events.get(asset, [])
    if not events:
        log.warning(
            "require_quality(%s): no triggering event for this run; skipping downstream tasks",
            asset.name,
        )
        return False

    summary = events[-1].extra.get(DQ_RESULT_EXTRA_KEY)
    score = summary.get("score") if isinstance(summary, dict) else None
    if not isinstance(score, (int, float)) or isinstance(score, bool):
        log.warning(
            "require_quality(%s): triggering event has no data quality summary; skipping downstream tasks",
            asset.name,
        )
        return False

    if score < min_score:
        log.warning(
            "require_quality(%s): score %s below required minimum %s; skipping downstream tasks",
            asset.name,
            score,
            min_score,
        )
        return False

    return True


def require_quality(
    asset: Asset,
    *,
    min_score: float,
    task_id: str | None = None,
) -> Any:
    """
    Gate a Dag run on the data quality score attached to one of its triggering asset events.

    Reads the summary a :class:`~airflow.providers.dataquality.operators.dq_check.DQCheckOperator`
    attaches to ``asset``'s outlet event, under ``asset_event.extra["airflow.dq.result"]``
    (see :func:`asset_quality`), and short-circuits the run — skipping every downstream task
    — when that summary is missing or its score is below ``min_score``. Call it inside a Dag
    scheduled by ``asset``::

        with DAG("orders_consumer", schedule=orders) as consumer:
            start = require_quality(orders, min_score=0.95)
            start >> process_orders()

    :param asset: The asset whose triggering event carries the quality summary.
    :param min_score: Minimum required score in ``[0, 1]``; the run proceeds only when the
        triggering event's score is at least this value.
    :param task_id: Task id for the generated gate task. Defaults to
        ``f"require_quality_{asset.name}"`` so gating on several assets in one Dag doesn't
        collide on task id.
    """
    if not 0 <= min_score <= 1:
        raise ValueError(f"min_score must be between 0 and 1, got {min_score!r}")
    gate_task_id = task_id or f"require_quality_{asset.name}"

    @task.short_circuit(task_id=gate_task_id)
    def _require_quality(**context: Any) -> bool:
        return _quality_score_passes(asset, min_score, context["triggering_asset_events"])

    return _require_quality()
