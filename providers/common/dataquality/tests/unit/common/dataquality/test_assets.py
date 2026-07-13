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

import pytest

from airflow.providers.common.dataquality.assets import (
    DQ_EXTRA_KEY,
    DQ_RESULT_EXTRA_KEY,
    _quality_score_passes,
    asset_quality,
    get_asset_quality_config,
    get_asset_ruleset,
    require_quality,
)
from airflow.providers.common.dataquality.exceptions import DQRuleValidationError
from airflow.providers.common.dataquality.rules import DQRule, RuleSet
from airflow.sdk import Asset
from airflow.sdk.execution_time.comms import AssetEventDagRunReferenceResult
from airflow.sdk.execution_time.context import TriggeringAssetEventsAccessor

RULESET = RuleSet(
    name="orders",
    rules=(DQRule(name="volume", check="row_count", condition={"greater_than": 0}),),
)

ORDERS = Asset("orders")


def make_event(
    *,
    extra: dict,
    asset: Asset = ORDERS,
    run_id: str = "manual__2026-07-04",
    timestamp: str = "2026-07-04T06:00:00Z",
) -> AssetEventDagRunReferenceResult:
    event = {
        "asset": {"name": asset.name, "uri": asset.uri, "extra": {}},
        "extra": extra,
        "source_task_id": "dq",
        "source_dag_id": "orders_pipeline",
        "source_run_id": run_id,
        "source_map_index": -1,
        "source_aliases": [],
        "timestamp": timestamp,
    }
    return AssetEventDagRunReferenceResult.model_validate(event)


def make_triggering_events(*, extra: dict, asset: Asset = ORDERS) -> TriggeringAssetEventsAccessor:
    return TriggeringAssetEventsAccessor.build([make_event(extra=extra, asset=asset)])


class TestAssetQuality:
    def test_config_stored_under_dq_extra_key(self):
        asset = asset_quality(
            Asset("orders", uri="postgres://wh/warehouse/analytics/orders"),
            ruleset=RULESET,
            conn_id="warehouse",
            table="analytics.orders",
        )
        config = asset.extra[DQ_EXTRA_KEY]
        assert config["conn_id"] == "warehouse"
        assert config["table"] == "analytics.orders"
        assert config["ruleset"] == RULESET.to_dict()

    def test_config_is_json_serializable(self):
        import json

        asset = asset_quality(Asset("orders"), ruleset=RULESET)
        json.dumps(asset.extra)

    def test_ruleset_round_trips_through_asset(self):
        asset = asset_quality(Asset("orders"), ruleset=RULESET)
        assert get_asset_ruleset(asset) == RULESET

    def test_ruleset_from_yaml_file(self, tmp_path):
        ruleset_file = tmp_path / "orders.yaml"
        ruleset_file.write_text(
            """
            name: orders
            rules:
              - name: volume
                check: row_count
                condition:
                  greater_than: 0
            """
        )
        asset = asset_quality(Asset("orders"), ruleset=str(ruleset_file))
        assert get_asset_ruleset(asset).rules[0].name == "volume"

    def test_get_config_returns_none_without_quality(self):
        assert get_asset_quality_config(Asset("plain")) is None

    def test_get_ruleset_raises_without_quality(self):
        with pytest.raises(DQRuleValidationError, match="no data quality config"):
            get_asset_ruleset(Asset("plain"))


class TestQualityScorePasses:
    """
    Covers the pure decision logic behind ``require_quality()``.

    Kept independent of ``@task.short_circuit`` / Dag wiring, which is exercised by the
    standard provider's own tests — this only needs to prove "given this triggering event,
    does the gate pass or fail".
    """

    def test_passes_when_score_at_min_score(self):
        events = make_triggering_events(extra={DQ_RESULT_EXTRA_KEY: {"score": 0.95}})
        assert _quality_score_passes(ORDERS, 0.95, events) is True

    def test_fails_when_score_below_min_score(self):
        events = make_triggering_events(extra={DQ_RESULT_EXTRA_KEY: {"score": 0.8}})
        assert _quality_score_passes(ORDERS, 0.95, events) is False

    def test_fails_when_no_triggering_event_for_asset(self):
        events = make_triggering_events(extra={DQ_RESULT_EXTRA_KEY: {"score": 1.0}}, asset=Asset("other"))
        assert _quality_score_passes(ORDERS, 0.5, events) is False

    def test_fails_when_event_has_no_dq_summary(self):
        events = make_triggering_events(extra={})
        assert _quality_score_passes(ORDERS, 0.5, events) is False

    def test_fails_when_summary_has_no_score(self):
        events = make_triggering_events(extra={DQ_RESULT_EXTRA_KEY: {"passed": 3}})
        assert _quality_score_passes(ORDERS, 0.5, events) is False

    def test_require_all_fails_when_any_event_is_below_min_score(self):
        low = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.4}}, run_id="run1", timestamp="2026-07-03T06:00:00Z"
        )
        high = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.99}}, run_id="run2", timestamp="2026-07-04T06:00:00Z"
        )
        events = TriggeringAssetEventsAccessor.build([low, high])

        assert _quality_score_passes(ORDERS, 0.9, events) is False

    def test_require_all_passes_when_every_event_meets_min_score(self):
        first = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.95}}, run_id="run1", timestamp="2026-07-03T06:00:00Z"
        )
        second = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.99}}, run_id="run2", timestamp="2026-07-04T06:00:00Z"
        )
        events = TriggeringAssetEventsAccessor.build([first, second])

        assert _quality_score_passes(ORDERS, 0.9, events) is True

    def test_latest_only_uses_most_recent_event_when_require_all_false(self):
        first = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.4}}, run_id="run1", timestamp="2026-07-03T06:00:00Z"
        )
        second = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.99}}, run_id="run2", timestamp="2026-07-04T06:00:00Z"
        )
        events = TriggeringAssetEventsAccessor.build([first, second])

        assert _quality_score_passes(ORDERS, 0.9, events, require_all=False) is True

    def test_latest_only_fails_when_most_recent_event_is_below_min_score(self):
        first = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.99}}, run_id="run1", timestamp="2026-07-03T06:00:00Z"
        )
        second = make_event(
            extra={DQ_RESULT_EXTRA_KEY: {"score": 0.4}}, run_id="run2", timestamp="2026-07-04T06:00:00Z"
        )
        events = TriggeringAssetEventsAccessor.build([first, second])

        assert _quality_score_passes(ORDERS, 0.9, events, require_all=False) is False


class TestRequireQuality:
    def test_rejects_min_score_below_zero(self):
        with pytest.raises(ValueError, match="min_score must be between 0 and 1"):
            require_quality(ORDERS, min_score=-0.1)

    def test_rejects_min_score_above_one(self):
        with pytest.raises(ValueError, match="min_score must be between 0 and 1"):
            require_quality(ORDERS, min_score=1.1)
