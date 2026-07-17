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

from unittest import mock

import pytest

from airflow.providers.common.dataquality.backends.object_storage import ObjectStorageResultsBackend
from airflow.providers.common.dataquality.engines.sql import Observation
from airflow.providers.common.dataquality.execution import (
    _attach_to_outlet_events,
    _build_run_from_context,
    _evaluate_observation,
    _get_hook,
    _get_outlet_asset_names,
    _resolve_ruleset,
    persist_quality_results,
    run_quality_checks,
)
from airflow.providers.common.dataquality.rules import Condition, DQRule, RuleSet
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sdk import Asset
from airflow.sdk.execution_time.context import OutletEventAccessors

ORDER_ID_NOT_NULL = DQRule(
    name="order_id_not_null",
    check="null_count",
    column="order_id",
    condition=Condition(equal_to=0),
)
ORDER_AMOUNT_VALID = DQRule(
    name="order_amount_valid",
    check="min",
    column="amount",
    condition=Condition(geq_to=0),
)
ORDER_RULESET = RuleSet(name="orders_quality", rules=(ORDER_ID_NOT_NULL, ORDER_AMOUNT_VALID))


def make_context():
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "try_number", "map_index"])
    ti.dag_id = "orders_pipeline"
    ti.task_id = "custom_quality_task"
    ti.run_id = "manual__2026-07-04"
    ti.try_number = 1
    ti.map_index = -1
    return {"ti": ti, "outlet_events": mock.create_autospec(OutletEventAccessors, instance=True)}


class TestRunQualityChecks:
    def test_runs_rules_with_supplied_hook(self):
        hook = mock.create_autospec(DbApiHook, instance=True)
        hook.get_records.return_value = [
            (ORDER_ID_NOT_NULL.rule_uid, 0),
            (ORDER_AMOUNT_VALID.rule_uid, 5),
        ]

        result = run_quality_checks(hook=hook, table="orders", ruleset=ORDER_RULESET)

        assert result.ruleset == ORDER_RULESET
        assert result.table == "orders"
        assert [rule_result.status for rule_result in result.results] == ["pass", "pass"]
        hook.get_records.assert_called_once()

    @mock.patch("airflow.providers.common.dataquality.execution.BaseHook.get_connection", autospec=True)
    def test_runs_rules_with_conn_id(self, mock_get_connection):
        hook = mock.create_autospec(DbApiHook, instance=True)
        hook.get_records.return_value = [(ORDER_ID_NOT_NULL.rule_uid, 1)]
        connection = mock.Mock(spec=["get_hook"])
        connection.get_hook.return_value = hook
        mock_get_connection.return_value = connection

        result = run_quality_checks(
            conn_id="warehouse",
            hook_params={"schema": "analytics"},
            table="orders",
            ruleset=RuleSet(name="orders_quality", rules=(ORDER_ID_NOT_NULL,)),
        )

        assert result.results[0].status == "fail"
        mock_get_connection.assert_called_once_with("warehouse")
        connection.get_hook.assert_called_once_with(hook_params={"schema": "analytics"})

    def test_returns_error_result_when_value_cannot_be_evaluated(self):
        hook = mock.create_autospec(DbApiHook, instance=True)
        hook.get_records.return_value = [(ORDER_ID_NOT_NULL.rule_uid, "not-a-number")]

        result = run_quality_checks(
            hook=hook,
            table="orders",
            ruleset=RuleSet(name="orders_quality", rules=(ORDER_ID_NOT_NULL,)),
        )

        assert result.results[0].status == "error"
        assert "Could not evaluate observed value" in result.results[0].error_message


class TestPersistQualityResults:
    def test_persists_results_for_custom_task(self, tmp_path):
        hook = mock.create_autospec(DbApiHook, instance=True)
        hook.get_records.return_value = [
            (ORDER_ID_NOT_NULL.rule_uid, 0),
            (ORDER_AMOUNT_VALID.rule_uid, 5),
        ]
        result = run_quality_checks(hook=hook, table="orders", ruleset=ORDER_RULESET)
        backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")

        with mock.patch(
            "airflow.providers.common.dataquality.execution.get_backend_from_config",
            return_value=backend,
        ):
            summary = persist_quality_results(result, context=make_context())

        assert summary["passed"] == 2
        history = backend.read_task_rule_history(
            dag_id="orders_pipeline",
            task_id="custom_quality_task",
            rule_uid=ORDER_ID_NOT_NULL.rule_uid,
        )["items"]
        assert len(history) == 1
        assert history[0]["status"] == "pass"

    def test_attaches_summary_to_outlet_asset_event(self):
        hook = mock.create_autospec(DbApiHook, instance=True)
        hook.get_records.return_value = [(ORDER_ID_NOT_NULL.rule_uid, 0)]
        result = run_quality_checks(
            hook=hook,
            table="orders",
            ruleset=RuleSet(name="orders_quality", rules=(ORDER_ID_NOT_NULL,)),
        )
        asset = Asset("orders")
        context = make_context()

        with mock.patch(
            "airflow.providers.common.dataquality.execution.get_backend_from_config", return_value=None
        ):
            summary = persist_quality_results(result, context=context, outlets=[asset])

        outlet_events = context["outlet_events"]
        outlet_events.__getitem__.assert_called_with(asset)
        outlet_events.__getitem__.return_value.extra.__setitem__.assert_called_once_with(
            "airflow.dataquality.result", summary
        )

    def test_backend_failure_does_not_fail_custom_task(self):
        hook = mock.create_autospec(DbApiHook, instance=True)
        hook.get_records.return_value = [(ORDER_ID_NOT_NULL.rule_uid, 0)]
        result = run_quality_checks(
            hook=hook,
            table="orders",
            ruleset=RuleSet(name="orders_quality", rules=(ORDER_ID_NOT_NULL,)),
        )
        backend = mock.create_autospec(ObjectStorageResultsBackend, instance=True)
        backend.write_run.side_effect = OSError("bucket unreachable")

        with mock.patch(
            "airflow.providers.common.dataquality.execution.get_backend_from_config",
            return_value=backend,
        ):
            summary = persist_quality_results(result, context=make_context())

        assert summary["passed"] == 1
        backend.write_run.assert_called_once()


class TestPrivateHelpers:
    @pytest.mark.parametrize(
        "ruleset_arg",
        [
            ORDER_RULESET,
            ORDER_RULESET.to_dict(),
        ],
    )
    def test_resolve_ruleset_from_model_or_dict(self, ruleset_arg):
        assert _resolve_ruleset(ruleset_arg) == ORDER_RULESET

    def test_resolve_ruleset_from_file(self, tmp_path):
        ruleset_file = tmp_path / "orders_ruleset.yaml"
        ruleset_file.write_text(
            """
name: orders_quality
rules:
  - name: order_id_not_null
    check: null_count
    column: order_id
    condition:
      equal_to: 0
""",
        )

        ruleset = _resolve_ruleset(str(ruleset_file))

        assert ruleset.name == "orders_quality"
        assert ruleset.rules[0].name == "order_id_not_null"

    def test_get_hook_requires_conn_id_when_hook_is_not_supplied(self):
        with pytest.raises(ValueError, match="Either conn_id or hook is required"):
            _get_hook(None, None)

    @mock.patch("airflow.providers.common.dataquality.execution.BaseHook.get_connection", autospec=True)
    def test_get_hook_uses_conn_id_and_hook_params(self, mock_get_connection):
        hook = mock.create_autospec(DbApiHook, instance=True)
        connection = mock.Mock(spec=["get_hook"])
        connection.get_hook.return_value = hook
        mock_get_connection.return_value = connection

        assert _get_hook("warehouse", {"schema": "analytics"}) == hook
        mock_get_connection.assert_called_once_with("warehouse")
        connection.get_hook.assert_called_once_with(hook_params={"schema": "analytics"})

    def test_evaluate_observation_records_warn_for_warn_severity_failure(self):
        warn_rule = DQRule(
            name="volume",
            check="row_count",
            condition=Condition(greater_than=100),
            severity="warn",
        )

        result = _evaluate_observation(Observation(rule=warn_rule, observed_value=5, duration_ms=12.3))

        assert result.status == "warn"
        assert result.observed_value == 5
        assert result.duration_ms == 12.3

    def test_evaluate_observation_records_error_for_query_error(self):
        result = _evaluate_observation(Observation(rule=ORDER_ID_NOT_NULL, error_message="query failed"))

        assert result.status == "error"
        assert result.error_message == "query failed"

    def test_build_run_from_context_uses_task_metadata_and_outlets(self):
        context = make_context()
        context["ti"].map_index = None
        asset = Asset("orders")

        run = _build_run_from_context(
            context=context,
            ruleset=ORDER_RULESET,
            table="orders",
            outlets=[asset],
            started_at="2026-07-04T12:00:00+00:00",
            finished_at="2026-07-04T12:00:01+00:00",
        )

        assert run.dag_id == "orders_pipeline"
        assert run.task_id == "custom_quality_task"
        assert run.map_index == -1
        assert run.ruleset_name == "orders_quality"
        assert run.table_ref == "orders"
        assert run.asset_names == ("orders",)
        assert run.started_at == "2026-07-04T12:00:00+00:00"
        assert run.finished_at == "2026-07-04T12:00:01+00:00"

    def test_get_outlet_asset_names_ignores_outlets_without_names(self):
        named_asset = Asset("orders")
        unnamed_outlet = mock.Mock(spec=[])

        assert _get_outlet_asset_names([named_asset, unnamed_outlet]) == ["orders"]

    def test_attach_to_outlet_events_noops_without_context_key(self):
        _attach_to_outlet_events({}, [Asset("orders")], {"score": 1.0})

    def test_attach_to_outlet_events_ignores_single_outlet_failure(self):
        asset = Asset("orders")
        outlet_events = mock.create_autospec(OutletEventAccessors, instance=True)
        outlet_events.__getitem__.side_effect = RuntimeError("not available")

        _attach_to_outlet_events({"outlet_events": outlet_events}, [asset], {"score": 1.0})

        outlet_events.__getitem__.assert_called_once_with(asset)
