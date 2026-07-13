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

from airflow.providers.common.dataquality.assets import asset_quality
from airflow.providers.common.dataquality.backends.object_storage import ObjectStorageResultsBackend
from airflow.providers.common.dataquality.exceptions import DQCheckFailedError
from airflow.providers.common.dataquality.operators.dq_check import DQCheckOperator
from airflow.providers.common.dataquality.rules import DQRule, RuleSet, Severity
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sdk import Asset
from airflow.sdk.execution_time.context import OutletEventAccessors

NULLS = DQRule(name="nulls", check="null_count", column="id", condition={"equal_to": 0})
VOLUME_WARN = DQRule(
    name="volume", check="row_count", condition={"greater_than": 100}, severity=Severity.WARN
)
RULESET = RuleSet(name="orders", rules=(NULLS, VOLUME_WARN))


def make_context():
    # spec'd to the attributes DQCheckOperator actually reads off `ti`, since the real
    # RuntimeTaskInstance is a heavyweight pydantic model with many unrelated required fields.
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "try_number", "map_index"])
    ti.dag_id = "orders_pipeline"
    ti.task_id = "dq"
    ti.run_id = "manual__2026-07-04"
    ti.try_number = 1
    ti.map_index = -1
    return {"ti": ti, "outlet_events": mock.create_autospec(OutletEventAccessors, instance=True)}


def make_operator(records, **kwargs):
    kwargs.setdefault("task_id", "dq")
    kwargs.setdefault("ruleset", RULESET)
    kwargs.setdefault("table", "orders")
    operator = DQCheckOperator(conn_id="warehouse", **kwargs)
    hook = mock.create_autospec(DbApiHook, instance=True)
    hook.get_records.return_value = records
    return operator, hook


@pytest.fixture(autouse=True)
def results_backend(tmp_path):
    """Patch the module-level config lookup so persisted results land in an isolated tmp_path."""
    backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
    with mock.patch(
        "airflow.providers.common.dataquality.operators.dq_check.get_backend_from_config",
        return_value=backend,
    ):
        yield backend


class TestDQCheckOperatorConstruction:
    def test_requires_ruleset(self):
        with pytest.raises(ValueError, match="ruleset is required"):
            DQCheckOperator(task_id="dq", table="orders", conn_id="c")

    def test_requires_table(self):
        with pytest.raises(ValueError, match="table is required"):
            DQCheckOperator(task_id="dq", ruleset=RULESET, conn_id="c")

    def test_rejects_bad_fail_on(self):
        with pytest.raises(ValueError, match="fail_on"):
            DQCheckOperator(task_id="dq", ruleset=RULESET, table="orders", conn_id="c", fail_on="maybe")

    def test_requires_conn_id(self):
        with pytest.raises(ValueError, match="conn_id is required"):
            DQCheckOperator(task_id="dq", ruleset=RULESET, table="orders")

    def test_asset_without_conn_id_fails_fast(self):
        asset = asset_quality(
            Asset("orders", uri="postgres://wh/warehouse/analytics/orders"),
            ruleset=RULESET,
            table="analytics.orders",
        )

        with pytest.raises(ValueError, match="conn_id is required"):
            DQCheckOperator(task_id="dq", asset=asset)

    def test_asset_supplies_defaults_and_outlet(self):
        asset = asset_quality(
            Asset("orders", uri="postgres://wh/warehouse/analytics/orders"),
            ruleset=RULESET,
            conn_id="warehouse",
            table="analytics.orders",
        )
        operator = DQCheckOperator(task_id="dq", asset=asset)
        assert operator.table == "analytics.orders"
        assert operator.conn_id == "warehouse"
        assert asset in operator.outlets

    def test_explicit_arguments_beat_asset_config(self):
        asset = asset_quality(
            Asset("orders", uri="postgres://wh/warehouse/analytics/orders"),
            ruleset=RULESET,
            conn_id="warehouse",
            table="analytics.orders",
        )
        operator = DQCheckOperator(task_id="dq", asset=asset, table="other_table", conn_id="other_conn")
        assert operator.table == "other_table"
        assert operator.conn_id == "other_conn"


class TestDQCheckOperatorExecute:
    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_all_pass_returns_summary_and_persists(self, mock_get_db_hook, results_backend):
        operator, hook = make_operator(records=[(NULLS.rule_uid, 0), (VOLUME_WARN.rule_uid, 500)])
        mock_get_db_hook.return_value = hook

        summary = operator.execute(make_context())

        assert summary["passed"] == 2
        assert summary["failed"] == 0
        assert summary["score"] == 1.0
        history = results_backend.read_task_rule_history("orders_pipeline", "dq", NULLS.rule_uid)["items"]
        assert len(history) == 1
        assert history[0]["status"] == "pass"

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_error_severity_failure_fails_task(self, mock_get_db_hook, results_backend):
        operator, hook = make_operator(records=[(NULLS.rule_uid, 3), (VOLUME_WARN.rule_uid, 500)])
        mock_get_db_hook.return_value = hook

        with pytest.raises(DQCheckFailedError, match="nulls"):
            operator.execute(make_context())

        # The failing result is persisted even though the task fails.
        assert (
            results_backend.read_task_rule_history("orders_pipeline", "dq", NULLS.rule_uid)["items"][0][
                "status"
            ]
            == "fail"
        )

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_warn_severity_failure_does_not_fail_task_by_default(self, mock_get_db_hook):
        operator, hook = make_operator(records=[(NULLS.rule_uid, 0), (VOLUME_WARN.rule_uid, 5)])
        mock_get_db_hook.return_value = hook

        summary = operator.execute(make_context())

        assert summary["warned"] == 1
        assert summary["warned_rules"] == ["volume"]
        assert summary["score"] == pytest.approx(0.875)

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_rule_description_is_persisted(self, mock_get_db_hook, results_backend):
        described_rule = DQRule(
            name="nulls",
            check="null_count",
            column="id",
            condition={"equal_to": 0},
            description="Order IDs must always be present.",
        )
        operator, hook = make_operator(
            records=[(described_rule.rule_uid, 0)],
            ruleset=RuleSet(name="orders", rules=(described_rule,)),
        )
        mock_get_db_hook.return_value = hook

        operator.execute(make_context())

        history = results_backend.read_task_rule_history("orders_pipeline", "dq", described_rule.rule_uid)[
            "items"
        ]
        assert history[0]["description"] == "Order IDs must always be present."

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_fail_on_warn_fails_task_on_warn_failure(self, mock_get_db_hook):
        operator, hook = make_operator(
            records=[(NULLS.rule_uid, 0), (VOLUME_WARN.rule_uid, 5)], fail_on="warn"
        )
        mock_get_db_hook.return_value = hook

        with pytest.raises(DQCheckFailedError, match="volume"):
            operator.execute(make_context())

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_fail_on_never_records_failures_without_raising(self, mock_get_db_hook):
        operator, hook = make_operator(
            records=[(NULLS.rule_uid, 3), (VOLUME_WARN.rule_uid, 5)], fail_on="never"
        )
        mock_get_db_hook.return_value = hook

        summary = operator.execute(make_context())

        assert summary["failed"] == 1
        assert summary["warned"] == 1

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_execution_error_always_fails_task(self, mock_get_db_hook):
        """A connection that's actually down fails the batch query and every per-rule fallback query."""
        operator, hook = make_operator(records=None, fail_on="never")
        hook.get_records.side_effect = RuntimeError("boom")
        hook.get_first.side_effect = RuntimeError("boom")
        mock_get_db_hook.return_value = hook

        with pytest.raises(DQCheckFailedError, match="errored"):
            operator.execute(make_context())

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_non_numeric_observed_value_is_persisted_as_error(self, mock_get_db_hook, results_backend):
        operator, hook = make_operator(records=[(NULLS.rule_uid, "not-a-number")], fail_on="never")
        mock_get_db_hook.return_value = hook

        with pytest.raises(DQCheckFailedError, match="errored"):
            operator.execute(make_context())

        history = results_backend.read_task_rule_history("orders_pipeline", "dq", NULLS.rule_uid)["items"]
        assert history[0]["status"] == "error"
        assert "Could not evaluate observed value" in history[0]["error_message"]

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_backend_failure_does_not_fail_the_check(self, mock_get_db_hook):
        backend = mock.create_autospec(ObjectStorageResultsBackend, instance=True)
        backend.write_run.side_effect = OSError("bucket unreachable")
        operator, hook = make_operator(records=[(NULLS.rule_uid, 0), (VOLUME_WARN.rule_uid, 500)])
        mock_get_db_hook.return_value = hook

        with mock.patch(
            "airflow.providers.common.dataquality.operators.dq_check.get_backend_from_config",
            return_value=backend,
        ):
            summary = operator.execute(make_context())

        assert summary["passed"] == 2
        backend.write_run.assert_called_once()

    @mock.patch.object(DQCheckOperator, "get_db_hook")
    def test_summary_attached_to_outlet_events(self, mock_get_db_hook):
        asset = asset_quality(
            Asset("orders", uri="postgres://wh/warehouse/analytics/orders"),
            ruleset=RULESET,
            conn_id="warehouse",
            table="orders",
        )
        operator, hook = make_operator(
            records=[(NULLS.rule_uid, 0), (VOLUME_WARN.rule_uid, 500)], asset=asset
        )
        mock_get_db_hook.return_value = hook
        context = make_context()

        summary = operator.execute(context)

        outlet_events = context["outlet_events"]
        outlet_events.__getitem__.assert_called_with(asset)
        extra = outlet_events.__getitem__.return_value.extra
        extra.__setitem__.assert_called_once_with("airflow.dataquality.result", summary)
