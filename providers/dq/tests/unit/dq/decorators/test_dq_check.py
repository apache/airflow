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

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.dq.backends.object_storage import ObjectStorageResultsBackend
from airflow.providers.dq.decorators.dq_check import _DQCheckDecoratedOperator
from airflow.providers.dq.rules import DQRule, RuleSet
from airflow.sdk.execution_time.context import OutletEventAccessors

NULLS = DQRule(name="nulls", check="null_count", column="id", condition={"equal_to": 0})
RULESET = RuleSet(name="orders", rules=(NULLS,))


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


@pytest.fixture(autouse=True)
def results_backend(tmp_path):
    """Patch the module-level config lookup so persisted results land in an isolated tmp_path."""
    backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
    with mock.patch("airflow.providers.dq.operators.dq_check.get_backend_from_config", return_value=backend):
        yield backend


def make_decorated_operator(records, python_callable, **kwargs):
    kwargs.setdefault("task_id", "dq")
    if not kwargs.pop("omit_ruleset", False):
        kwargs.setdefault("ruleset", RULESET)
    kwargs.setdefault("table", "orders")
    operator = _DQCheckDecoratedOperator(conn_id="warehouse", python_callable=python_callable, **kwargs)
    hook = mock.create_autospec(DbApiHook, instance=True)
    hook.get_records.return_value = records
    return operator, hook


class TestDQCheckDecoratedOperator:
    def test_custom_operator_name(self):
        assert _DQCheckDecoratedOperator.custom_operator_name == "@task.dq_check"

    @mock.patch.object(_DQCheckDecoratedOperator, "get_db_hook")
    def test_none_return_runs_check_as_declared(self, mock_get_db_hook):
        operator, hook = make_decorated_operator(records=[(NULLS.rule_uid, 0)], python_callable=lambda: None)
        mock_get_db_hook.return_value = hook

        summary = operator.execute(make_context())

        assert operator.table == "orders"
        assert summary["passed"] == 1

    @mock.patch.object(_DQCheckDecoratedOperator, "get_db_hook")
    def test_ruleset_can_be_provided_only_at_runtime(self, mock_get_db_hook):
        other_rule = DQRule(name="row_count_ok", check="row_count", condition={"greater_than": 0})
        other_ruleset = RuleSet(name="dynamic", rules=(other_rule,))

        operator, hook = make_decorated_operator(
            records=[(other_rule.rule_uid, 10)],
            python_callable=lambda: other_ruleset,
            omit_ruleset=True,
        )
        mock_get_db_hook.return_value = hook

        summary = operator.execute(make_context())

        assert summary["passed"] == 1

    def test_missing_runtime_ruleset_raises_value_error(self):
        operator, hook = make_decorated_operator(records=[], python_callable=lambda: None, omit_ruleset=True)
        with mock.patch.object(_DQCheckDecoratedOperator, "get_db_hook", return_value=hook):
            with pytest.raises(ValueError, match="ruleset is required"):
                operator.execute(make_context())

    @mock.patch.object(_DQCheckDecoratedOperator, "get_db_hook")
    def test_op_kwargs_are_passed_to_callable(self, mock_get_db_hook):
        seen = {}

        def resolve_target(suffix):
            seen["suffix"] = suffix
            return None

        operator, hook = make_decorated_operator(
            records=[(NULLS.rule_uid, 0)],
            python_callable=resolve_target,
            op_kwargs={"suffix": "eu"},
        )
        mock_get_db_hook.return_value = hook

        operator.execute(make_context())

        assert seen["suffix"] == "eu"

    def test_invalid_return_raises_type_error(self):
        operator, hook = make_decorated_operator(records=[], python_callable=lambda: 42)
        with mock.patch.object(_DQCheckDecoratedOperator, "get_db_hook", return_value=hook):
            with pytest.raises(TypeError, match="must return a RuleSet"):
                operator.execute(make_context())
