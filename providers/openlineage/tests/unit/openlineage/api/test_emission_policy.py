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
from pendulum import now

from airflow.providers.common.compat.sdk import DAG, XComArg, dag, task
from airflow.providers.openlineage.api.emission_policy import (
    extend_global_openlineage_emission_policy,
)
from airflow.providers.openlineage.utils.emission_policy import (
    OL_EMISSION_POLICY_PARAM,
    _read_param,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def _make_dag_and_task(dag_id: str = "test_dag"):
    """Create a minimal DAG with one EmptyOperator task."""
    with DAG(dag_id=dag_id, schedule=None, start_date=now()) as d:
        t = EmptyOperator(task_id="test_task")
    return d, t


def _task_flags(task) -> dict:
    """Internal-access helper for tests — read the raw stored task flags."""
    return _read_param(task, OL_EMISSION_POLICY_PARAM)


def _dag_flags(dag) -> dict:
    """Internal-access helper for tests — read the raw stored dag flags."""
    return _read_param(dag, OL_EMISSION_POLICY_PARAM)


class TestExtendOnTask:
    def test_emit_false_stored_on_task(self):
        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=False)
        assert _task_flags(task) == {"emit": False}

    def test_multiple_flags_stored_together(self):
        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(
            task,
            include_source_code=False,
            hook_lineage=False,
            extract_operator_metadata=True,
        )
        assert _task_flags(task) == {
            "include_source_code": False,
            "hook_lineage": False,
            "extract_operator_metadata": True,
        }

    def test_no_flags_provided_returns_obj_unchanged(self):
        _, task = _make_dag_and_task()
        result = extend_global_openlineage_emission_policy(task)
        assert result is task
        assert _task_flags(task) == {}

    def test_returns_same_object(self):
        _, task = _make_dag_and_task()
        result = extend_global_openlineage_emission_policy(task, emit=False)
        assert result is task

    def test_successive_calls_merge_flags(self):
        """Multiple calls accumulate — later call wins for the same key."""
        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=False, include_source_code=False)
        extend_global_openlineage_emission_policy(task, include_source_code=True)
        flags = _task_flags(task)
        assert flags["emit"] is False
        assert flags["include_source_code"] is True

    def test_emit_dag_events_on_task_not_stored(self):
        """emit_dag_events on a task is silently discarded (not a task-level flag)."""
        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit_dag_events=False)
        assert "emit_dag_events" not in _task_flags(task)


class TestExtendOnDag:
    def test_dag_emit_false_stored_on_dag(self):
        dag, _ = _make_dag_and_task()
        extend_global_openlineage_emission_policy(dag, emit=False)
        assert _dag_flags(dag) == {"emit": False}

    def test_emit_dag_events_stored_on_dag(self):
        dag, _ = _make_dag_and_task()
        extend_global_openlineage_emission_policy(dag, emit_dag_events=False)
        assert _dag_flags(dag) == {"emit_dag_events": False}

    def test_dag_call_propagates_task_flags_to_all_tasks(self):
        with DAG(dag_id="prop_dag", schedule=None, start_date=now()) as dag:
            t1 = EmptyOperator(task_id="t1")
            t2 = EmptyOperator(task_id="t2")
            t3 = EmptyOperator(task_id="t3")

        extend_global_openlineage_emission_policy(dag, include_source_code=False)

        for t in (t1, t2, t3):
            assert _task_flags(t).get("include_source_code") is False

    def test_dag_emit_propagates_to_all_tasks_and_dag(self):
        with DAG(dag_id="emit_dag", schedule=None, start_date=now()) as dag:
            t1 = EmptyOperator(task_id="t1")
            t2 = EmptyOperator(task_id="t2")

        extend_global_openlineage_emission_policy(dag, emit=False)

        for t in (t1, t2):
            assert _task_flags(t).get("emit") is False
        assert _dag_flags(dag).get("emit") is False

    def test_emit_dag_events_only_affects_dag_not_tasks(self):
        with DAG(dag_id="ede_dag", schedule=None, start_date=now()) as dag:
            t1 = EmptyOperator(task_id="t1")

        extend_global_openlineage_emission_policy(dag, emit_dag_events=False)

        assert _dag_flags(dag).get("emit_dag_events") is False
        assert "emit_dag_events" not in _task_flags(t1)

    def test_dag_returns_dag_object(self):
        dag, _ = _make_dag_and_task()
        result = extend_global_openlineage_emission_policy(dag, emit=False)
        assert result is dag

    def test_dag_no_tasks_does_not_fail(self):
        """Calling on a DAG with no tasks should not raise."""
        with DAG(dag_id="empty_dag", schedule=None, start_date=now()) as dag:
            pass
        extend_global_openlineage_emission_policy(dag, include_source_code=False)


class TestExtendOnXComArg:
    def test_xcomarg_delegates_to_operator(self):
        with DAG(dag_id="xcom_dag", schedule=None, start_date=now()):
            op = PythonOperator(task_id="xcom_task", python_callable=lambda: None)

        xarg = XComArg(op)
        result = extend_global_openlineage_emission_policy(xarg, emit=False)
        assert result is xarg
        assert _task_flags(op).get("emit") is False


class TestExtendOnTaskFlow:
    """TaskFlow API: ``@task``-decorated calls return an XComArg, ``@dag`` returns a DAG."""

    def test_task_decorator_call_stores_flags_on_operator(self):
        @task
        def my_task():
            return None

        with DAG(dag_id="tf_task_dag", schedule=None, start_date=now()):
            xarg = my_task()

        result = extend_global_openlineage_emission_policy(xarg, emit=False)
        assert result is xarg
        assert _task_flags(xarg.operator).get("emit") is False

    def test_task_decorator_multiple_flags(self):
        @task
        def my_task():
            return None

        with DAG(dag_id="tf_task_multi_dag", schedule=None, start_date=now()):
            xarg = my_task()

        extend_global_openlineage_emission_policy(
            xarg,
            include_source_code=False,
            hook_lineage=False,
        )
        assert _task_flags(xarg.operator) == {
            "include_source_code": False,
            "hook_lineage": False,
        }

    def test_dag_decorator_propagates_task_flags(self):
        @task
        def t1():
            return None

        @task
        def t2():
            return None

        @dag(schedule=None, start_date=now())
        def my_pipeline():
            t1()
            t2()

        dag_obj = my_pipeline()

        extend_global_openlineage_emission_policy(dag_obj, include_source_code=False)

        for task_obj in dag_obj.task_dict.values():
            assert _task_flags(task_obj).get("include_source_code") is False

    def test_dag_decorator_emit_dag_events_stored_on_dag(self):
        @task
        def t1():
            return None

        @dag(schedule=None, start_date=now())
        def my_pipeline():
            t1()

        dag_obj = my_pipeline()

        extend_global_openlineage_emission_policy(dag_obj, emit_dag_events=False)

        assert _dag_flags(dag_obj).get("emit_dag_events") is False
        for task_obj in dag_obj.task_dict.values():
            assert "emit_dag_events" not in _task_flags(task_obj)

    def test_dag_decorator_emit_propagates_to_tasks_and_dag(self):
        @task
        def t1():
            return None

        @task
        def t2():
            return None

        @dag(schedule=None, start_date=now())
        def my_pipeline():
            t1()
            t2()

        dag_obj = my_pipeline()

        extend_global_openlineage_emission_policy(dag_obj, emit=False)

        assert _dag_flags(dag_obj).get("emit") is False
        for task_obj in dag_obj.task_dict.values():
            assert _task_flags(task_obj).get("emit") is False


class TestUnsupportedObjects:
    """extend_global_openlineage_emission_policy() rejects anything else.

    There is intentionally no "global authoring scope": deployment-wide changes belong in the
    ``emission_policy`` Airflow configuration with an empty ``scope: {}`` rule.
    """

    @pytest.mark.parametrize(
        "obj",
        [None, 42, "a_string", object(), {"some": "dict"}, [1, 2, 3]],
        ids=["None", "int", "str", "object", "dict", "list"],
    )
    def test_rejects_non_dag_non_task_object(self, obj):
        with pytest.raises(TypeError, match="DAG, an Operator, or an XComArg"):
            extend_global_openlineage_emission_policy(obj, emit=False)

    def test_error_message_mentions_global_scope_workaround(self):
        with pytest.raises(TypeError, match="emission_policy"):
            extend_global_openlineage_emission_policy(object(), emit=False)

    def test_no_flags_provided_still_raises_for_unsupported_type(self):
        """Even with no flags, a wrong argument type fails loud rather than silently no-op."""
        with pytest.raises(TypeError):
            extend_global_openlineage_emission_policy(object())
