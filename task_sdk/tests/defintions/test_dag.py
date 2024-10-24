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

import weakref
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from airflow.exceptions import DuplicateTaskIdFound
from airflow.models.param import Param, ParamsDict
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.definitions.dag import DAG, dag as dag_decorator

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)


class TestDag:
    def test_dag_topological_sort_dag_without_tasks(self):
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert () == dag.topological_sort()

    def test_dag_naive_start_date_string(self):
        DAG("DAG", schedule=None, default_args={"start_date": "2019-06-01"})

    def test_dag_naive_start_end_dates_strings(self):
        DAG("DAG", schedule=None, default_args={"start_date": "2019-06-01", "end_date": "2019-06-05"})

    def test_dag_start_date_propagates_to_end_date(self):
        """
        Tests that a start_date string with a timezone and an end_date string without a timezone
        are accepted and that the timezone from the start carries over the end

        This test is a little indirect, it works by setting start and end equal except for the
        timezone and then testing for equality after the DAG construction.  They'll be equal
        only if the same timezone was applied to both.

        An explicit check the `tzinfo` attributes for both are the same is an extra check.
        """
        dag = DAG(
            "DAG",
            schedule=None,
            default_args={"start_date": "2019-06-05T00:00:00+05:00", "end_date": "2019-06-05T00:00:00"},
        )
        assert dag.default_args["start_date"] == dag.default_args["end_date"]
        assert dag.default_args["start_date"].tzinfo == dag.default_args["end_date"].tzinfo

    def test_dag_as_context_manager(self):
        """
        Test DAG as a context manager.
        When used as a context manager, Operators are automatically added to
        the DAG (unless they specify a different DAG)
        """
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})
        dag2 = DAG("dag2", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner2"})

        with dag:
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2", dag=dag2)

        assert op1.dag is dag
        assert op1.owner == "owner1"
        assert op2.dag is dag2
        assert op2.owner == "owner2"

        with dag2:
            op3 = BaseOperator(task_id="op3")

        assert op3.dag is dag2
        assert op3.owner == "owner2"

        with dag:
            with dag2:
                op4 = BaseOperator(task_id="op4")
            op5 = BaseOperator(task_id="op5")

        assert op4.dag is dag2
        assert op5.dag is dag
        assert op4.owner == "owner2"
        assert op5.owner == "owner1"

        with DAG("creating_dag_in_cm", schedule=None, start_date=DEFAULT_DATE) as dag:
            BaseOperator(task_id="op6")

        assert dag.dag_id == "creating_dag_in_cm"
        assert dag.tasks[0].task_id == "op6"

        with dag:
            with dag:
                op7 = BaseOperator(task_id="op7")
            op8 = BaseOperator(task_id="op8")
        op9 = BaseOperator(task_id="op8")
        op9.dag = dag2

        assert op7.dag == dag
        assert op8.dag == dag
        assert op9.dag == dag2

    def test_params_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = DAG("test-dag", schedule=None)

        assert isinstance(dag.params, ParamsDict)
        assert 0 == len(dag.params)

    def test_params_passed_and_params_in_default_args_no_override(self):
        """
        Test that when 'params' exists as a key passed to the default_args dict
        in addition to params being passed explicitly as an argument to the
        dag, that the 'params' key of the default_args dict is merged with the
        dict of the params argument.
        """
        params1 = {"parameter1": 1}
        params2 = {"parameter2": 2}

        dag = DAG("test-dag", schedule=None, default_args={"params": params1}, params=params2)

        assert params1["parameter1"] == dag.params["parameter1"]
        assert params2["parameter2"] == dag.params["parameter2"]

    def test_not_none_schedule_with_non_default_params(self):
        """
        Test if there is a DAG with a schedule and have some params that don't have a default value raise a
        error while DAG parsing. (Because we can't schedule them if there we don't know what value to use)
        """
        params = {"param1": Param(type="string")}

        with pytest.raises(ValueError):
            DAG("my-dag", schedule=timedelta(days=1), start_date=DEFAULT_DATE, params=params)

    def test_roots(self):
        """Verify if dag.roots returns the root tasks of a DAG."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            op2 = BaseOperator(task_id="t2")
            op3 = BaseOperator(task_id="t3")
            op4 = BaseOperator(task_id="t4")
            op5 = BaseOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.roots) == {op1, op2}

    def test_leaves(self):
        """Verify if dag.leaves returns the leaf tasks of a DAG."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            op2 = BaseOperator(task_id="t2")
            op3 = BaseOperator(task_id="t3")
            op4 = BaseOperator(task_id="t4")
            op5 = BaseOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.leaves) == {op4, op5}

    def test_duplicate_task_ids_not_allowed_with_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
                BaseOperator(task_id="t1")

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_not_allowed_without_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        dag = DAG("test_dag", schedule=None, start_date=DEFAULT_DATE)
        op1 = BaseOperator(task_id="t1", dag=dag)
        with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
            BaseOperator(task_id="t1", dag=dag)

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_for_same_task_is_allowed(self):
        """Verify that same tasks with Duplicate task_id do not raise error"""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = op2 = BaseOperator(task_id="t1")
            op3 = BaseOperator(task_id="t3")
            op1 >> op3
            op2 >> op3

        assert op1 == op2
        assert dag.task_dict == {op1.task_id: op1, op3.task_id: op3}
        assert dag.task_dict == {op2.task_id: op2, op3.task_id: op3}

    def test_fail_dag_when_schedule_is_non_none_and_empty_start_date(self):
        # Check that we get a ValueError 'start_date' for self.start_date when schedule is non-none
        with pytest.raises(ValueError, match="start_date is required when catchup=True"):
            DAG(dag_id="dag_with_non_none_schedule_and_empty_start_date", schedule="@hourly", catchup=True)

    def test_partial_subset_updates_all_references_while_deepcopy(self):
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            op2 = BaseOperator(task_id="t2")
            op3 = BaseOperator(task_id="t3")
            op1 >> op2
            op2 >> op3

        partial = dag.partial_subset("t2", include_upstream=True, include_downstream=False)
        assert id(partial.task_dict["t1"].downstream_list[0].dag) == id(partial)

        # Copied DAG should not include unused task IDs in used_group_ids
        assert "t3" not in partial.task_group.used_group_ids

    def test_partial_subset_taskgroup_join_ids(self):
        from airflow.sdk import TaskGroup

        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            start = BaseOperator(task_id="start")
            with TaskGroup(group_id="outer", prefix_group_id=False) as outer_group:
                with TaskGroup(group_id="tg1", prefix_group_id=False) as tg1:
                    BaseOperator(task_id="t1")
                with TaskGroup(group_id="tg2", prefix_group_id=False) as tg2:
                    BaseOperator(task_id="t2")

                start >> tg1 >> tg2

        # Pre-condition checks
        task = dag.get_task("t2")
        assert task.task_group.upstream_group_ids == {"tg1"}
        assert isinstance(task.task_group.parent_group, weakref.ProxyType)
        assert task.task_group.parent_group == outer_group

        partial = dag.partial_subset(["t2"], include_upstream=True, include_downstream=False)
        copied_task = partial.get_task("t2")
        assert copied_task.task_group.upstream_group_ids == {"tg1"}
        assert isinstance(copied_task.task_group.parent_group, weakref.ProxyType)
        assert copied_task.task_group.parent_group

        # Make sure we don't affect the original!
        assert task.task_group.upstream_group_ids is not copied_task.task_group.upstream_group_ids

    def test_dag_owner_links(self):
        dag = DAG(
            "dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            owner_links={"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"},
        )

        assert dag.owner_links == {"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"}

        # Check wrong formatted owner link
        with pytest.raises(ValueError, match="Wrong link format"):
            DAG("dag", schedule=None, start_date=DEFAULT_DATE, owner_links={"owner1": "my-bad-link"})

        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE)
        dag.owner_links["owner1"] = "my-bad-link"
        with pytest.raises(ValueError, match="Wrong link format"):
            dag.validate()

    def test_continuous_schedule_linmits_max_active_runs(self):
        from airflow.timetables.simple import ContinuousTimetable

        dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=1)
        assert isinstance(dag.timetable, ContinuousTimetable)
        assert dag.max_active_runs == 1

        dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=0)
        assert isinstance(dag.timetable, ContinuousTimetable)
        assert dag.max_active_runs == 0

        with pytest.raises(ValueError, match="ContinuousTimetable requires max_active_runs <= 1"):
            dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=25)


# Test some of the arg valiadtion. This is not all the validations we perform, just some of them.
@pytest.mark.parametrize(
    ["attr", "value"],
    [
        pytest.param("max_consecutive_failed_dag_runs", "not_an_int", id="max_consecutive_failed_dag_runs"),
        pytest.param("dagrun_timeout", "not_an_int", id="dagrun_timeout"),
        pytest.param("max_active_runs", "not_an_int", id="max_active_runs"),
    ],
)
def test_invalid_type_for_args(attr: str, value: Any):
    with pytest.raises(TypeError):
        DAG("invalid-default-args", **{attr: value})


@pytest.mark.parametrize(
    "tags, should_pass",
    [
        pytest.param([], True, id="empty tags"),
        pytest.param(["a normal tag"], True, id="one tag"),
        pytest.param(["a normal tag", "another normal tag"], True, id="two tags"),
        pytest.param(["a" * 100], True, id="a tag that's of just length 100"),
        pytest.param(["a normal tag", "a" * 101], False, id="two tags and one of them is of length > 100"),
    ],
)
def test__tags_length(tags: list[str], should_pass: bool):
    if should_pass:
        DAG("test-dag", schedule=None, tags=tags)
    else:
        with pytest.raises(ValueError):
            DAG("test-dag", schedule=None, tags=tags)


@pytest.mark.parametrize(
    "input_tags, expected_result",
    [
        pytest.param([], set(), id="empty tags"),
        pytest.param(
            ["a normal tag"],
            {"a normal tag"},
            id="one tag",
        ),
        pytest.param(
            ["a normal tag", "another normal tag"],
            {"a normal tag", "another normal tag"},
            id="two different tags",
        ),
        pytest.param(
            ["a", "a"],
            {"a"},
            id="two same tags",
        ),
    ],
)
def test__tags_duplicates(input_tags: list[str], expected_result: set[str]):
    result = DAG("test-dag", tags=input_tags)
    assert result.tags == expected_result


def test__tags_mutable():
    expected_tags = {"6", "7"}
    test_dag = DAG("test-dag")
    test_dag.tags.add("6")
    test_dag.tags.add("7")
    test_dag.tags.add("8")
    test_dag.tags.remove("8")
    assert test_dag.tags == expected_tags


class TestDagDecorator:
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": datetime.now(tz=timezone.utc),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    VALUE = 42

    def test_fileloc(self):
        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline(): ...

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "noop_pipeline"
        assert dag.fileloc == __file__

    def test_set_dag_id(self):
        """Test that checks you can set dag_id from decorator."""

        @dag_decorator("test", schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline(): ...

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "test"

    def test_default_dag_id(self):
        """Test that @dag uses function name as default dag id."""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline(): ...

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "noop_pipeline"

    @pytest.mark.parametrize(
        argnames=["dag_doc_md", "expected_doc_md"],
        argvalues=[
            pytest.param("dag docs.", "dag docs.", id="use_dag_doc_md"),
            pytest.param(None, "Regular DAG documentation", id="use_dag_docstring"),
        ],
    )
    def test_documentation_added(self, dag_doc_md, expected_doc_md):
        """Test that @dag uses function docs as doc_md for DAG object if doc_md is not explicitly set."""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS, doc_md=dag_doc_md)
        def noop_pipeline():
            """Regular DAG documentation"""

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "noop_pipeline"
        assert dag.doc_md == expected_doc_md

    def test_fails_if_arg_not_set(self):
        """Test that @dag decorated function fails if positional argument is not set"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline(value): ...

        # Test that if arg is not passed it raises a type error as expected.
        with pytest.raises(TypeError):
            noop_pipeline()
