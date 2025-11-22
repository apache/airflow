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

import re
import warnings
import weakref
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from airflow.sdk import Context, Label, TaskGroup
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import DAG, dag as dag_decorator
from airflow.sdk.definitions.param import DagParam, Param, ParamsDict
from airflow.sdk.exceptions import AirflowDagCycleException, DuplicateTaskIdFound, RemovedInAirflow4Warning

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)


class TestDag:
    @pytest.mark.parametrize(
        ("dag_id", "exc_type", "exc_value"),
        [
            pytest.param(
                123,
                TypeError,
                "The key has to be a string and is <class 'int'>:123",
                id="type",
            ),
            pytest.param(
                "a" * 1000,
                ValueError,
                "The key has to be less than 250 characters, not 1000",
                id="long",
            ),
            pytest.param(
                "something*invalid",
                ValueError,
                "The key 'something*invalid' has to be made of alphanumeric characters, dashes, "
                "dots, and underscores exclusively",
                id="illegal",
            ),
        ],
    )
    def test_dag_id_validation(self, dag_id, exc_type, exc_value):
        with pytest.raises(exc_type) as ctx:
            DAG(dag_id)
        assert str(ctx.value) == exc_value

    def test_dag_topological_sort_dag_without_tasks(self):
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert dag.topological_sort() == ()

    def test_dag_naive_start_date_string(self):
        DAG("DAG", schedule=None, default_args={"start_date": "2019-06-01"})

    def test_dag_naive_start_date_constructor_default_args_string(self):
        dag = DAG("DAG", start_date=DEFAULT_DATE, schedule=None, default_args={"start_date": "2019-06-01"})

        assert dag.start_date == DEFAULT_DATE

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

    def test_none_or_empty_access_control_does_not_warn(self) -> None:
        """Ensure that `RemovedInAirflow4Warning` warnings do not arise when `access_control` is `None`."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _ = DAG("test-no-warnings-dag", access_control=None, schedule=None, start_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "role_access_control_entry",
        [
            {},
            {"DAGs": {}},
            {"DAG Runs": {}},
            {"DAGs": {}, "DAG Runs": {}},
            {"DAGs": {"can_read"}},
            {"DAG Runs": {"can_read"}},
            {"DAGs": {"can_read"}, "DAG Runs": {"can_read"}},
        ],
    )
    def test_non_empty_access_control_warns(self, role_access_control_entry: dict[str, set[str]]) -> None:
        """Ensure that `RemovedInAirflow4Warning` warnings are triggered when `access_control` is non-empty."""
        access_control = (
            {"fake-role": role_access_control_entry}
            if len(role_access_control_entry) > 0
            else role_access_control_entry
        )
        with pytest.warns(
            RemovedInAirflow4Warning, match=re.escape("The airflow.security.permissions module is deprecated")
        ):
            _ = DAG("should-warn-dag", access_control=access_control, schedule=None, start_date=DEFAULT_DATE)

    def test_params_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = DAG("test-dag", schedule=None)

        assert isinstance(dag.params, ParamsDict)
        assert len(dag.params) == 0

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

        with pytest.raises(
            ValueError,
            match="Dag 'my-dag' is not allowed to define a Schedule, "
            "as there are required params without default values, or the default values are not valid.",
        ):
            DAG("my-dag", schedule=timedelta(days=1), start_date=DEFAULT_DATE, params=params)

    def test_roots(self):
        """Verify if dag.roots returns the root tasks of a Dag."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            op2 = BaseOperator(task_id="t2")
            op3 = BaseOperator(task_id="t3")
            op4 = BaseOperator(task_id="t4")
            op5 = BaseOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.roots) == {op1, op2}

    def test_leaves(self):
        """Verify if dag.leaves returns the leaf tasks of a Dag."""
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
            with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the Dag"):
                BaseOperator(task_id="t1")

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_not_allowed_without_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        dag = DAG("test_dag", schedule=None, start_date=DEFAULT_DATE)
        op1 = BaseOperator(task_id="t1", dag=dag)
        with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the Dag"):
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
        from airflow.sdk.definitions.taskgroup import TaskGroup

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

    def test_partial_subset_with_depth(self):
        """Test that partial_subset respects the depth parameter for filtering."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            # Create a linear chain: t1 -> t2 -> t3 -> t4 -> t5
            t1 = BaseOperator(task_id="t1")
            t2 = BaseOperator(task_id="t2")
            t3 = BaseOperator(task_id="t3")
            t4 = BaseOperator(task_id="t4")
            t5 = BaseOperator(task_id="t5")
            t1 >> t2 >> t3 >> t4 >> t5

        # Test downstream with depth=1 (only direct downstream)
        partial = dag.partial_subset("t3", include_downstream=True, include_upstream=False, depth=1)
        assert set(partial.task_dict.keys()) == {"t3", "t4"}

        # Test downstream with depth=2
        partial = dag.partial_subset("t3", include_downstream=True, include_upstream=False, depth=2)
        assert set(partial.task_dict.keys()) == {"t3", "t4", "t5"}

        # Test upstream with depth=1 (only direct upstream)
        partial = dag.partial_subset("t3", include_downstream=False, include_upstream=True, depth=1)
        assert set(partial.task_dict.keys()) == {"t2", "t3"}

        # Test upstream with depth=2
        partial = dag.partial_subset("t3", include_downstream=False, include_upstream=True, depth=2)
        assert set(partial.task_dict.keys()) == {"t1", "t2", "t3"}

        # Test both directions with depth=1
        partial = dag.partial_subset("t3", include_downstream=True, include_upstream=True, depth=1)
        assert set(partial.task_dict.keys()) == {"t2", "t3", "t4"}

        # Test with depth=None (unlimited, original behavior - should get all upstream/downstream)
        partial = dag.partial_subset("t3", include_downstream=True, include_upstream=True, depth=None)
        assert set(partial.task_dict.keys()) == {"t1", "t2", "t3", "t4", "t5"}

    def test_partial_subset_with_depth_branching(self):
        """Test partial_subset with depth on a branching DAG structure."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            # Create a diamond pattern:
            #     t1
            #    /  \
            #   t2  t3
            #    \  /
            #     t4
            #     |
            #     t5
            t1 = BaseOperator(task_id="t1")
            t2 = BaseOperator(task_id="t2")
            t3 = BaseOperator(task_id="t3")
            t4 = BaseOperator(task_id="t4")
            t5 = BaseOperator(task_id="t5")
            t1 >> [t2, t3]
            [t2, t3] >> t4
            t4 >> t5

        # From t4, depth=1 upstream should get both t2 and t3
        partial = dag.partial_subset("t4", include_downstream=False, include_upstream=True, depth=1)
        assert set(partial.task_dict.keys()) == {"t2", "t3", "t4"}

        # From t4, depth=2 upstream should get t1, t2, t3
        partial = dag.partial_subset("t4", include_downstream=False, include_upstream=True, depth=2)
        assert set(partial.task_dict.keys()) == {"t1", "t2", "t3", "t4"}

        # From t1, depth=1 downstream should get t2 and t3
        partial = dag.partial_subset("t1", include_downstream=True, include_upstream=False, depth=1)
        assert set(partial.task_dict.keys()) == {"t1", "t2", "t3"}

        # From t1, depth=2 downstream should get t2, t3, and t4
        partial = dag.partial_subset("t1", include_downstream=True, include_upstream=False, depth=2)
        assert set(partial.task_dict.keys()) == {"t1", "t2", "t3", "t4"}

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
        from airflow.sdk.definitions.timetables.simple import ContinuousTimetable

        dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=1)
        assert isinstance(dag.timetable, ContinuousTimetable)
        assert dag.max_active_runs == 1

        dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=0)
        assert isinstance(dag.timetable, ContinuousTimetable)
        assert dag.max_active_runs == 0

        with pytest.raises(ValueError, match="ContinuousTimetable requires max_active_runs <= 1"):
            dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=25)

    def test_dag_add_task_checks_trigger_rule(self):
        # A non fail stop dag should allow any trigger rule
        from airflow.sdk import TriggerRule
        from airflow.sdk.exceptions import FailFastDagInvalidTriggerRule

        class CustomOperator(BaseOperator):
            def execute(self, context):
                pass

        task_with_non_default_trigger_rule = CustomOperator(
            task_id="task_with_non_default_trigger_rule", trigger_rule=TriggerRule.ALWAYS
        )
        non_fail_fast_dag = DAG(
            dag_id="test_dag_add_task_checks_trigger_rule",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=False,
        )
        non_fail_fast_dag.add_task(task_with_non_default_trigger_rule)

        # a fail stop dag should allow default trigger rule
        from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_TRIGGER_RULE

        fail_fast_dag = DAG(
            dag_id="test_dag_add_task_checks_trigger_rule",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=True,
        )
        task_with_default_trigger_rule = CustomOperator(
            task_id="task_with_default_trigger_rule", trigger_rule=DEFAULT_TRIGGER_RULE
        )
        fail_fast_dag.add_task(task_with_default_trigger_rule)

        # a fail stop dag should not allow a non-default trigger rule
        task_with_non_default_trigger_rule = CustomOperator(
            task_id="task_with_non_default_trigger_rule", trigger_rule=TriggerRule.ALWAYS
        )
        with pytest.raises(FailFastDagInvalidTriggerRule):
            fail_fast_dag.add_task(task_with_non_default_trigger_rule)


# Test some of the arg validation. This is not all the validations we perform, just some of them.
@pytest.mark.parametrize(
    ("attr", "value"),
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
    ("tags", "should_pass"),
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
        with pytest.raises(ValueError, match="tag cannot be longer than 100 characters"):
            DAG("test-dag", schedule=None, tags=tags)


@pytest.mark.parametrize(
    ("input_tags", "expected_result"),
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


def test_create_dag_while_active_context():
    """Test that we can safely create a Dag whilst a Dag is activated via ``with dag1:``."""
    with DAG(dag_id="simple_dag"):
        DAG(dag_id="dag2")
        # No asserts needed, it just needs to not fail


@pytest.mark.parametrize("max_active_runs", [0, 1])
def test_continuous_schedule_interval_limits_max_active_runs(max_active_runs):
    from airflow.sdk.definitions.timetables.simple import ContinuousTimetable

    dag = DAG(dag_id="continuous", schedule="@continuous", max_active_runs=max_active_runs)
    assert isinstance(dag.timetable, ContinuousTimetable)
    assert dag.max_active_runs == max_active_runs


def test_continuous_schedule_interval_limits_max_active_runs_error():
    with pytest.raises(
        ValueError, match="Invalid max_active_runs: ContinuousTimetable requires max_active_runs <= 1"
    ):
        DAG(dag_id="continuous", schedule="@continuous", max_active_runs=2)


class TestDagDecorator:
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": datetime.now(tz=timezone.utc),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    VALUE = 42

    def test_dag_decorator_without_args(self):
        """Test that @dag can be used without any arguments."""

        @dag_decorator
        def noop_pipeline(): ...

        dag = noop_pipeline()
        assert dag.dag_id == "noop_pipeline"

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
        ("dag_doc_md", "expected_doc_md"),
        [
            pytest.param("dag docs.", "dag docs.", id="use_dag_doc_md"),
            pytest.param(None, "Regular Dag documentation", id="use_dag_docstring"),
        ],
    )
    def test_documentation_added(self, dag_doc_md, expected_doc_md):
        """Test that @dag uses function docs as doc_md for Dag object if doc_md is not explicitly set."""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS, doc_md=dag_doc_md)
        def noop_pipeline():
            """Regular Dag documentation"""

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

    def test_documentation_template_rendered(self):
        """Test that @dag uses function docs as doc_md for Dag object"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline():
            """
            {% if True %}
               Regular Dag documentation
            {% endif %}
            """

        dag = noop_pipeline()
        assert dag.dag_id == "noop_pipeline"
        assert "Regular Dag documentation" in dag.doc_md

    def test_resolve_documentation_template_file_not_rendered(self, tmp_path):
        """Test that @dag uses function docs as doc_md for Dag object"""

        raw_content = """
        {% if True %}
            External Markdown Dag documentation
        {% endif %}
        """

        path = tmp_path / "testfile.md"
        path.write_text(raw_content)

        @dag_decorator("test-dag", schedule=None, start_date=DEFAULT_DATE, doc_md=str(path))
        def markdown_docs(): ...

        dag = markdown_docs()
        assert dag.dag_id == "test-dag"
        assert dag.doc_md == raw_content

    def test_dag_param_resolves(self):
        """Test that dag param is correctly resolved by operator"""
        from airflow.decorators import task

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=self.VALUE):
            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator

        xcom_pass_to_op()

        assert isinstance(self.operator.op_args[0], DagParam)
        self.operator.render_template_fields({})
        assert self.operator.op_args[0] == 42


class DoNothingOperator(BaseOperator):
    """
    An operator that does nothing.
    Used to test Dag cycle detection.
    """

    def execute(self, context: Context) -> None:
        pass


class TestCycleTester:
    def test_cycle_empty(self):
        # test empty
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert not dag.check_cycle()

    def test_cycle_single_task(self):
        # test single task
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        with dag:
            DoNothingOperator(task_id="A")

        assert not dag.check_cycle()

    def test_semi_complex(self):
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            create_cluster = DoNothingOperator(task_id="c")
            pod_task = DoNothingOperator(task_id="p")
            pod_task_xcom = DoNothingOperator(task_id="x")
            delete_cluster = DoNothingOperator(task_id="d")
            pod_task_xcom_result = DoNothingOperator(task_id="r")
            create_cluster >> pod_task >> delete_cluster
            create_cluster >> pod_task_xcom >> delete_cluster
            pod_task_xcom >> pod_task_xcom_result

    def test_cycle_no_cycle(self):
        # test no cycle
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            op1 = DoNothingOperator(task_id="A")
            op2 = DoNothingOperator(task_id="B")
            op3 = DoNothingOperator(task_id="C")
            op4 = DoNothingOperator(task_id="D")
            op5 = DoNothingOperator(task_id="E")
            op6 = DoNothingOperator(task_id="F")
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op2.set_downstream(op4)
            op5.set_downstream(op6)

        assert not dag.check_cycle()

    def test_cycle_loop(self):
        # test self loop
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> A
        with dag:
            op1 = DoNothingOperator(task_id="A")
            op1.set_downstream(op1)

        with pytest.raises(AirflowDagCycleException):
            assert not dag.check_cycle()

    def test_cycle_downstream_loop(self):
        # test downstream self loop
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> B -> C -> D -> E -> E
        with dag:
            op1 = DoNothingOperator(task_id="A")
            op2 = DoNothingOperator(task_id="B")
            op3 = DoNothingOperator(task_id="C")
            op4 = DoNothingOperator(task_id="D")
            op5 = DoNothingOperator(task_id="E")
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)
            op5.set_downstream(op5)

        with pytest.raises(AirflowDagCycleException):
            assert not dag.check_cycle()

    def test_cycle_large_loop(self):
        # large loop
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> B -> C -> D -> E -> A
        with dag:
            start = DoNothingOperator(task_id="start")
            current = start

            for i in range(10000):
                next_task = DoNothingOperator(task_id=f"task_{i}")
                current.set_downstream(next_task)
                current = next_task

            current.set_downstream(start)
        with pytest.raises(AirflowDagCycleException):
            assert not dag.check_cycle()

    def test_cycle_arbitrary_loop(self):
        # test arbitrary loop
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # E-> A -> B -> F -> A
        #       -> C -> F
        with dag:
            op1 = DoNothingOperator(task_id="A")
            op2 = DoNothingOperator(task_id="B")
            op3 = DoNothingOperator(task_id="C")
            op4 = DoNothingOperator(task_id="E")
            op5 = DoNothingOperator(task_id="F")
            op1.set_downstream(op2)
            op1.set_downstream(op3)
            op4.set_downstream(op1)
            op3.set_downstream(op5)
            op2.set_downstream(op5)
            op5.set_downstream(op1)

        with pytest.raises(AirflowDagCycleException):
            assert not dag.check_cycle()

    def test_cycle_task_group_with_edge_labels(self):
        # Test a cycle is not detected when Labels are used between tasks in Task Groups.

        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        with dag:
            with TaskGroup(group_id="group"):
                op1 = DoNothingOperator(task_id="A")
                op2 = DoNothingOperator(task_id="B")

                op1 >> Label("label") >> op2

        assert not dag.check_cycle()
