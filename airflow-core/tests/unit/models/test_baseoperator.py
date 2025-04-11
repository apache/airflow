#
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

import copy
from collections import defaultdict
from datetime import datetime

import pytest

from airflow.exceptions import TaskDeferralTimeout
from airflow.models.baseoperator import (
    BaseOperator,
)
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import TriggerFailureReason
from airflow.providers.common.sql.operators import sql
from airflow.sdk import task as task_decorator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType

from tests_common.test_utils.mock_operators import MockOperator
from unit.models import DEFAULT_DATE


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class TestBaseOperator:
    def test_trigger_rule_validation(self):
        from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_TRIGGER_RULE

        fail_fast_dag = DAG(
            dag_id="test_dag_trigger_rule_validation",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=True,
        )
        non_fail_fast_dag = DAG(
            dag_id="test_dag_trigger_rule_validation",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=False,
        )

        # An operator with default trigger rule and a fail-stop dag should be allowed
        BaseOperator(task_id="test_valid_trigger_rule", dag=fail_fast_dag, trigger_rule=DEFAULT_TRIGGER_RULE)
        # An operator with non default trigger rule and a non fail-stop dag should be allowed
        BaseOperator(
            task_id="test_valid_trigger_rule", dag=non_fail_fast_dag, trigger_rule=TriggerRule.ALWAYS
        )

    def test_baseoperator_raises_exception_when_task_id_plus_taskgroup_id_exceeds_250_chars(self):
        """Test exception is raised when operator task id + taskgroup id > 250 chars."""
        dag = DAG(dag_id="foo", schedule=None, start_date=datetime.now())

        tg1 = TaskGroup("A" * 20, dag=dag)
        with pytest.raises(ValueError, match="The key has to be less than 250 characters"):
            BaseOperator(task_id="1" * 250, task_group=tg1, dag=dag)

    def test_baseoperator_with_task_id_and_taskgroup_id_less_than_250_chars(self):
        """Test exception is not raised when operator task id + taskgroup id < 250 chars."""
        dag = DAG(dag_id="foo", schedule=None, start_date=datetime.now())

        tg1 = TaskGroup("A" * 10, dag=dag)
        try:
            BaseOperator(task_id="1" * 239, task_group=tg1, dag=dag)
        except Exception as e:
            pytest.fail(f"Exception raised: {e}")

    def test_baseoperator_with_task_id_less_than_250_chars(self):
        """Test exception is not raised when operator task id  < 250 chars."""
        dag = DAG(dag_id="foo", schedule=None, start_date=datetime.now())

        try:
            BaseOperator(task_id="1" * 249, dag=dag)
        except Exception as e:
            pytest.fail(f"Exception raised: {e}")

    def test_task_naive_datetime(self):
        naive_datetime = DEFAULT_DATE.replace(tzinfo=None)

        op_no_dag = BaseOperator(
            task_id="test_task_naive_datetime", start_date=naive_datetime, end_date=naive_datetime
        )

        assert op_no_dag.start_date.tzinfo
        assert op_no_dag.end_date.tzinfo

    # ensure the default logging config is used for this test, no matter what ran before
    @pytest.mark.usefixtures("reset_logging_config")
    def test_logging_propogated_by_default(self, caplog):
        """Test that when set_context hasn't been called that log records are emitted"""
        BaseOperator(task_id="test").log.warning("test")
        # This looks like "how could it fail" but this actually checks that the handler called `emit`. Testing
        # the other case (that when we have set_context it goes to the file is harder to achieve without
        # leaking a lot of state)
        assert caplog.messages == ["test"]

    def test_resume_execution(self):
        op = BaseOperator(task_id="hi")
        with pytest.raises(TaskDeferralTimeout):
            op.resume_execution(
                next_method="__fail__",
                next_kwargs={"error": TriggerFailureReason.TRIGGER_TIMEOUT},
                context={},
            )


def test_deepcopy():
    # Test bug when copying an operator attached to a DAG
    with DAG("dag0", schedule=None, start_date=DEFAULT_DATE) as dag:

        @dag.task
        def task0():
            pass

        MockOperator(task_id="task1", arg1=task0())
    copy.deepcopy(dag)


def get_states(dr):
    """
    For a given dag run, get a dict of states.

    Example::
        {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
            "my_work": "failed",
        }
    """
    ti_dict = defaultdict(dict)
    for ti in dr.get_task_instances():
        if ti.map_index == -1:
            ti_dict[ti.task_id] = ti.state
        else:
            ti_dict[ti.task_id][ti.map_index] = ti.state
    return dict(ti_dict)


@pytest.mark.db_test
def test_teardown_and_fail_fast(dag_maker):
    """
    when fail_fast enabled, teardowns should run according to their setups.
    in this case, the second teardown skips because its setup skips.
    """
    with dag_maker(fail_fast=True) as dag:
        for num in (1, 2):
            with TaskGroup(f"tg_{num}"):

                @task_decorator
                def my_setup():
                    print("setting up multiple things")
                    return [1, 2, 3]

                @task_decorator
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    raise ValueError("this fails")
                    return val

                @task_decorator
                def my_teardown():
                    print("teardown")

                s = my_setup()
                t = my_teardown().as_teardown(setups=s)
                with t:
                    my_work(s)
    tg1, tg2 = dag.task_group.children.values()
    tg1 >> tg2
    dr = dag.test()
    states = get_states(dr)
    expected = {
        "tg_1.my_setup": "success",
        "tg_1.my_teardown": "success",
        "tg_1.my_work": "failed",
        "tg_2.my_setup": "skipped",
        "tg_2.my_teardown": "skipped",
        "tg_2.my_work": "skipped",
    }
    assert states == expected


@pytest.mark.db_test
def test_get_task_instances(session):
    import pendulum

    first_logical_date = pendulum.datetime(2023, 1, 1)
    second_logical_date = pendulum.datetime(2023, 1, 2)
    third_logical_date = pendulum.datetime(2023, 1, 3)

    test_dag = DAG(dag_id="test_dag", schedule=None, start_date=first_logical_date)
    task = BaseOperator(task_id="test_task", dag=test_dag)

    common_dr_kwargs = {
        "dag_id": test_dag.dag_id,
        "run_type": DagRunType.MANUAL,
    }
    dr1 = DagRun(logical_date=first_logical_date, run_id="test_run_id_1", **common_dr_kwargs)
    ti_1 = TaskInstance(run_id=dr1.run_id, task=task)
    dr2 = DagRun(logical_date=second_logical_date, run_id="test_run_id_2", **common_dr_kwargs)
    ti_2 = TaskInstance(run_id=dr2.run_id, task=task)
    dr3 = DagRun(logical_date=third_logical_date, run_id="test_run_id_3", **common_dr_kwargs)
    ti_3 = TaskInstance(run_id=dr3.run_id, task=task)
    session.add_all([dr1, dr2, dr3, ti_1, ti_2, ti_3])
    session.commit()

    # get all task instances
    assert task.get_task_instances(session=session) == [ti_1, ti_2, ti_3]
    # get task instances with start_date
    assert task.get_task_instances(session=session, start_date=second_logical_date) == [ti_2, ti_3]
    # get task instances with end_date
    assert task.get_task_instances(session=session, end_date=second_logical_date) == [ti_1, ti_2]
    # get task instances with start_date and end_date
    assert task.get_task_instances(
        session=session, start_date=second_logical_date, end_date=second_logical_date
    ) == [ti_2]


def test_mro():
    class Mixin(sql.BaseSQLOperator):
        pass

    class Branch(Mixin, sql.BranchSQLOperator):
        pass

    # The following throws an exception if metaclass breaks MRO:
    #   airflow.exceptions.AirflowException: Invalid arguments were passed to Branch (task_id: test). Invalid arguments were:
    #   **kwargs: {'sql': 'sql', 'follow_task_ids_if_true': ['x'], 'follow_task_ids_if_false': ['y']}
    op = Branch(
        task_id="test",
        conn_id="abc",
        sql="sql",
        follow_task_ids_if_true=["x"],
        follow_task_ids_if_false=["y"],
    )
    assert isinstance(op, Branch)
