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

import itertools

import pytest

from airflow.decorators import setup, task, teardown
from airflow.models.baseoperator import BaseOperator

pytestmark = pytest.mark.db_test


def cleared_tasks(dag, task_id):
    dag_ = dag.partial_subset(task_id, include_downstream=True, include_upstream=False)
    return {x.task_id for x in dag_.tasks}


def get_task_attr(task_like, attr):
    try:
        return getattr(task_like, attr)
    except AttributeError:
        return getattr(task_like.operator, attr)


def make_task(name, type_, setup_=False, teardown_=False):
    if type_ == "classic" and setup_:
        return BaseOperator(task_id=name).as_setup()
    elif type_ == "classic" and teardown_:
        return BaseOperator(task_id=name).as_teardown()
    elif type_ == "classic":
        return BaseOperator(task_id=name)
    elif setup_:

        @setup
        def setuptask():
            pass

        return setuptask.override(task_id=name)()
    elif teardown_:

        @teardown
        def teardowntask():
            pass

        return teardowntask.override(task_id=name)()
    else:

        @task
        def my_task():
            pass

        return my_task.override(task_id=name)()


@pytest.mark.parametrize(
    "setup_type, work_type, teardown_type", itertools.product(["classic", "taskflow"], repeat=3)
)
def test_as_teardown(dag_maker, setup_type, work_type, teardown_type):
    """
    Check that as_teardown works properly as implemented in PlainXComArg

    It should mark the teardown as teardown, and if a task is provided, it should mark that as setup
    and set it as a direct upstream.
    """
    with dag_maker() as dag:
        s1 = make_task(name="s1", type_=setup_type)
        w1 = make_task(name="w1", type_=work_type)
        t1 = make_task(name="t1", type_=teardown_type)
    # initial conditions
    assert cleared_tasks(dag, "w1") == {"w1"}

    # after setting deps, still none are setup / teardown
    # verify relationships
    s1 >> w1 >> t1
    assert cleared_tasks(dag, "w1") == {"w1", "t1"}
    assert get_task_attr(t1, "is_teardown") is False
    assert get_task_attr(s1, "is_setup") is False
    assert get_task_attr(t1, "upstream_task_ids") == {"w1"}

    # now when we use as_teardown, s1 should be setup, t1 should be teardown, and we should have s1 >> t1
    t1.as_teardown(setups=s1)
    assert cleared_tasks(dag, "w1") == {"s1", "w1", "t1"}
    assert get_task_attr(t1, "is_teardown") is True
    assert get_task_attr(s1, "is_setup") is True
    assert get_task_attr(t1, "upstream_task_ids") == {"w1", "s1"}


@pytest.mark.parametrize(
    "setup_type, work_type, teardown_type", itertools.product(["classic", "taskflow"], repeat=3)
)
def test_as_teardown_oneline(dag_maker, setup_type, work_type, teardown_type):
    """
    Check that as_teardown implementations work properly. Tests all combinations of taskflow and classic.

    It should mark the teardown as teardown, and if a task is provided, it should mark that as setup
    and set it as a direct upstream.
    """

    with dag_maker() as dag:
        s1 = make_task(name="s1", type_=setup_type)
        w1 = make_task(name="w1", type_=work_type)
        t1 = make_task(name="t1", type_=teardown_type)

    # verify initial conditions
    for task_ in (s1, w1, t1):
        assert get_task_attr(task_, "upstream_list") == []
        assert get_task_attr(task_, "downstream_list") == []
        assert get_task_attr(task_, "is_setup") is False
        assert get_task_attr(task_, "is_teardown") is False
        assert cleared_tasks(dag, get_task_attr(task_, "task_id")) == {get_task_attr(task_, "task_id")}

    # now set the deps in one line
    s1 >> w1 >> t1.as_teardown(setups=s1)

    # verify resulting configuration
    # should be equiv to the following:
    #   * s1.is_setup = True
    #   * t1.is_teardown = True
    #   * s1 >> t1
    #   * s1 >> w1 >> t1
    for task_, exp_up, exp_down in [
        (s1, set(), {"w1", "t1"}),
        (w1, {"s1"}, {"t1"}),
        (t1, {"s1", "w1"}, set()),
    ]:
        assert get_task_attr(task_, "upstream_task_ids") == exp_up
        assert get_task_attr(task_, "downstream_task_ids") == exp_down
    assert cleared_tasks(dag, "s1") == {"s1", "w1", "t1"}
    assert cleared_tasks(dag, "w1") == {"s1", "w1", "t1"}
    assert cleared_tasks(dag, "t1") == {"t1"}
    for task_, exp_is_setup, exp_is_teardown in [
        (s1, True, False),
        (w1, False, False),
        (t1, False, True),
    ]:
        assert get_task_attr(task_, "is_setup") is exp_is_setup
        assert get_task_attr(task_, "is_teardown") is exp_is_teardown


@pytest.mark.parametrize("type_", ["classic", "taskflow"])
def test_cannot_be_both_setup_and_teardown(dag_maker, type_):
    # can't change a setup task to a teardown task or vice versa
    for first, second in [("setup", "teardown"), ("teardown", "setup")]:
        with dag_maker():
            s1 = make_task(name="s1", type_=type_)
            getattr(s1, f"as_{first}")()
            with pytest.raises(
                ValueError, match=f"Cannot mark task 's1' as {second}; task is already a {first}."
            ):
                getattr(s1, f"as_{second}")()
                s1.as_teardown()


def test_cannot_set_on_failure_fail_dagrun_unless_teardown_classic(dag_maker):
    with dag_maker():
        t = make_task(name="t", type_="classic")
        assert t.is_teardown is False
        with pytest.raises(
            ValueError,
            match="Cannot set task on_failure_fail_dagrun for 't' because it is not a teardown task",
        ):
            t.on_failure_fail_dagrun = True


def test_cannot_set_on_failure_fail_dagrun_unless_teardown_taskflow(dag_maker):
    @task(on_failure_fail_dagrun=True)
    def my_bad_task():
        pass

    @task
    def my_ok_task():
        pass

    with dag_maker():
        with pytest.raises(
            ValueError,
            match="Cannot set task on_failure_fail_dagrun for "
            "'my_bad_task' because it is not a teardown task",
        ):
            my_bad_task()
        # no issue
        m = my_ok_task()
        assert m.operator.is_teardown is False
        # also fine
        m = my_ok_task().as_teardown()
        assert m.operator.is_teardown is True
        assert m.operator.on_failure_fail_dagrun is False
        # and also fine
        m = my_ok_task().as_teardown(on_failure_fail_dagrun=True)
        assert m.operator.is_teardown is True
        assert m.operator.on_failure_fail_dagrun is True
        # but we can't unset
        with pytest.raises(
            ValueError, match="Cannot mark task 'my_ok_task__2' as setup; task is already a teardown."
        ):
            m.as_setup()
        with pytest.raises(
            ValueError, match="Cannot mark task 'my_ok_task__2' as setup; task is already a teardown."
        ):
            m.operator.is_setup = True
