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

from itertools import product

import pytest

from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator


def cleared_tasks(dag, task_id):
    dag_ = dag.partial_subset(task_id, include_downstream=True, include_upstream=False)
    return {x.task_id for x in dag_.tasks}


def get_task_attr(task_like, attr):
    try:
        return getattr(task_like, attr)
    except AttributeError:
        return getattr(task_like.operator, attr)


def make_task(name, type_):
    if type_ == "classic":
        return BaseOperator(task_id=name)
    else:

        @task
        def my_task():
            pass

        return my_task.override(task_id=name)()


@pytest.mark.parametrize("setup_type, work_type, teardown_type", product(*3 * [["classic", "taskflow"]]))
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
    t1.as_teardown(s1)
    assert cleared_tasks(dag, "w1") == {"s1", "w1", "t1"}
    assert get_task_attr(t1, "is_teardown") is True
    assert get_task_attr(s1, "is_setup") is True
    assert get_task_attr(t1, "upstream_task_ids") == {"w1", "s1"}


@pytest.mark.parametrize("setup_type, work_type, teardown_type", product(*3 * [["classic", "taskflow"]]))
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
    s1 >> w1 >> t1.as_teardown(s1)

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
