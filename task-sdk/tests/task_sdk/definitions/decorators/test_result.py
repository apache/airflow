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

import re

import pytest

from airflow.sdk import DAG, result, task


def test_result_error_if_not_task():
    with pytest.raises(
        TypeError,
        match=re.escape("@result must be used on top of a @task-decorated function"),
    ):

        @result
        def f():
            pass


def test_result_marks_returns_dag_result():
    @result
    @task
    def foo():
        pass

    t = foo()
    assert t.operator.returns_dag_result is True


def test_retain_returns_dag_result_when_other_attrs_are_overridden():
    @result
    @task
    def foo():
        pass

    with DAG("test_retain_returns_dag_result_when_other_attrs_are_overridden") as dag:
        foo.override(task_id="foo2")()
    assert dag.get_task("foo2").returns_dag_result is True


def test_retain_returns_dag_result_when_task_is_expanded():
    @result
    @task
    def foo(x):
        return x

    with DAG("test_retain_returns_dag_result_when_task_is_expanded") as dag:
        foo.expand(x=[1, 2, 3])

    assert dag.get_task("foo").returns_dag_result is True
