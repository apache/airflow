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
from datetime import datetime, timedelta

import pytest

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from tests.test_utils.config import conf_vars

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": datetime.today(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

VALUE = 42


def assert_is_value(num: int):
    if num != VALUE:
        raise Exception("The test has failed")


def build_python_op():
    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_xcom_dag", default_args=DEFAULT_ARGS):
        operator = PythonOperator(
            python_callable=f,
            task_id="test_xcom_op",
            do_xcom_push=True,
        )
        return operator


class TestXComArgBuild:
    def test_xcom_ctor(self):
        python_op = build_python_op()
        actual = XComArg(python_op, "test_key")
        assert actual
        assert actual.operator == python_op
        assert actual.key == "test_key"
        # Asserting the overridden __eq__ method
        assert actual == XComArg(python_op, "test_key")
        expected_str = (
            "{{ task_instance.xcom_pull(task_ids='test_xcom_op', "
            "dag_id='test_xcom_dag', key='test_key') }}"
        )
        assert str(actual) == expected_str
        assert (
            f"echo {actual}" == "echo {{ task_instance.xcom_pull(task_ids='test_xcom_op', "
            "dag_id='test_xcom_dag', key='test_key') }}"
        )

    def test_xcom_key_is_empty_str(self):
        python_op = build_python_op()
        actual = XComArg(python_op, key="")
        assert actual.key == ""
        assert (
            str(actual) == "{{ task_instance.xcom_pull(task_ids='test_xcom_op', "
            "dag_id='test_xcom_dag', key='') }}"
        )

    def test_set_downstream(self):
        with DAG("test_set_downstream", default_args=DEFAULT_ARGS):
            op_a = BashOperator(task_id="a", bash_command="echo a")
            op_b = BashOperator(task_id="b", bash_command="echo b")
            bash_op1 = BashOperator(task_id="c", bash_command="echo c")
            bash_op2 = BashOperator(task_id="d", bash_command="echo c")
            xcom_args_a = XComArg(op_a)
            xcom_args_b = XComArg(op_b)

            bash_op1 >> xcom_args_a >> xcom_args_b >> bash_op2

        assert op_a in bash_op1.downstream_list
        assert op_b in op_a.downstream_list
        assert bash_op2 in op_b.downstream_list

    def test_set_upstream(self):
        with DAG("test_set_upstream", default_args=DEFAULT_ARGS):
            op_a = BashOperator(task_id="a", bash_command="echo a")
            op_b = BashOperator(task_id="b", bash_command="echo b")
            bash_op1 = BashOperator(task_id="c", bash_command="echo c")
            bash_op2 = BashOperator(task_id="d", bash_command="echo c")
            xcom_args_a = XComArg(op_a)
            xcom_args_b = XComArg(op_b)

            bash_op1 << xcom_args_a << xcom_args_b << bash_op2

        assert op_a in bash_op1.upstream_list
        assert op_b in op_a.upstream_list
        assert bash_op2 in op_b.upstream_list

    def test_xcom_arg_property_of_base_operator(self):
        with DAG("test_xcom_arg_property_of_base_operator", default_args=DEFAULT_ARGS):
            op_a = BashOperator(task_id="a", bash_command="echo a")

        assert op_a.output == XComArg(op_a)

    def test_xcom_key_getitem(self):
        python_op = build_python_op()
        actual = XComArg(python_op, key="another_key")
        assert actual.key == "another_key"
        actual_new_key = actual["another_key_2"]
        assert actual_new_key.key == "another_key_2"


@pytest.mark.system("core")
class TestXComArgRuntime:
    @conf_vars({("core", "executor"): "DebugExecutor"})
    def test_xcom_pass_to_op(self):
        with DAG(dag_id="test_xcom_pass_to_op", default_args=DEFAULT_ARGS) as dag:
            operator = PythonOperator(
                python_callable=lambda: VALUE,
                task_id="return_value_1",
                do_xcom_push=True,
            )
            xarg = XComArg(operator)
            operator2 = PythonOperator(
                python_callable=assert_is_value,
                op_args=[xarg],
                task_id="assert_is_value_1",
            )
            operator >> operator2
        dag.run()

    @conf_vars({("core", "executor"): "DebugExecutor"})
    def test_xcom_push_and_pass(self):
        def push_xcom_value(key, value, **context):
            ti = context["task_instance"]
            ti.xcom_push(key, value)

        with DAG(dag_id="test_xcom_push_and_pass", default_args=DEFAULT_ARGS) as dag:
            op1 = PythonOperator(
                python_callable=push_xcom_value,
                task_id="push_xcom_value",
                op_args=["my_key", VALUE],
            )
            xarg = XComArg(op1, key="my_key")
            op2 = PythonOperator(
                python_callable=assert_is_value,
                task_id="assert_is_value_1",
                op_args=[xarg],
            )
            op1 >> op2
        dag.run()
