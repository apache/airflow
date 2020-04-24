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

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": datetime(2020, 4, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

VALUE = 42


def assert_is_value(num: int):
    assert num == VALUE


class TestXComArg:
    def test_xcom_pass_to_op(self):
        with DAG(dag_id="test_xcom_pass_to_op", default_args=DEFAULT_ARGS):
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
        operator.run(ignore_ti_state=True, ignore_first_depends_on_past=True)
        operator2.run(ignore_ti_state=True, ignore_first_depends_on_past=True)

    def test_xcom_push_and_pass(self):
        def push_xcom_value(key, value, **context):
            ti = context["task_instance"]
            ti.xcom_push(key, value)

        with DAG(dag_id="test_xcom_push_and_pass", default_args=DEFAULT_ARGS):
            op1 = PythonOperator(
                python_callable=push_xcom_value,
                task_id="push_xcom_value",
                op_args=["my_key", VALUE],
            )
            xarg = XComArg(op1, keys=["my_key"])
            op2 = PythonOperator(
                python_callable=assert_is_value,
                task_id="assert_is_value_1",
                op_args=[xarg],
            )
            op1 >> op2
        op1.run(ignore_ti_state=True, ignore_first_depends_on_past=True)
        op2.run(ignore_ti_state=True, ignore_first_depends_on_past=True)

    def test_set_downstream(self):
        with DAG("test_set_downstream", default_args=DEFAULT_ARGS):
            op_a = BashOperator(task_id="a", bash_command="echo a")
            op_b = BashOperator(task_id="b", bash_command="echo b")
            bash_op = BashOperator(task_id="c", bash_command="echo c")
            xcom_args_a = XComArg(op_a)
            xcom_args_b = XComArg(op_b)

            xcom_args_a >> xcom_args_b >> bash_op

        assert len(op_a.downstream_list) == 2
        assert op_b in op_a.downstream_list
        assert bash_op in op_a.downstream_list

    def test_set_upstream(self):
        with DAG("test_set_upstream", default_args=DEFAULT_ARGS):
            op_a = BashOperator(task_id="a", bash_command="echo a")
            op_b = BashOperator(task_id="b", bash_command="echo b")
            bash_op = BashOperator(task_id="c", bash_command="echo c")
            xcom_args_a = XComArg(op_a)
            xcom_args_b = XComArg(op_b)

            xcom_args_a << xcom_args_b << bash_op

        assert len(op_a.upstream_list) == 2
        assert op_b in op_a.upstream_list
        assert bash_op in op_a.upstream_list

    def test_xcom_arg_property_of_base_operator(self):
        with DAG("test_xcom_arg_property_of_base_operator", default_args=DEFAULT_ARGS):
            op_a = BashOperator(task_id="a", bash_command="echo a")

        assert op_a.output == XComArg(op_a)
