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
import functools
import sys
import typing
from collections import namedtuple
from datetime import date

import pytest

from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.providers.common.compat.sdk import AirflowException, XComNotFound

from tests_common.test_utils.version_compat import (
    AIRFLOW_V_3_0_1,
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_1_PLUS,
    XCOM_RETURN_KEY,
)
from unit.standard.operators.test_python import BasePythonTest

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import (
        DAG,
        BaseOperator,
        TaskGroup,
        XComArg,
        setup,
        task as task_decorator,
        teardown,
    )
    from airflow.sdk.bases.decorator import DecoratedMappedOperator
    from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput
    from airflow.sdk.definitions.mappedoperator import MappedOperator
else:
    from airflow.decorators import (  # type: ignore[attr-defined,no-redef]
        setup,
        task as task_decorator,
        teardown,
    )
    from airflow.decorators.base import DecoratedMappedOperator  # type: ignore[no-redef]
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]
    from airflow.models.dag import DAG  # type: ignore[assignment,no-redef]
    from airflow.models.expandinput import DictOfListsExpandInput
    from airflow.models.mappedoperator import MappedOperator  # type: ignore[assignment,no-redef]
    from airflow.models.xcom_arg import XComArg  # type: ignore[no-redef]
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import TriggerRule, timezone
else:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

pytestmark = pytest.mark.db_test


if typing.TYPE_CHECKING:
    from airflow.models.dagrun import DagRun

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
PY38 = sys.version_info >= (3, 8)
PY311 = sys.version_info >= (3, 11)


class TestAirflowTaskDecorator(BasePythonTest):
    default_date = DEFAULT_DATE

    def test_python_operator_python_callable_is_callable(self):
        """Tests that @task will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with pytest.raises(TypeError):
            task_decorator(not_callable)

    @pytest.mark.parametrize(
        "resolve",
        [
            pytest.param(eval, id="eval"),
            pytest.param(lambda t: t, id="stringify"),
        ],
    )
    @pytest.mark.parametrize(
        "annotation",
        [
            "dict",
            "dict[str, int]",
            "typing.Dict",
            "typing.Dict[str, int]",
        ],
    )
    def test_infer_multiple_outputs_using_dict_typing(self, resolve, annotation):
        @task_decorator
        def identity_dict(x: int, y: int) -> resolve(annotation):
            return {"x": x, "y": y}

        assert identity_dict(5, 5).operator.multiple_outputs is True

        # Check invoking ``@task_decorator.__call__()`` yields the correct inference.
        @task_decorator()
        def identity_dict_with_decorator_call(x: int, y: int) -> resolve(annotation):
            return {"x": x, "y": y}

        assert identity_dict_with_decorator_call(5, 5).operator.multiple_outputs is True

    def test_infer_multiple_outputs_typed_dict(self):
        from typing import TypedDict

        class TypeDictClass(TypedDict):
            pass

        @task_decorator
        def t1() -> TypeDictClass:
            return {}

        assert t1().operator.multiple_outputs is True

    # We do not enable `from __future__ import annotations` for particular this test module,
    # that mean `str | None` annotation would raise TypeError in Python 3.9 and below
    @pytest.mark.skipif(sys.version_info < (3, 10), reason="PEP 604 is implemented in Python 3.10")
    def test_infer_multiple_outputs_pep_604_union_type(self):
        @task_decorator
        def t1() -> str | None:
            # Before PEP 604 which are implemented in Python 3.10 `str | None`
            # returns `types.UnionType` which are class and could be check in `issubclass()`.
            # However in Python 3.10+ this construction returns object `typing.Union`
            # which can not be used in `issubclass()`
            return "foo"

        assert t1().operator.multiple_outputs is False

    def test_infer_multiple_outputs_union_type(self):
        @task_decorator
        def t1() -> str | None:
            return "foo"

        assert t1().operator.multiple_outputs is False

    @pytest.mark.xfail(
        reason="TODO AIP72: All @task calls now go to __getattr__ in decorators/__init__.py and this test expects user code to throw the error. Needs to be handled better, likely by changing `fixup_decorator_warning_stack`"
    )
    def test_infer_multiple_outputs_forward_annotation(self):
        if typing.TYPE_CHECKING:

            class FakeTypeCheckingOnlyClass: ...

            class UnresolveableName: ...

        @task_decorator
        def t1(x: "FakeTypeCheckingOnlyClass", y: int) -> dict[int, int]:  # type: ignore[empty-body]
            ...

        assert t1(5, 5).operator.multiple_outputs is True

        @task_decorator
        def t2(x: "FakeTypeCheckingOnlyClass", y: int) -> "dict[int, int]":  # type: ignore[empty-body]
            ...

        assert t2(5, 5).operator.multiple_outputs is True

        @task_decorator
        def t3(  # type: ignore[empty-body]
            x: "FakeTypeCheckingOnlyClass",
            y: int,
        ) -> "UnresolveableName[int, int]": ...

        with pytest.warns(UserWarning, match="Cannot infer multiple_outputs.*t3") as recwarn:
            line = sys._getframe().f_lineno - 5 if PY38 else sys._getframe().f_lineno - 2

        if PY311:
            # extra line explaining the error location in Py311
            line = line - 1

        warn = recwarn[0]
        assert warn.filename == __file__
        assert warn.lineno == line

        assert t3(5, 5).operator.multiple_outputs is False

    def test_infer_multiple_outputs_using_other_typing(self):
        @task_decorator
        def identity_tuple(x: int, y: int) -> tuple[int, int]:
            return x, y

        assert identity_tuple(5, 5).operator.multiple_outputs is False

        @task_decorator
        def identity_int(x: int) -> int:
            return x

        assert identity_int(5).operator.multiple_outputs is False

        @task_decorator
        def identity_notyping(x: int):
            return x

        assert identity_notyping(5).operator.multiple_outputs is False

        # The following cases ensure invoking ``@task_decorator.__call__()`` yields the correct inference.
        @task_decorator()
        def identity_tuple_with_decorator_call(x: int, y: int) -> tuple[int, int]:
            return x, y

        assert identity_tuple_with_decorator_call(5, 5).operator.multiple_outputs is False

        @task_decorator()
        def identity_int_with_decorator_call(x: int) -> int:
            return x

        assert identity_int_with_decorator_call(5).operator.multiple_outputs is False

        @task_decorator()
        def identity_notyping_with_decorator_call(x: int):
            return x

        assert identity_notyping_with_decorator_call(5).operator.multiple_outputs is False

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Different test for AF 2")
    def test_manual_multiple_outputs_false_with_typings(self, run_task):
        @task_decorator(multiple_outputs=False)
        def identity2(x: int, y: int) -> tuple[int, int]:
            return x, y

        res = identity2(8, 4)
        run_task(task=res.operator)

        assert not res.operator.multiple_outputs
        assert run_task.xcom.get(key=res.key) == (8, 4)
        assert run_task.xcom.get(key="return_value_0") is None
        assert run_task.xcom.get(key="return_value_1") is None

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for AF 3")
    def test_manual_multiple_outputs_false_with_typings_af2(self):
        @task_decorator(multiple_outputs=False)
        def identity2(x: int, y: int) -> tuple[int, int]:
            return x, y

        with self.dag_non_serialized:
            res = identity2(8, 4)

        dr = self.create_dag_run()
        res.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ti = dr.get_task_instances()[0]

        assert res.operator.multiple_outputs is False
        assert ti.xcom_pull() == (8, 4)
        assert ti.xcom_pull(key="return_value_0") is None
        assert ti.xcom_pull(key="return_value_1") is None

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Different test for AF 2")
    def test_multiple_outputs_ignore_typing(self, run_task):
        @task_decorator
        def identity_tuple(x: int, y: int) -> tuple[int, int]:
            return x, y

        ident = identity_tuple(35, 36)
        run_task(task=ident.operator)

        assert not ident.operator.multiple_outputs
        assert run_task.xcom.get(key=ident.key) == (35, 36)
        assert run_task.xcom.get(key="return_value_0") is None
        assert run_task.xcom.get(key="return_value_1") is None

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for AF 3")
    def test_multiple_outputs_ignore_typing_af2(self):
        @task_decorator
        def identity_tuple(x: int, y: int) -> tuple[int, int]:
            return x, y

        with self.dag_non_serialized:
            ident = identity_tuple(35, 36)

        dr = self.create_dag_run()
        ident.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ti = dr.get_task_instances()[0]

        assert not ident.operator.multiple_outputs
        assert ti.xcom_pull() == (35, 36)
        assert ti.xcom_pull(key="return_value_0") is None
        assert ti.xcom_pull(key="return_value_1") is None

    def test_fails_bad_signature(self):
        """Tests that @task will fail if signature is not binding."""

        @task_decorator
        def add_number(num: int) -> int:
            return num + 2

        with pytest.raises(TypeError):
            add_number(2, 3)
        with pytest.raises(TypeError):
            add_number()
        add_number("test")

    def test_fails_context_parameter_other_than_none(self):
        """Fail if a context parameter has a default and it's not None."""
        error_message = "Context key parameter try_number can't have a default other than None"

        @task_decorator
        def add_number_to_try_number(num: int, try_number: int = 0):
            return num + try_number

        with pytest.raises(ValueError, match=error_message):
            add_number_to_try_number(1)

    def test_fail_method(self):
        """Tests that @task will fail if signature is not binding."""

        with pytest.raises(TypeError):

            class Test:
                num = 2

                @task_decorator
                def add_number(self, num: int) -> int:
                    return self.num + num

    def test_fail_multiple_outputs_key_type(self, dag_maker):
        @task_decorator(multiple_outputs=True)
        def add_number(num: int):
            return {2: num}

        with dag_maker():
            add_number(2)

        dr = dag_maker.create_dagrun()

        error_expected = AirflowException if (not AIRFLOW_V_3_0_PLUS or AIRFLOW_V_3_0_1) else TypeError
        with pytest.raises(error_expected):
            dag_maker.run_ti("add_number", dr)

    def test_fail_multiple_outputs_no_dict(self, dag_maker):
        @task_decorator(multiple_outputs=True)
        def add_number(num: int):
            return num

        with dag_maker():
            add_number(2)

        dr = dag_maker.create_dagrun()
        error_expected = AirflowException if (not AIRFLOW_V_3_0_PLUS or AIRFLOW_V_3_0_1) else TypeError
        with pytest.raises(error_expected):
            dag_maker.run_ti("add_number", dr)

    def test_multiple_outputs_empty_dict(self, dag_maker):
        @task_decorator(multiple_outputs=True)
        def empty_dict():
            return {}

        with dag_maker():
            empty_dict()

        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("empty_dict", dr)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == {}

    def test_multiple_outputs_return_none(self, dag_maker):
        @task_decorator(multiple_outputs=True)
        def test_func():
            return

        with dag_maker():
            test_func()

        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("test_func", dr)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() is None

    def test_python_callable_arguments_are_templatized(self):
        """Test @task op_args are templatized"""

        @task_decorator
        def arg_task(*args):
            raise RuntimeError("Should not executed")

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple("Named", ["var1", "var2"])
        named_tuple = Named("{{ ds }}", "unchanged")

        with self.dag_non_serialized:
            ret = arg_task(4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple)

        dr = self.create_dag_run()
        if AIRFLOW_V_3_0_PLUS:
            ti = TaskInstance(task=ret.operator, run_id=dr.run_id, dag_version_id=dr.created_dag_version_id)
        else:
            ti = TaskInstance(task=ret.operator, run_id=dr.run_id)
        rendered_op_args = ti.render_templates().op_args
        assert len(rendered_op_args) == 4
        assert rendered_op_args[0] == 4
        assert rendered_op_args[1] == date(2019, 1, 1)
        assert rendered_op_args[2] == f"dag {self.dag_id} ran on {self.ds_templated}."
        assert rendered_op_args[3] == Named(self.ds_templated, "unchanged")

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""

        @task_decorator
        def kwargs_task(an_int, a_date, a_templated_string):
            raise RuntimeError("Should not executed")

        with self.dag_non_serialized:
            ret = kwargs_task(
                an_int=4, a_date=date(2019, 1, 1), a_templated_string="dag {{dag.dag_id}} ran on {{ds}}."
            )

        dr = self.create_dag_run()
        if AIRFLOW_V_3_0_PLUS:
            ti = TaskInstance(task=ret.operator, run_id=dr.run_id, dag_version_id=dr.created_dag_version_id)
        else:
            ti = TaskInstance(task=ret.operator, run_id=dr.run_id)
        rendered_op_kwargs = ti.render_templates().op_kwargs
        assert rendered_op_kwargs["an_int"] == 4
        assert rendered_op_kwargs["a_date"] == date(2019, 1, 1)
        assert rendered_op_kwargs["a_templated_string"] == f"dag {self.dag_id} ran on {self.ds_templated}."

    def test_manual_task_id(self):
        """Test manually setting task_id"""

        @task_decorator(task_id="some_name")
        def do_run():
            return 4

        with self.dag_non_serialized:
            do_run()
            assert self.dag_non_serialized.task_ids == ["some_name"]

    def test_multiple_calls(self):
        """Test calling task multiple times in a DAG"""

        @task_decorator
        def do_run():
            return 4

        with self.dag_non_serialized:
            do_run()
            assert self.dag_non_serialized.task_ids == ["do_run"]
            do_run_1 = do_run()
            do_run_2 = do_run()
            assert self.dag_non_serialized.task_ids == ["do_run", "do_run__1", "do_run__2"]

        assert do_run_1.operator.task_id == "do_run__1"
        assert do_run_2.operator.task_id == "do_run__2"

    def test_multiple_calls_in_task_group(self):
        """Test calling task multiple times in a TaskGroup"""

        @task_decorator
        def do_run():
            return 4

        group_id = "KnightsOfNii"
        with self.dag_non_serialized:
            with TaskGroup(group_id=group_id):
                do_run()
                assert [f"{group_id}.do_run"] == self.dag_non_serialized.task_ids
                do_run()
                assert [f"{group_id}.do_run", f"{group_id}.do_run__1"] == self.dag_non_serialized.task_ids

        assert len(self.dag_non_serialized.task_ids) == 2

    def test_call_20(self):
        """Test calling decorated function 21 times in a DAG"""

        @task_decorator
        def __do_run():
            return 4

        with self.dag_non_serialized:
            __do_run()
            for _ in range(20):
                __do_run()

        assert self.dag_non_serialized.task_ids[-1] == "__do_run__20"

    def test_multiple_outputs(self, dag_maker):
        """Tests pushing multiple outputs as a dictionary"""

        @task_decorator(multiple_outputs=True)
        def return_dict(number: int):
            return {"number": number + 1, "43": 43}

        test_number = 10
        with dag_maker():
            return_dict(test_number)

        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("return_dict", dr)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key="number") == test_number + 1
        assert ti.xcom_pull(key="43") == 43
        assert ti.xcom_pull() == {"number": test_number + 1, "43": 43}

    def test_default_args(self):
        """Test that default_args are captured when calling the function correctly"""

        @task_decorator
        def do_run():
            return 4

        self.dag_non_serialized.default_args["owner"] = "airflow"
        with self.dag_non_serialized:
            ret = do_run()
        assert ret.operator.owner == "airflow"

        @task_decorator
        def test_apply_default_raise(unknown):
            return unknown

        with pytest.raises(TypeError):
            with self.dag_non_serialized:
                test_apply_default_raise()

        @task_decorator
        def test_apply_default(owner):
            return owner

        with self.dag_non_serialized:
            ret = test_apply_default()
        assert "owner" in ret.operator.op_kwargs

    def test_xcom_arg(self, dag_maker):
        """Tests that returned key in XComArg is returned correctly"""

        @task_decorator
        def add_2(number: int):
            return number + 2

        @task_decorator
        def add_num(number: int, num2: int = 2):
            return number + num2

        test_number = 10

        with dag_maker():
            bigger_number = add_2(test_number)
            ret = add_num(bigger_number, XComArg(bigger_number.operator))

        dr = dag_maker.create_dagrun()

        dag_maker.run_ti("add_2", dr)
        dag_maker.run_ti("add_num", dr)

        ti_add_num = next(ti for ti in dr.get_task_instances() if ti.task_id == "add_num")
        assert ti_add_num.xcom_pull(key=ret.key) == (test_number + 2) * 2

    def test_dag_task(self):
        """Tests dag.task property to generate task"""

        @self.dag_non_serialized.task
        def add_2(number: int):
            return number + 2

        test_number = 10
        res = add_2(test_number)
        add_2(res)

        assert "add_2" in self.dag_non_serialized.task_ids

    def test_dag_task_multiple_outputs(self):
        """Tests dag.task property to generate task with multiple outputs"""

        @self.dag_non_serialized.task(multiple_outputs=True)
        def add_2(number: int):
            return {"1": number + 2, "2": 42}

        test_number = 10
        add_2(test_number)
        add_2(test_number)

        assert "add_2" in self.dag_non_serialized.task_ids

    @pytest.mark.parametrize(
        argnames=("op_doc_attr", "op_doc_value", "expected_doc_md"),
        argvalues=[
            pytest.param("doc", "task docs.", None, id="set_doc"),
            pytest.param("doc_json", '{"task": "docs."}', None, id="set_doc_json"),
            pytest.param("doc_md", "task docs.", "task docs.", id="set_doc_md"),
            pytest.param("doc_rst", "task docs.", None, id="set_doc_rst"),
            pytest.param("doc_yaml", "task:\n\tdocs", None, id="set_doc_yaml"),
            pytest.param("doc_md", None, "Adds 2 to number.", id="no_doc_md_use_docstring"),
        ],
    )
    def test_task_documentation(self, op_doc_attr, op_doc_value, expected_doc_md):
        """Tests that task_decorator loads doc_md from function doc if doc_md is not explicitly provided."""
        kwargs = {}
        kwargs[op_doc_attr] = op_doc_value

        @task_decorator(**kwargs)
        def add_2(number: int):
            """Adds 2 to number."""
            return number + 2

        test_number = 10
        with self.dag_non_serialized:
            ret = add_2(test_number)

        assert ret.operator.doc_md == expected_doc_md

    def test_user_provided_task_id_in_a_loop_is_used(self):
        """Tests that when looping that user provided task_id is used"""

        @task_decorator(task_id="hello_task")
        def hello():
            """
            Print Hello world
            """
            print("Hello world")

        with self.dag_non_serialized:
            for i in range(3):
                hello.override(task_id=f"my_task_id_{i * 2}")()
            hello()  # This task would have hello_task as the task_id

        assert self.dag_non_serialized.task_ids == [
            "my_task_id_0",
            "my_task_id_2",
            "my_task_id_4",
            "hello_task",
        ]

    def test_user_provided_pool_and_priority_weight_works(self):
        """Tests that when looping that user provided pool, priority_weight etc is used"""

        @task_decorator(task_id="hello_task")
        def hello():
            """
            Print Hello world
            """
            print("Hello world")

        with self.dag_non_serialized:
            for i in range(3):
                hello.override(pool="my_pool", priority_weight=i)()

        weights = []
        for _task in self.dag_non_serialized.tasks:
            assert _task.pool == "my_pool"
            weights.append(_task.priority_weight)
        assert weights == [0, 1, 2]

    def test_python_callable_args_work_as_well_as_baseoperator_args(self, dag_maker):
        """Tests that when looping that user provided pool, priority_weight etc is used"""

        @task_decorator(task_id="hello_task")
        def hello(x, y):
            """
            Print Hello world
            """
            print("Hello world", x, y)
            return x, y

        with dag_maker():
            output = hello.override(task_id="mytask")(x=2, y=3)
            output2 = hello.override()(2, 3)  # nothing overridden but should work

        dr = dag_maker.create_dagrun()
        assert output.operator.op_kwargs == {"x": 2, "y": 3}
        assert output2.operator.op_args == (2, 3)
        dag_maker.run_ti("mytask", dr)
        dag_maker.run_ti("hello_task", dr)


def test_mapped_decorator_shadow_context() -> None:
    @task_decorator
    def print_info(message: str, run_id: str = "") -> None:
        print(f"{run_id}: {message}")

    with pytest.raises(ValueError, match=r"cannot call partial\(\) on task context variable 'run_id'"):
        print_info.partial(run_id="hi")

    with pytest.raises(ValueError, match=r"cannot call expand\(\) on task context variable 'run_id'"):
        print_info.expand(run_id=["hi", "there"])


def test_mapped_decorator_wrong_argument() -> None:
    @task_decorator
    def print_info(message: str, run_id: str = "") -> None:
        print(f"{run_id}: {message}")

    with pytest.raises(TypeError) as ct:
        print_info.partial(wrong_name="hi")
    assert str(ct.value) == "partial() got an unexpected keyword argument 'wrong_name'"

    with pytest.raises(TypeError) as ct:
        print_info.expand(wrong_name=["hi", "there"])
    assert str(ct.value) == "expand() got an unexpected keyword argument 'wrong_name'"

    with pytest.raises(
        ValueError, match=r"expand\(\) got an unexpected type 'str' for keyword argument 'message'"
    ):
        print_info.expand(message="hi")


def test_mapped_decorator():
    @task_decorator
    def print_info(m1: str, m2: str, run_id: str = "") -> None:
        print(f"{run_id}: {m1} {m2}")

    @task_decorator
    def print_everything(**kwargs) -> None:
        print(kwargs)

    with DAG("test_mapped_decorator", schedule=None, start_date=DEFAULT_DATE):
        t0 = print_info.expand(m1=["a", "b"], m2={"foo": "bar"})
        t1 = print_info.partial(m1="hi").expand(m2=[1, 2, 3])
        t2 = print_everything.partial(whatever="123").expand(any_key=[1, 2], works=t1)

    assert isinstance(t2, XComArg)
    assert isinstance(t2.operator, DecoratedMappedOperator)
    assert t2.operator.task_id == "print_everything"
    assert t2.operator.op_kwargs_expand_input == DictOfListsExpandInput({"any_key": [1, 2], "works": t1})

    assert t0.operator.task_id == "print_info"
    assert t1.operator.task_id == "print_info__1"


def test_mapped_decorator_invalid_args() -> None:
    @task_decorator
    def double(number: int):
        return number * 2

    literal = [1, 2, 3]

    with pytest.raises(TypeError, match="arguments 'other', 'b'"):
        double.partial(other=[1], b=["a"])
    with pytest.raises(TypeError, match="argument 'other'"):
        double.expand(number=literal, other=[1])
    with pytest.raises(ValueError, match="argument 'number'"):
        double.expand(number=1)  # type: ignore[arg-type]


def test_partial_mapped_decorator() -> None:
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk.definitions.xcom_arg import PlainXComArg
    else:
        from airflow.models.xcom_arg import PlainXComArg  # type: ignore[attr-defined, no-redef]

    @task_decorator
    def product(number: int, multiple: int):
        return number * multiple

    literal = [1, 2, 3]

    with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
        quadrupled = product.partial(multiple=3).expand(number=literal)
        doubled = product.partial(multiple=2).expand(number=literal)
        trippled = product.partial(multiple=3).expand(number=literal)

        product.partial(multiple=2)  # No operator is actually created.

    assert isinstance(doubled, PlainXComArg)
    assert isinstance(trippled, PlainXComArg)
    assert isinstance(quadrupled, PlainXComArg)

    assert dag.task_dict == {
        "product": quadrupled.operator,
        "product__1": doubled.operator,
        "product__2": trippled.operator,
    }

    assert isinstance(doubled.operator, DecoratedMappedOperator)
    assert doubled.operator.op_kwargs_expand_input == DictOfListsExpandInput({"number": literal})
    assert doubled.operator.partial_kwargs["op_kwargs"] == {"multiple": 2}

    assert isinstance(trippled.operator, DecoratedMappedOperator)  # For type-checking on partial_kwargs.
    assert trippled.operator.partial_kwargs["op_kwargs"] == {"multiple": 3}

    assert doubled.operator is not trippled.operator


def test_mapped_decorator_unmap_merge_op_kwargs(dag_maker, session):
    with dag_maker(session=session, serialized=True):

        @task_decorator
        def task1():
            return ["x"]

        @task_decorator
        def task2(arg1, arg2): ...

        task2.partial(arg1=1).expand(arg2=task1())

    run = dag_maker.create_dagrun(session=session)

    # Run task1.
    dec = run.task_instance_scheduling_decisions(session=session)
    assert [ti.task_id for ti in dec.schedulable_tis] == ["task1"]
    ti = dec.schedulable_tis[0]
    dag_maker.run_ti(task_id=ti.task_id, dag_run=run, session=session)

    # Expand task2.
    dec = run.task_instance_scheduling_decisions(session=session)
    assert [ti.task_id for ti in dec.schedulable_tis] == ["task2"]
    ti = dec.schedulable_tis[0]

    # Use the real task for unmapping to mimic actual execution path
    ti.task = dag_maker.dag.task_dict[ti.task_id]

    if AIRFLOW_V_3_0_PLUS:
        unmapped = ti.task.unmap((ti.get_template_context(session),))
    else:
        unmapped = ti.task.unmap((ti.get_template_context(session), session))
    assert set(unmapped.op_kwargs) == {"arg1", "arg2"}


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Different test for AF 2")
def test_mapped_render_template_fields(dag_maker, session):
    @task_decorator
    def fn(arg1, arg2): ...

    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        mapped = fn.partial(arg2="{{ ti.task_id }}").expand(arg1=task1.output)

    dr = dag_maker.create_dagrun()
    ti: TaskInstance = dr.get_task_instance(task1.task_id, session=session)

    ti.xcom_push(key=XCOM_RETURN_KEY, value=["{{ ds }}"], session=session)

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=1,
            keys=None,
        )
    )
    session.flush()

    mapped_ti: TaskInstance = dr.get_task_instance(mapped.operator.task_id, session=session)
    mapped_ti.map_index = 0
    assert isinstance(mapped_ti.task, MappedOperator)
    mapped.operator.render_template_fields(context=mapped_ti.get_template_context(session=session))
    assert isinstance(mapped_ti.task, BaseOperator)

    assert mapped_ti.task.op_kwargs["arg1"] == "{{ ds }}"
    assert mapped_ti.task.op_kwargs["arg2"] == "fn"


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for AF 2")
def test_mapped_render_template_fields_af2(dag_maker, session):
    from airflow.utils.task_instance_session import set_current_task_instance_session

    @task_decorator
    def fn(arg1, arg2): ...

    with set_current_task_instance_session(session=session):
        with dag_maker(session=session):
            task1 = BaseOperator(task_id="op1")
            mapped = fn.partial(arg2="{{ ti.task_id }}").expand(arg1=task1.output)

        dr = dag_maker.create_dagrun()
        ti: TaskInstance = dr.get_task_instance(task1.task_id, session=session)

        ti.xcom_push(key=XCOM_RETURN_KEY, value=["{{ ds }}"], session=session)

        session.add(
            TaskMap(
                dag_id=dr.dag_id,
                task_id=task1.task_id,
                run_id=dr.run_id,
                map_index=-1,
                length=1,
                keys=None,
            )
        )
        session.flush()

        mapped_ti: TaskInstance = dr.get_task_instance(mapped.operator.task_id, session=session)
        mapped_ti.map_index = 0
        assert isinstance(mapped_ti.task, MappedOperator)
        mapped.operator.render_template_fields(context=mapped_ti.get_template_context(session=session))
        assert isinstance(mapped_ti.task, BaseOperator)

        assert mapped_ti.task.op_kwargs["arg1"] == "{{ ds }}"
        assert mapped_ti.task.op_kwargs["arg2"] == "fn"


def test_task_decorator_has_wrapped_attr():
    """
    Test  @task original underlying function is accessible
    through the __wrapped__ attribute.
    """

    def org_test_func():
        pass

    decorated_test_func = task_decorator(org_test_func)

    assert hasattr(decorated_test_func, "__wrapped__"), (
        "decorated function does not have __wrapped__ attribute"
    )
    assert decorated_test_func.__wrapped__ is org_test_func, "__wrapped__ attr is not the original function"


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Skip on Airflow < 3.0")
def test_task_decorator_has_doc_attr():
    """
    Test @task original underlying function docstring
    through the __doc__ attribute.
    """

    def org_test_func():
        """Docstring"""

    decorated_test_func = task_decorator(org_test_func)
    assert hasattr(decorated_test_func, "__doc__"), "decorated function should have __doc__ attribute"
    assert decorated_test_func.__doc__ == org_test_func.__doc__, (
        "__doc__ attr should be the original docstring"
    )


def test_upstream_exception_produces_none_xcom(dag_maker, session):
    from airflow.providers.common.compat.sdk import AirflowSkipException

    try:
        from airflow.sdk import TriggerRule
    except ImportError:
        # Compatibility for Airflow < 3.1
        from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

    result = None

    with dag_maker(session=session, serialized=True) as dag:

        @dag.task()
        def up1() -> str:
            return "example"

        @dag.task()
        def up2() -> None:
            raise AirflowSkipException()

        @dag.task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        def down(a, b):
            nonlocal result
            result = f"{a!r} {b!r}"

        down(up1(), up2())

    dr: DagRun = dag_maker.create_dagrun()

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 2  # "up1" and "up2"
    for ti in decision.schedulable_tis:
        dag_maker.run_ti(ti.task_id, dag_run=dr, session=session)

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 1  # "down"
    dag_maker.run_ti(decision.schedulable_tis[0].task_id, dag_run=dr, session=session)
    assert result == "'example' None"


@pytest.mark.parametrize("multiple_outputs", [True, False])
def test_multiple_outputs_produces_none_xcom_when_task_is_skipped(dag_maker, session, multiple_outputs):
    from airflow.providers.common.compat.sdk import AirflowSkipException

    try:
        from airflow.sdk import TriggerRule
    except ImportError:
        # Compatibility for Airflow < 3.1
        from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

    result = None

    with dag_maker(session=session, serialized=True) as dag:

        @dag.task()
        def up1() -> str:
            return "example"

        @dag.task(multiple_outputs=multiple_outputs)
        def up2(x) -> dict | None:
            if x == 2:
                return {"x": "example"}
            raise AirflowSkipException()

        @dag.task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        def down(a, b):
            nonlocal result
            result = f"{a!r} {b!r}"

        down(up1(), up2(1)["x"])

    dr = dag_maker.create_dagrun()

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 2  # "up1" and "up2"
    for ti in decision.schedulable_tis:
        dag_maker.run_ti(ti.task_id, dag_run=dr, session=session)

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 1  # "down"
    if multiple_outputs:
        dag_maker.run_ti(decision.schedulable_tis[0].task_id, dag_run=dr, session=session)
        assert result == "'example' None"
    else:
        with pytest.raises(XComNotFound):
            dag_maker.run_ti(decision.schedulable_tis[0].task_id, dag_run=dr, session=session)


@pytest.mark.filterwarnings("error")
def test_no_warnings(reset_logging_config, caplog):
    @task_decorator
    def some_task():
        return 1

    @task_decorator
    def other(x): ...

    with DAG(dag_id="test", start_date=DEFAULT_DATE, schedule=None):
        other(some_task())
    assert caplog.messages == []


@pytest.mark.need_serialized_dag
def test_task_decorator_asset(dag_maker, session):
    result = None
    uri = "s3://bucket/name"
    asset_name = "test_asset"

    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import Asset

        asset = Asset(uri=uri, name=asset_name)
    else:
        from airflow.datasets import Dataset as Asset

        asset = Asset(uri)

    with dag_maker(session=session) as dag:

        @dag.task()
        def up1() -> Asset:
            return asset

        @dag.task()
        def up2(src: Asset) -> str:
            return src.uri

        @dag.task()
        def down(a: str):
            nonlocal result
            result = a

        src = up1()
        s = up2(src)
        down(s)

    dr: DagRun = dag_maker.create_dagrun()
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 1  # "up1"
    ti = decision.schedulable_tis[0]
    dag_maker.run_ti(ti.task_id, dag_run=dr, session=session)

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 1  # "up2"
    ti = decision.schedulable_tis[0]
    dag_maker.run_ti(ti.task_id, dag_run=dr, session=session)

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 1  # "down"
    ti = decision.schedulable_tis[0]
    dag_maker.run_ti(ti.task_id, dag_run=dr, session=session)
    assert result == uri


def test_teardown_trigger_rule_selective_application(dag_maker, session):
    with dag_maker(session=session, serialized=True) as created_dag:
        dag = created_dag

    @dag.task
    def my_work():
        return "abc"

    @setup
    @dag.task
    def my_setup():
        return "abc"

    @teardown
    @dag.task
    def my_teardown():
        return "abc"

    work_task = my_work()
    setup_task = my_setup()
    teardown_task = my_teardown()
    assert work_task.operator.trigger_rule == TriggerRule.ALL_SUCCESS
    assert setup_task.operator.trigger_rule == TriggerRule.ALL_SUCCESS
    assert teardown_task.operator.trigger_rule == TriggerRule.ALL_DONE_SETUP_SUCCESS


def test_teardown_trigger_rule_override_behavior(dag_maker, session):
    with dag_maker(session=session, serialized=True) as created_dag:
        dag = created_dag

    @dag.task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def my_work():
        return "abc"

    @setup
    @dag.task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def my_setup():
        return "abc"

    @teardown
    @dag.task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def my_teardown():
        return "abc"

    work_task = my_work()
    setup_task = my_setup()
    with pytest.raises(Exception, match="Trigger rule not configurable for teardown tasks."):
        my_teardown()
    assert work_task.operator.trigger_rule == TriggerRule.ONE_SUCCESS
    assert setup_task.operator.trigger_rule == TriggerRule.ONE_SUCCESS


async def async_fn():
    return 42


def test_python_task():
    from airflow.providers.standard.decorators.python import _PythonDecoratedOperator, python_task
    from airflow.sdk.bases.decorator import _TaskDecorator

    decorator = python_task(async_fn)

    assert isinstance(decorator, _TaskDecorator)
    assert decorator.function == async_fn
    assert decorator.operator_class == _PythonDecoratedOperator
    assert not decorator.multiple_outputs
    assert decorator.kwargs == {"task_id": "async_fn"}
