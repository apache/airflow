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

import datetime
from collections import defaultdict
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import patch

import pytest
from sqlalchemy import delete, select

from airflow.exceptions import AirflowSkipException
from airflow.models.dag_version import DagVersion
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseOperator, TaskGroup, setup, task, task_group, teardown
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.task.priority_strategy import PriorityWeightStrategy
from airflow.task.trigger_rule import TriggerRule
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.mapping import expand_mapped_task
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.taskinstance import run_task_instance
from unit.models import DEFAULT_DATE

pytestmark = pytest.mark.db_test

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


@patch("airflow.sdk.definitions._internal.abstractoperator.AbstractOperator.render_template")
@pytest.mark.usefixtures("testing_dag_bundle")
def test_task_mapping_with_dag_and_list_of_pandas_dataframe(mock_render_template, caplog):
    class UnrenderableClass:
        def __bool__(self):
            raise ValueError("Similar to Pandas DataFrames, this class raises an exception.")

    class CustomOperator(BaseOperator):
        template_fields = ("arg",)

        def __init__(self, arg, **kwargs):
            super().__init__(**kwargs)
            self.arg = arg

        def execute(self, context: Context):
            pass

    with DAG("test-dag", schedule=None, start_date=DEFAULT_DATE) as dag:
        task1 = CustomOperator(task_id="op1", arg=None)
        unrenderable_values = [UnrenderableClass(), UnrenderableClass()]
        mapped = CustomOperator.partial(task_id="task_2").expand(arg=unrenderable_values)
        task1 >> mapped
    sync_dag_to_db(dag)
    dag.test()
    assert (
        "Unable to check if the value of type 'UnrenderableClass' is False for task 'task_2', field 'arg'"
        in caplog.text
    )
    mock_render_template.assert_called()


@pytest.mark.parametrize(
    ("num_existing_tis", "expected"),
    (
        pytest.param(0, [(0, None), (1, None), (2, None)], id="only-unmapped-ti-exists"),
        pytest.param(
            3,
            [(0, "success"), (1, "success"), (2, "success")],
            id="all-tis-exist",
        ),
        pytest.param(
            5,
            [
                (0, "success"),
                (1, "success"),
                (2, "success"),
                (3, TaskInstanceState.REMOVED),
                (4, TaskInstanceState.REMOVED),
            ],
            id="tis-to-be-removed",
        ),
    ),
)
def test_expand_mapped_task_instance(dag_maker, session, num_existing_tis, expected):
    literal = [1, 2, {"a": "b"}]
    with dag_maker(session=session, serialized=True) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    mapped_deser = dag.task_dict[mapped.task_id]

    dr = dag_maker.create_dagrun()

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=len(literal),
            keys=None,
        )
    )

    if num_existing_tis:
        # Remove the map_index=-1 TI when we're creating other TIs
        session.execute(
            delete(TaskInstance).where(
                TaskInstance.dag_id == mapped.dag_id,
                TaskInstance.task_id == mapped.task_id,
                TaskInstance.run_id == dr.run_id,
            )
        )

    dag_version = DagVersion.get_latest_version(dr.dag_id)

    for index in range(num_existing_tis):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(
            mapped_deser,
            run_id=dr.run_id,
            map_index=index,
            state=TaskInstanceState.SUCCESS,
            dag_version_id=dag_version.id,
        )
        session.add(ti)
    session.flush()

    TaskMap.expand_mapped_task(mapped_deser, dr.run_id, session=session)

    indices = session.execute(
        select(TaskInstance.map_index, TaskInstance.state)
        .where(
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()

    assert indices == expected


def test_expand_mapped_task_failed_state_in_db(dag_maker, session):
    """
    This test tries to recreate a faulty state in the database and checks if we can recover from it.
    The state that happens is that there exists mapped task instances and the unmapped task instance.
    So we have instances with map_index [-1, 0, 1]. The -1 task instances should be removed in this case.
    """
    literal = [1, 2]
    with dag_maker(session=session, serialized=True) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()
    mapped_deser = dag.task_dict[mapped.task_id]

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=len(literal),
            keys=None,
        )
    )
    dag_version = DagVersion.get_latest_version(dr.dag_id)
    for index in range(2):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(
            mapped_deser,
            run_id=dr.run_id,
            map_index=index,
            state=TaskInstanceState.SUCCESS,
            dag_version_id=dag_version.id,
        )
        session.add(ti)
    session.flush()

    indices = session.execute(
        select(TaskInstance.map_index, TaskInstance.state)
        .where(
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()
    # Make sure we have the faulty state in the database
    assert indices == [(-1, None), (0, "success"), (1, "success")]

    TaskMap.expand_mapped_task(mapped_deser, dr.run_id, session=session)

    indices = session.execute(
        select(TaskInstance.map_index, TaskInstance.state)
        .where(
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()
    # The -1 index should be cleaned up
    assert indices == [(0, "success"), (1, "success")]


def test_expand_mapped_task_instance_skipped_on_zero(dag_maker, session):
    with dag_maker(session=session, serialized=True) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()

    expand_mapped_task(dag.task_dict[mapped.task_id], dr.run_id, task1.task_id, length=0, session=session)

    indices = session.execute(
        select(TaskInstance.map_index, TaskInstance.state)
        .where(
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()

    assert indices == [(-1, TaskInstanceState.SKIPPED)]


@pytest.mark.parametrize(
    ("num_existing_tis", "expected"),
    (
        pytest.param(0, [(0, None), (1, None), (2, None)], id="only-unmapped-ti-exists"),
        pytest.param(
            3,
            [(0, "success"), (1, "success"), (2, "success")],
            id="all-tis-exist",
        ),
        pytest.param(
            5,
            [
                (0, "success"),
                (1, "success"),
                (2, "success"),
                (3, TaskInstanceState.REMOVED),
                (4, TaskInstanceState.REMOVED),
            ],
            id="tis-to-be-removed",
        ),
    ),
)
def test_expand_kwargs_mapped_task_instance(dag_maker, session, num_existing_tis, expected):
    literal = [{"arg1": "a"}, {"arg1": "b"}, {"arg1": "c"}]
    with dag_maker(session=session, serialized=True) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand_kwargs(task1.output)

    dr = dag_maker.create_dagrun()

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=len(literal),
            keys=None,
        )
    )

    if num_existing_tis:
        # Remove the map_index=-1 TI when we're creating other TIs
        session.execute(
            delete(TaskInstance).where(
                TaskInstance.dag_id == mapped.dag_id,
                TaskInstance.task_id == mapped.task_id,
                TaskInstance.run_id == dr.run_id,
            )
        )
    dag_version = DagVersion.get_latest_version(dr.dag_id)

    for index in range(num_existing_tis):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(
            dag.get_task(mapped.task_id),
            run_id=dr.run_id,
            map_index=index,
            state=TaskInstanceState.SUCCESS,
            dag_version_id=dag_version.id,
        )
        session.add(ti)
    session.flush()

    TaskMap.expand_mapped_task(dag.task_dict[mapped.task_id], dr.run_id, session=session)

    indices = session.execute(
        select(TaskInstance.map_index, TaskInstance.state)
        .where(
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()

    assert indices == expected


def test_map_product_expansion(dag_maker, session):
    """Test the cross-product effect of mapping two inputs"""
    outputs = []

    with dag_maker(dag_id="product", session=session, serialized=True) as dag:

        @dag.task
        def emit_numbers():
            return [1, 2]

        @dag.task
        def emit_letters():
            return {"a": "x", "b": "y", "c": "z"}

        @dag.task
        def show(number, letter):
            outputs.append((number, letter))

        show.expand(number=emit_numbers(), letter=emit_letters())

    dr = dag_maker.create_dagrun()
    for fn in (emit_numbers, emit_letters):
        session.add(
            TaskMap(
                dag_id=dr.dag_id,
                task_id=fn.__name__,
                run_id=dr.run_id,
                map_index=-1,
                length=len(fn.function()),
                keys=None,
            )
        )

    session.flush()
    show_task = dag.get_task("show")
    mapped_tis, max_map_index = TaskMap.expand_mapped_task(show_task, dr.run_id, session=session)
    assert max_map_index + 1 == len(mapped_tis) == 6


def _create_mapped_with_name_template_classic(*, task_id, map_names, template):
    class HasMapName(BaseOperator):
        def __init__(self, *, map_name: str, **kwargs):
            super().__init__(**kwargs)
            self.map_name = map_name

        def execute(self, context):
            context["map_name"] = self.map_name

    return HasMapName.partial(task_id=task_id, map_index_template=template).expand(
        map_name=map_names,
    )


def _create_mapped_with_name_template_taskflow(*, task_id, map_names, template):
    from airflow.sdk import get_current_context

    @task(task_id=task_id, map_index_template=template)
    def task1(map_name):
        context = get_current_context()
        context["map_name"] = map_name

    return task1.expand(map_name=map_names)


def _create_named_map_index_renders_on_failure_classic(*, task_id, map_names, template):
    class HasMapName(BaseOperator):
        def __init__(self, *, map_name: str, **kwargs):
            super().__init__(**kwargs)
            self.map_name = map_name

        def execute(self, context):
            context["map_name"] = self.map_name
            raise AirflowSkipException("Imagine this task failed!")

    return HasMapName.partial(task_id=task_id, map_index_template=template).expand(
        map_name=map_names,
    )


def _create_named_map_index_renders_on_failure_taskflow(*, task_id, map_names, template):
    from airflow.sdk import get_current_context

    @task(task_id=task_id, map_index_template=template)
    def task1(map_name):
        context = get_current_context()
        context["map_name"] = map_name
        raise AirflowSkipException("Imagine this task failed!")

    return task1.expand(map_name=map_names)


@pytest.mark.parametrize(
    ("template", "expected_rendered_names"),
    [
        pytest.param(None, [None, None], id="unset"),
        pytest.param("", ["", ""], id="constant"),
        pytest.param("{{ ti.task_id }}-{{ ti.map_index }}", ["task1-0", "task1-1"], id="builtin"),
        pytest.param("{{ ti.task_id }}-{{ map_name }}", ["task1-a", "task1-b"], id="custom"),
    ],
)
@pytest.mark.parametrize(
    "create_mapped_task",
    [
        pytest.param(_create_mapped_with_name_template_classic, id="classic"),
        pytest.param(_create_mapped_with_name_template_taskflow, id="taskflow"),
        pytest.param(_create_named_map_index_renders_on_failure_classic, id="classic-failure"),
        pytest.param(_create_named_map_index_renders_on_failure_taskflow, id="taskflow-failure"),
    ],
)
def test_expand_mapped_task_instance_with_named_index(
    dag_maker,
    session,
    create_mapped_task,
    template,
    expected_rendered_names,
) -> None:
    """Test that the correct number of downstream tasks are generated when mapping with an XComArg"""
    dag_id = "test_dag_12345"
    with dag_maker(
        dag_id=dag_id,
        start_date=DEFAULT_DATE,
        serialized=True,
    ):
        create_mapped_task(task_id="task1", map_names=["a", "b"], template=template)

    dr = dag_maker.create_dagrun(session=session)
    tis = dr.get_task_instances(session=session)
    for ti in tis:
        run_task_instance(ti, dag_maker.dag.get_task(ti.task_id))
    session.flush()

    indices = session.scalars(
        select(TaskInstance.rendered_map_index)
        .where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == "task1",
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()

    assert indices == expected_rendered_names


@pytest.mark.parametrize(
    "create_mapped_task",
    [
        pytest.param(_create_mapped_with_name_template_classic, id="classic"),
        pytest.param(_create_mapped_with_name_template_taskflow, id="taskflow"),
    ],
)
def test_expand_mapped_task_task_instance_mutation_hook(dag_maker, session, create_mapped_task) -> None:
    """Test that the tast_instance_mutation_hook is called."""
    expected_map_index = [0, 1, 2]

    with dag_maker(session=session, serialized=True) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()

    with mock.patch("airflow.settings.task_instance_mutation_hook") as mock_hook:
        expand_mapped_task(
            dag.task_dict[mapped.task_id],
            dr.run_id,
            task1.task_id,
            length=len(expected_map_index),
            session=session,
        )

        for index, call in enumerate(mock_hook.call_args_list):
            assert call.args[0].map_index == expected_map_index[index]


class TestMappedSetupTeardown:
    @staticmethod
    def get_states(dr):
        ti_dict = defaultdict(dict)
        for ti in dr.get_task_instances():
            if ti.map_index == -1:
                ti_dict[ti.task_id] = ti.state
            else:
                ti_dict[ti.task_id][ti.map_index] = ti.state
        return dict(ti_dict)

    def classic_operator(self, task_id, ret=None, partial=False, fail=False):
        def success_callable(ret=None):
            def inner(*args, **kwargs):
                print(args)
                print(kwargs)
                if ret:
                    return ret

            return inner

        def failure_callable():
            def inner(*args, **kwargs):
                print(args)
                print(kwargs)
                raise ValueError("fail")

            return inner

        kwargs = dict(task_id=task_id)
        if not fail:
            kwargs.update(python_callable=success_callable(ret=ret))
        else:
            kwargs.update(python_callable=failure_callable())
        if partial:
            return PythonOperator.partial(**kwargs)
        return PythonOperator(**kwargs)

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_one_to_many_work_failed(self, type_, dag_maker):
        """
        Work task failed.  Setup maps to teardown.  Should have 3 teardowns all successful even
        though the work task has failed.
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @setup
                def my_setup():
                    print("setting up multiple things")
                    return [1, 2, 3]

                @task
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    raise ValueError("fail!")

                @teardown
                def my_teardown(val):
                    print(f"teardown: {val}")

                s = my_setup()
                t = my_teardown.expand(val=s)
                with t:
                    my_work(s)
        else:

            @task
            def my_work(val):
                print(f"work: {val}")
                raise ValueError("i fail")

            with dag_maker() as dag:
                my_setup = self.classic_operator("my_setup", [[1], [2], [3]])
                my_teardown = self.classic_operator("my_teardown", partial=True)
                t = my_teardown.expand(op_args=my_setup.output)
                with t.as_teardown(setups=my_setup):
                    my_work(my_setup.output)

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_work": "failed",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_many_one_explicit_odd_setup_mapped_setups_fail(self, type_, dag_maker):
        """
        one unmapped setup goes to two different teardowns
        one mapped setup goes to same teardown
        mapped setups fail
        teardowns should still run
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @task
                def other_setup():
                    print("other setup")
                    return "other setup"

                @task
                def other_work():
                    print("other work")
                    return "other work"

                @task
                def other_teardown():
                    print("other teardown")
                    return "other teardown"

                @task
                def my_setup(val):
                    print(f"setup: {val}")
                    raise ValueError("fail")
                    return val

                @task
                def my_work(val):
                    print(f"work: {val}")

                @task
                def my_teardown(val):
                    print(f"teardown: {val}")

                s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
                o_setup = other_setup()
                o_teardown = other_teardown()
                with o_teardown.as_teardown(setups=o_setup):
                    other_work()
                t = my_teardown(s).as_teardown(setups=s)
                with t:
                    my_work(s)
                o_setup >> t
        else:
            with dag_maker() as dag:

                @task
                def other_work():
                    print("other work")
                    return "other work"

                @task
                def my_work(val):
                    print(f"work: {val}")

                my_teardown = self.classic_operator("my_teardown")

                my_setup = self.classic_operator("my_setup", partial=True, fail=True)
                s = my_setup.expand(op_args=[["data1.json"], ["data2.json"], ["data3.json"]])
                o_setup = self.classic_operator("other_setup")
                o_teardown = self.classic_operator("other_teardown")
                with o_teardown.as_teardown(setups=o_setup):
                    other_work()
                t = my_teardown.as_teardown(setups=s)
                with t:
                    my_work(s.output)
                o_setup >> t

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": {0: "failed", 1: "failed", 2: "failed"},
            "other_setup": "success",
            "other_teardown": "success",
            "other_work": "success",
            "my_teardown": "success",
            "my_work": "upstream_failed",
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_many_one_explicit_odd_setup_all_setups_fail(self, type_, dag_maker):
        """
        one unmapped setup goes to two different teardowns
        one mapped setup goes to same teardown
        all setups fail
        teardowns should not run
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @task
                def other_setup():
                    print("other setup")
                    raise ValueError("fail")
                    return "other setup"

                @task
                def other_work():
                    print("other work")
                    return "other work"

                @task
                def other_teardown():
                    print("other teardown")
                    return "other teardown"

                @task
                def my_setup(val):
                    print(f"setup: {val}")
                    raise ValueError("fail")
                    return val

                @task
                def my_work(val):
                    print(f"work: {val}")

                @task
                def my_teardown(val):
                    print(f"teardown: {val}")

                s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
                o_setup = other_setup()
                o_teardown = other_teardown()
                with o_teardown.as_teardown(setups=o_setup):
                    other_work()
                t = my_teardown(s).as_teardown(setups=s)
                with t:
                    my_work(s)
                o_setup >> t
        else:
            with dag_maker() as dag:

                @task
                def other_setup():
                    print("other setup")
                    raise ValueError("fail")
                    return "other setup"

                @task
                def other_work():
                    print("other work")
                    return "other work"

                @task
                def other_teardown():
                    print("other teardown")
                    return "other teardown"

                @task
                def my_work(val):
                    print(f"work: {val}")

                my_setup = self.classic_operator("my_setup", partial=True, fail=True)
                s = my_setup.expand(op_args=[["data1.json"], ["data2.json"], ["data3.json"]])
                o_setup = other_setup()
                o_teardown = other_teardown()
                with o_teardown.as_teardown(setups=o_setup):
                    other_work()
                my_teardown = self.classic_operator("my_teardown")
                t = my_teardown.as_teardown(setups=s)
                with t:
                    my_work(s.output)
                o_setup >> t

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_teardown": "upstream_failed",
            "other_setup": "failed",
            "other_work": "upstream_failed",
            "other_teardown": "upstream_failed",
            "my_setup": {0: "failed", 1: "failed", 2: "failed"},
            "my_work": "upstream_failed",
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_many_one_explicit_odd_setup_one_mapped_fails(self, type_, dag_maker):
        """
        one unmapped setup goes to two different teardowns
        one mapped setup goes to same teardown
        one of the mapped setup instances fails
        teardowns should all run
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @task
                def other_setup():
                    print("other setup")
                    return "other setup"

                @task
                def other_work():
                    print("other work")
                    return "other work"

                @task
                def other_teardown():
                    print("other teardown")
                    return "other teardown"

                @task
                def my_setup(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"setup: {val}")
                    return val

                @task
                def my_work(val):
                    print(f"work: {val}")

                @task
                def my_teardown(val):
                    print(f"teardown: {val}")

                s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
                o_setup = other_setup()
                o_teardown = other_teardown()
                with o_teardown.as_teardown(setups=o_setup):
                    other_work()
                t = my_teardown(s).as_teardown(setups=s)
                with t:
                    my_work(s)
                o_setup >> t
        else:
            with dag_maker() as dag:

                @task
                def other_setup():
                    print("other setup")
                    return "other setup"

                @task
                def other_work():
                    print("other work")
                    return "other work"

                @task
                def other_teardown():
                    print("other teardown")
                    return "other teardown"

                def my_setup_callable(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"setup: {val}")
                    return val

                my_setup = PythonOperator.partial(task_id="my_setup", python_callable=my_setup_callable)

                @task
                def my_work(val):
                    print(f"work: {val}")

                def my_teardown_callable(val):
                    print(f"teardown: {val}")

                s = my_setup.expand(op_args=[["data1.json"], ["data2.json"], ["data3.json"]])
                o_setup = other_setup()
                o_teardown = other_teardown()
                with o_teardown.as_teardown(setups=o_setup):
                    other_work()
                my_teardown = PythonOperator(
                    task_id="my_teardown", op_args=[s.output], python_callable=my_teardown_callable
                )
                t = my_teardown.as_teardown(setups=s)
                with t:
                    my_work(s.output)
                o_setup >> t

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": {0: "success", 1: "failed", 2: "skipped"},
            "other_setup": "success",
            "other_teardown": "success",
            "other_work": "success",
            "my_teardown": "success",
            "my_work": "upstream_failed",
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_one_to_many_as_teardown(self, type_, dag_maker):
        """
        1 setup mapping to 3 teardowns
        1 work task
        work fails
        teardowns succeed
        dagrun should be failure
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @task
                def my_setup():
                    print("setting up multiple things")
                    return [1, 2, 3]

                @task
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    raise ValueError("this fails")
                    return val

                @task
                def my_teardown(val):
                    print(f"teardown: {val}")

                s = my_setup()
                t = my_teardown.expand(val=s).as_teardown(setups=s)
                with t:
                    my_work(s)
        else:
            with dag_maker() as dag:

                @task
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    raise ValueError("this fails")
                    return val

                my_teardown = self.classic_operator(task_id="my_teardown", partial=True)

                s = self.classic_operator(task_id="my_setup", ret=[[1], [2], [3]])
                t = my_teardown.expand(op_args=s.output).as_teardown(setups=s)
                with t:
                    my_work(s)
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
            "my_work": "failed",
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_one_to_many_as_teardown_on_failure_fail_dagrun(self, type_, dag_maker):
        """
        1 setup mapping to 3 teardowns
        1 work task
        work succeeds
        all but one teardown succeed
        on_failure_fail_dagrun=True
        dagrun should be success
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @task
                def my_setup():
                    print("setting up multiple things")
                    return [1, 2, 3]

                @task
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    return val

                @task
                def my_teardown(val):
                    print(f"teardown: {val}")
                    if val == 2:
                        raise ValueError("failure")

                s = my_setup()
                t = my_teardown.expand(val=s).as_teardown(setups=s, on_failure_fail_dagrun=True)
                with t:
                    my_work(s)
                # todo: if on_failure_fail_dagrun=True, should we still regard the WORK task as a leaf?
        else:
            with dag_maker() as dag:

                @task
                def my_work(val):
                    print(f"doing work with multiple things: {val}")
                    return val

                def my_teardown_callable(val):
                    print(f"teardown: {val}")
                    if val == 2:
                        raise ValueError("failure")

                s = self.classic_operator(task_id="my_setup", ret=[[1], [2], [3]])
                my_teardown = PythonOperator.partial(
                    task_id="my_teardown", python_callable=my_teardown_callable
                ).expand(op_args=s.output)
                t = my_teardown.as_teardown(setups=s, on_failure_fail_dagrun=True)
                with t:
                    my_work(s.output)

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "failed", 2: "success"},
            "my_work": "success",
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_mapped_task_group_simple(self, type_, dag_maker, session):
        """
        Mapped task group wherein there's a simple s >> w >> t pipeline.
        When s is skipped, all should be skipped
        When s is failed, all should be upstream failed
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @setup
                def my_setup(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"setup: {val}")

                @task
                def my_work(val):
                    print(f"work: {val}")

                @teardown
                def my_teardown(val):
                    print(f"teardown: {val}")

                @task_group
                def file_transforms(filename):
                    s = my_setup(filename)
                    t = my_teardown(filename)
                    s >> t
                    with t:
                        my_work(filename)

                file_transforms.expand(filename=["data1.json", "data2.json", "data3.json"])
        else:
            with dag_maker() as dag:

                def my_setup_callable(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"setup: {val}")

                @task
                def my_work(val):
                    print(f"work: {val}")

                def my_teardown_callable(val):
                    print(f"teardown: {val}")

                @task_group
                def file_transforms(filename):
                    s = PythonOperator(
                        task_id="my_setup", python_callable=my_setup_callable, op_args=filename
                    )
                    t = PythonOperator(
                        task_id="my_teardown", python_callable=my_teardown_callable, op_args=filename
                    )
                    with t.as_teardown(setups=s):
                        my_work(filename)

                file_transforms.expand(filename=[["data1.json"], ["data2.json"], ["data3.json"]])
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "file_transforms.my_setup": {0: "success", 1: "failed", 2: "skipped"},
            "file_transforms.my_work": {0: "success", 1: "upstream_failed", 2: "skipped"},
            "file_transforms.my_teardown": {0: "success", 1: "upstream_failed", 2: "skipped"},
        }

        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_mapped_task_group_work_fail_or_skip(self, type_, dag_maker):
        """
        Mapped task group wherein there's a simple s >> w >> t pipeline.
        When w is skipped, teardown should still run
        When w is failed, teardown should still run
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @setup
                def my_setup(val):
                    print(f"setup: {val}")

                @task
                def my_work(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"work: {val}")

                @teardown
                def my_teardown(val):
                    print(f"teardown: {val}")

                @task_group
                def file_transforms(filename):
                    s = my_setup(filename)
                    t = my_teardown(filename).as_teardown(setups=s)
                    with t:
                        my_work(filename)

                file_transforms.expand(filename=["data1.json", "data2.json", "data3.json"])
        else:
            with dag_maker() as dag:

                @task
                def my_work(vals):
                    val = vals[0]
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"work: {val}")

                @teardown
                def my_teardown(val):
                    print(f"teardown: {val}")

                def null_callable(val):
                    pass

                @task_group
                def file_transforms(filename):
                    s = PythonOperator(task_id="my_setup", python_callable=null_callable, op_args=filename)
                    t = PythonOperator(task_id="my_teardown", python_callable=null_callable, op_args=filename)
                    t = t.as_teardown(setups=s)
                    with t:
                        my_work(filename)

                file_transforms.expand(filename=[["data1.json"], ["data2.json"], ["data3.json"]])
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "file_transforms.my_setup": {0: "success", 1: "success", 2: "success"},
            "file_transforms.my_teardown": {0: "success", 1: "success", 2: "success"},
            "file_transforms.my_work": {0: "success", 1: "failed", 2: "skipped"},
        }
        assert states == expected

    @pytest.mark.parametrize("type_", ["taskflow", "classic"])
    def test_teardown_many_one_explicit(self, type_, dag_maker):
        """
        -- passing
        one mapped setup going to one unmapped work
        3 diff states for setup: success / failed / skipped
        teardown still runs, and receives the xcom from the single successful setup
        """
        if type_ == "taskflow":
            with dag_maker() as dag:

                @task
                def my_setup(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"setup: {val}")
                    return val

                @task
                def my_work(val):
                    print(f"work: {val}")

                @task
                def my_teardown(val):
                    print(f"teardown: {val}")

                s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
                with my_teardown(s).as_teardown(setups=s):
                    my_work(s)
        else:
            with dag_maker() as dag:

                def my_setup_callable(val):
                    if val == "data2.json":
                        raise ValueError("fail!")
                    if val == "data3.json":
                        raise AirflowSkipException("skip!")
                    print(f"setup: {val}")
                    return val

                @task
                def my_work(val):
                    print(f"work: {val}")

                s = PythonOperator.partial(task_id="my_setup", python_callable=my_setup_callable)
                s = s.expand(op_args=[["data1.json"], ["data2.json"], ["data3.json"]])
                t = self.classic_operator("my_teardown")
                with t.as_teardown(setups=s):
                    my_work(s.output)

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": {0: "success", 1: "failed", 2: "skipped"},
            "my_teardown": "success",
            "my_work": "upstream_failed",
        }
        assert states == expected

    def test_one_to_many_with_teardown_and_fail_fast(self, dag_maker):
        """
        With fail_fast enabled, the teardown for an already-completed setup
        should not be skipped.
        """
        with dag_maker(fail_fast=True) as dag:

            @task
            def my_setup():
                print("setting up multiple things")
                return [1, 2, 3]

            @task
            def my_work(val):
                print(f"doing work with multiple things: {val}")
                raise ValueError("this fails")
                return val

            @task
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup()
            t = my_teardown.expand(val=s).as_teardown(setups=s)
            with t:
                my_work(s)

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
            "my_work": "failed",
        }
        assert states == expected

    def test_one_to_many_with_teardown_and_fail_fast_more_tasks(self, dag_maker):
        """
        when fail_fast enabled, teardowns should run according to their setups.
        in this case, the second teardown skips because its setup skips.
        """
        with dag_maker(fail_fast=True) as dag:
            for num in (1, 2):
                with TaskGroup(f"tg_{num}"):

                    @task
                    def my_setup():
                        print("setting up multiple things")
                        return [1, 2, 3]

                    @task
                    def my_work(val):
                        print(f"doing work with multiple things: {val}")
                        raise ValueError("this fails")
                        return val

                    @task
                    def my_teardown(val):
                        print(f"teardown: {val}")

                    s = my_setup()
                    t = my_teardown.expand(val=s).as_teardown(setups=s)
                    with t:
                        my_work(s)
        tg1, tg2 = dag.task_group.children.values()
        tg1 >> tg2
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "tg_1.my_setup": "success",
            "tg_1.my_teardown": {0: "success", 1: "success", 2: "success"},
            "tg_1.my_work": "failed",
            "tg_2.my_setup": "skipped",
            "tg_2.my_teardown": "skipped",
            "tg_2.my_work": "skipped",
        }
        assert states == expected

    def test_one_to_many_with_teardown_and_fail_fast_more_tasks_mapped_setup(self, dag_maker):
        """
        when fail_fast enabled, teardowns should run according to their setups.
        in this case, the second teardown skips because its setup skips.
        """
        with dag_maker(fail_fast=True) as dag:
            for num in (1, 2):
                with TaskGroup(f"tg_{num}"):

                    @task
                    def my_pre_setup():
                        print("input to the setup")
                        return [1, 2, 3]

                    @task
                    def my_setup(val):
                        print("setting up multiple things")
                        return val

                    @task
                    def my_work(val):
                        print(f"doing work with multiple things: {val}")
                        raise ValueError("this fails")
                        return val

                    @task
                    def my_teardown(val):
                        print(f"teardown: {val}")

                    s = my_setup.expand(val=my_pre_setup())
                    t = my_teardown.expand(val=s).as_teardown(setups=s)
                    with t:
                        my_work(s)
        tg1, tg2 = dag.task_group.children.values()
        tg1 >> tg2

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "tg_1.my_pre_setup": "success",
            "tg_1.my_setup": {0: "success", 1: "success", 2: "success"},
            "tg_1.my_teardown": {0: "success", 1: "success", 2: "success"},
            "tg_1.my_work": "failed",
            "tg_2.my_pre_setup": "skipped",
            "tg_2.my_setup": "skipped",
            "tg_2.my_teardown": "skipped",
            "tg_2.my_work": "skipped",
        }
        assert states == expected

    def test_skip_one_mapped_task_from_task_group_with_generator(self, dag_maker):
        with dag_maker() as dag:

            @task
            def make_list():
                return [1, 2, 3]

            @task
            def double(n):
                if n == 2:
                    raise AirflowSkipException()
                return n * 2

            @task
            def last(n): ...

            @task_group
            def group(n: int) -> None:
                last(double(n))

            group.expand(n=make_list())

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "group.double": {0: "success", 1: "skipped", 2: "success"},
            "group.last": {0: "success", 1: "skipped", 2: "success"},
            "make_list": "success",
        }
        assert states == expected

    def test_skip_one_mapped_task_from_task_group(self, dag_maker):
        with dag_maker() as dag:

            @task
            def double(n):
                if n == 2:
                    raise AirflowSkipException()
                return n * 2

            @task
            def last(n): ...

            @task_group
            def group(n: int) -> None:
                last(double(n))

            group.expand(n=[1, 2, 3])

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "group.double": {0: "success", 1: "skipped", 2: "success"},
            "group.last": {0: "success", 1: "skipped", 2: "success"},
        }
        assert states == expected

    @pytest.mark.parametrize(
        (
            "email",
            "execution_timeout",
            "retry_delay",
            "max_retry_delay",
            "retry_exponential_backoff",
            "max_active_tis_per_dag",
            "max_active_tis_per_dagrun",
            "run_as_user",
            "resources",
            "has_on_execute_callback",
            "has_on_failure_callback",
            "has_on_retry_callback",
            "has_on_success_callback",
            "has_on_skipped_callback",
            "executor_config",
            "inlets",
            "outlets",
            "doc",
            "doc_md",
            "doc_json",
            "doc_yaml",
            "doc_rst",
        ),
        [
            pytest.param(
                # Default case
                "email",
                timedelta(seconds=10),
                timedelta(seconds=5),
                timedelta(seconds=60),
                2.0,
                1,
                2,
                "user",
                None,
                False,
                False,
                False,
                False,
                False,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                id="default",
            ),
            pytest.param(
                # With all optional values and callbacks set
                None,
                timedelta(seconds=20),
                timedelta(seconds=10),
                timedelta(seconds=120),
                False,
                3,
                5,
                None,
                {"CPU": 1},
                True,
                True,
                True,
                True,
                True,
                {"key": "value"},
                ["input_table"],
                ["output_table"],
                "Some docs",
                "MD docs",
                {"json": True},
                "yaml: true",
                "RST docs",
                id="with-values-and-callbacks",
            ),
        ],
    )
    def test_properties(
        self,
        email,
        execution_timeout,
        retry_delay,
        max_retry_delay,
        retry_exponential_backoff,
        max_active_tis_per_dag,
        max_active_tis_per_dagrun,
        run_as_user,
        resources,
        has_on_execute_callback,
        has_on_failure_callback,
        has_on_retry_callback,
        has_on_success_callback,
        has_on_skipped_callback,
        executor_config,
        inlets,
        outlets,
        doc,
        doc_md,
        doc_json,
        doc_yaml,
        doc_rst,
    ):
        op = PythonOperator.partial(
            task_id="mapped",
            python_callable=print,
            email=email,
            execution_timeout=execution_timeout,
            retry_delay=retry_delay,
            max_retry_delay=max_retry_delay,
            retry_exponential_backoff=retry_exponential_backoff,
            max_active_tis_per_dag=max_active_tis_per_dag,
            max_active_tis_per_dagrun=max_active_tis_per_dagrun,
            run_as_user=run_as_user,
            resources=resources,
            on_execute_callback=(lambda: None) if has_on_execute_callback else None,
            on_failure_callback=(lambda: None) if has_on_failure_callback else None,
            on_retry_callback=(lambda: None) if has_on_retry_callback else None,
            on_success_callback=(lambda: None) if has_on_success_callback else None,
            on_skipped_callback=(lambda: None) if has_on_skipped_callback else None,
            executor_config=executor_config,
            inlets=inlets,
            outlets=outlets,
            doc=doc,
            doc_md=doc_md,
            doc_json=doc_json,
            doc_yaml=doc_yaml,
            doc_rst=doc_rst,
        ).expand(op_args=["Hello", "world"])

        assert op.operator_name == PythonOperator.__name__
        assert op.roots == [op]
        assert op.leaves == [op]
        assert op.task_display_name == "mapped"
        assert op.owner == SerializedBaseOperator.owner
        assert op.trigger_rule == SerializedBaseOperator.trigger_rule
        assert not op.map_index_template
        assert not op.is_setup
        assert not op.is_teardown
        assert not op.depends_on_past
        assert op.ignore_first_depends_on_past == bool(SerializedBaseOperator.ignore_first_depends_on_past)
        assert not op.wait_for_downstream
        assert op.retries == SerializedBaseOperator.retries
        assert op.queue == SerializedBaseOperator.queue
        assert op.pool == SerializedBaseOperator.pool
        assert op.pool_slots == SerializedBaseOperator.pool_slots
        assert op.priority_weight == SerializedBaseOperator.priority_weight
        assert isinstance(op.weight_rule, PriorityWeightStrategy)
        assert op.email == email
        assert op.execution_timeout == execution_timeout
        assert op.retry_delay == retry_delay
        assert op.max_retry_delay == max_retry_delay
        assert op.retry_exponential_backoff == retry_exponential_backoff
        assert op.max_active_tis_per_dag == max_active_tis_per_dag
        assert op.max_active_tis_per_dagrun == max_active_tis_per_dagrun
        assert op.run_as_user == run_as_user
        assert op.email_on_failure
        assert op.email_on_retry
        assert (op.resources is not None) == bool(resources)
        assert op.has_on_execute_callback == has_on_execute_callback
        assert op.has_on_failure_callback == has_on_failure_callback
        assert op.has_on_retry_callback == has_on_retry_callback
        assert op.has_on_success_callback == has_on_success_callback
        assert op.has_on_skipped_callback == has_on_skipped_callback
        assert (op.executor_config is not None) == bool(executor_config)
        assert (op.inlets is not None) == bool(inlets)
        assert (op.outlets is not None) == bool(outlets)
        assert (op.doc is not None) == bool(doc)
        assert (op.doc_md is not None) == bool(doc_md)
        assert (op.doc_json is not None) == bool(doc_json)
        assert (op.doc_yaml is not None) == bool(doc_yaml)
        assert (op.doc_rst is not None) == bool(doc_rst)


def test_mapped_tasks_in_mapped_task_group_waits_for_upstreams_to_complete(dag_maker, session):
    """Test that one failed trigger rule works well in mapped task group"""
    with dag_maker() as dag:

        @dag.task
        def t1():
            return [1, 2, 3]

        @task_group("tg1")
        def tg1(a):
            @dag.task()
            def t2(a):
                return a

            @dag.task(trigger_rule=TriggerRule.ONE_FAILED)
            def t3(a):
                return a

            t2(a) >> t3(a)

        t = t1()
        tg1.expand(a=t)

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="t1")
    run_task_instance(ti, dag.get_task(ti.task_id))
    dr.task_instance_scheduling_decisions()
    ti3 = dr.get_task_instance(task_id="tg1.t3")
    assert not ti3.state


def test_mapped_operator_retry_delay_default(dag_maker):
    """
    Test that MappedOperator.retry_delay returns default value when not explicitly set.

    This test verifies the fix for a KeyError that occurred when accessing retry_delay
    on a MappedOperator without an explicit retry_delay value in partial_kwargs.
    The property should fall back to SerializedBaseOperator.retry_delay (300 seconds).
    """
    with dag_maker(dag_id="test_retry_delay", serialized=True) as dag:
        # Create a mapped operator without explicitly setting retry_delay
        MockOperator.partial(task_id="mapped_task").expand(arg2=[1, 2, 3])

    # Get the deserialized mapped task
    mapped_deser = dag.task_dict["mapped_task"]

    # Accessing retry_delay should not raise KeyError
    # and should return the default value (300 seconds)
    assert mapped_deser.retry_delay == datetime.timedelta(seconds=300)


def test_mapped_operator_retry_delay_explicit(dag_maker):
    """
    Test that MappedOperator.retry_delay returns explicit value when set.

    This test verifies that when retry_delay is explicitly set in partial(),
    the MappedOperator returns that value instead of the default.
    """
    custom_retry_delay = datetime.timedelta(seconds=600)

    with dag_maker(dag_id="test_retry_delay_explicit", serialized=True) as dag:
        # Create a mapped operator with explicit retry_delay
        MockOperator.partial(task_id="mapped_task_with_retry", retry_delay=custom_retry_delay).expand(
            arg2=[1, 2, 3]
        )

    # Get the deserialized mapped task
    mapped_deser = dag.task_dict["mapped_task_with_retry"]

    # Should return the explicitly set value
    assert mapped_deser.retry_delay == custom_retry_delay
