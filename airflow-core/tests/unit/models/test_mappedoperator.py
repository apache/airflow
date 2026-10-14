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
from uuid import UUID

import pytest
from sqlalchemy import delete, event, select

from airflow.exceptions import AirflowSkipException
from airflow.executors.base_executor import BaseExecutor
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import LegacyTaskDataOwner, TaskInstance, clear_task_instances
from airflow.models.xcom import XComModel, XComModelV1, build_xcom_read_query
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseOperator, TaskGroup, setup, task, task_group, teardown
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count
from airflow.task.trigger_rule import TriggerRule
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.mapping import (
    expand_mapped_task,
    expand_mapped_task_instances,
    push_mapped_length,
)
from tests_common.test_utils.mock_executor import MockExecutor
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.taskinstance import run_task_instance
from unit.models import DEFAULT_DATE

pytestmark = pytest.mark.db_test

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


@pytest.mark.parametrize("mapping", ["dict", "list", "group"])
def test_mapped_count_uses_retained_producer_in_callers_loop_pass(dag_maker, session, mapping):
    @task_group
    def body():
        source = PythonOperator(task_id="source", python_callable=list)
        if mapping == "dict":
            PythonOperator.partial(task_id="consumer", python_callable=list).expand(op_kwargs=source.output)
        elif mapping == "list":
            PythonOperator.partial(task_id="consumer", python_callable=list).expand_kwargs(source.output)
        else:

            @task_group
            def mapped_group(value):
                PythonOperator(task_id="consumer", python_callable=list)

            mapped_group.expand(value=source.output)

    with dag_maker(serialized=True):
        loop = create_loop(body, max_iterations=3)
    dr = dag_maker.create_dagrun()
    original = DynamicRegion.get_or_create(
        dag_id=dr.dag_id, run_id=dr.run_id, node_id=loop.group_id, session=session
    )
    session.add(original)
    session.flush()
    fork = DynamicRegion(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id=loop.group_id,
        forked_from_region_id=original.id,
        resumes_from_index=1,
    )
    session.add(fork)
    session.flush()
    source = next(ti for ti in dr.task_instances if ti.task_id == "body.source")
    consumer = next(ti for ti in dr.task_instances if ti.task_id.endswith("consumer"))
    source.region_id, source.region_index = original.id, 1
    source.state = TaskInstanceState.SUCCESS
    expansion = DynamicRegion.get_or_create(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id=consumer.task_id,
        parent_region_id=fork.id,
        parent_region_index=1,
        session=session,
    )
    session.add(expansion)
    session.flush()
    consumer.region_id, consumer.region_index = expansion.id, 0
    earlier = TaskInstance(
        dag_maker.serialized_dag.get_task(source.task_id),
        dag_version_id=source.dag_version_id,
        run_id=dr.run_id,
        region_id=original.id,
        region_index=0,
        state=TaskInstanceState.SUCCESS,
    )
    session.add(earlier)
    session.flush()
    for producer, length in [(earlier, 99), (source, 2)]:
        XComModel.set_for_attempt(
            task_instance_id=producer.id,
            key="return_value",
            value=[{}] * length,
            serialize=False,
            mapped_length=length,
            session=session,
        )
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)
    contexts = resolver.producer_contexts(consumer)

    assert (
        get_mapped_ti_count(
            dag_maker.serialized_dag.get_task(consumer.task_id),
            dr.run_id,
            producer_contexts=contexts,
            session=session,
        )
        == 2
    )
    if mapping == "group":
        consumer.region_index = 1
        consumer.task = dag_maker.serialized_dag.get_task(consumer.task_id)
        assert (
            consumer.get_relevant_upstream_map_indexes(
                consumer.task,
                2,
                producer_contexts=contexts,
                session=session,
            )
            == 1
        )


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

    push_mapped_length(dr.get_task_instance(task1.task_id, session=session), literal, session=session)

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

    expand_mapped_task_instances(mapped_deser, dr.run_id, session=session)

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
    So we have instances with map_index [-1, 0, 1]. The placeholder never ran, so it is deleted.
    """
    literal = [1, 2, 3]
    with dag_maker(session=session, serialized=True) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()
    mapped_deser = dag.task_dict[mapped.task_id]
    placeholder = next(ti for ti in dr.task_instances if ti.task_id == mapped.task_id)
    placeholder_id = placeholder.id

    push_mapped_length(dr.get_task_instance(task1.task_id, session=session), literal, session=session)
    dag_version = DagVersion.get_latest_version(dr.dag_id, session=session)
    for index in range(2):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(
            mapped_deser,
            run_id=dr.run_id,
            region_id=placeholder.region_id,
            region_index=index,
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

    expand_mapped_task_instances(mapped_deser, dr.run_id, session=session)

    indices = session.execute(
        select(TaskInstance.map_index, TaskInstance.state, TaskInstance.dag_version_id)
        .where(
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.run_id == dr.run_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()
    assert indices == [
        (0, "success", dag_version.id),
        (1, "success", dag_version.id),
        (2, None, dag_version.id),
    ]
    assert session.get(TaskInstance, placeholder_id) is None
    new_ti = dr.get_task_instance(mapped.task_id, map_index=2, session=session)
    scheduler = SchedulerJobRunner(job=Job(), executors=[MockExecutor()])
    with patch.object(BaseExecutor, "queue_workload", autospec=True) as queue_workload:
        scheduler._enqueue_task_instances_with_queued_state(
            [new_ti], executor=scheduler.executor, session=session
        )
    queue_workload.assert_called_once()


def test_stable_mapped_indexes_do_not_query_historical_max_try(dag_maker, session):
    with dag_maker(session=session, serialized=True) as dag:
        upstream = BaseOperator(task_id="upstream")
        mapped = MockOperator.partial(task_id="mapped").expand(arg2=upstream.output)
    run = dag_maker.create_dagrun()
    push_mapped_length(run.get_task_instance(upstream.task_id, session=session), [1, 2], session=session)
    expand_mapped_task_instances(dag.task_dict[mapped.task_id], run.run_id, session=session)
    statements = []

    def capture_sql(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(session.bind, "before_cursor_execute", capture_sql)
    live = run.get_task_instance(mapped.task_id, map_index=0, session=session)
    live.task = dag.task_dict[mapped.task_id]
    try:
        new_tis = run._revise_map_indexes_if_mapped(live, session=session)
    finally:
        event.remove(session.bind, "before_cursor_execute", capture_sql)

    assert new_tis == []
    assert not any("max(" in statement.lower() and "try_number" in statement for statement in statements)


def test_missing_mapped_index_uses_retained_max_try(dag_maker, session):
    with dag_maker(session=session, serialized=True) as dag:
        upstream = BaseOperator(task_id="upstream")
        mapped = MockOperator.partial(task_id="mapped").expand(arg2=upstream.output)
    run = dag_maker.create_dagrun()
    push_mapped_length(run.get_task_instance(upstream.task_id, session=session), [1, 2], session=session)
    expand_mapped_task_instances(dag.task_dict[mapped.task_id], run.run_id, session=session)
    removed = run.get_task_instance(mapped.task_id, map_index=1, session=session)
    removed.try_number = 3
    removed.archive(reason="retry", session=session)
    live = run.get_task_instance(mapped.task_id, map_index=0, session=session)
    live.task = dag.task_dict[mapped.task_id]

    new_tis = run._revise_map_indexes_if_mapped(live, session=session)

    assert len(new_tis) == 1
    assert new_tis[0].map_index == 1
    assert new_tis[0].try_number == 4
    assert removed.working_set is None


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

    push_mapped_length(dr.get_task_instance(task1.task_id, session=session), literal, session=session)

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

    expand_mapped_task_instances(dag.task_dict[mapped.task_id], dr.run_id, session=session)

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
        push_mapped_length(dr.get_task_instance(fn.__name__, session=session), fn.function(), session=session)

    session.flush()
    show_task = dag.get_task("show")
    mapped_tis, max_map_index = expand_mapped_task_instances(show_task, dr.run_id, session=session)
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
        pytest.param(None, ["0", "1"], id="unset"),
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
        select(TaskInstance.rendered_map_index)  # type: ignore[call-overload]
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
        assert op.weight_rule == "downstream"
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
    region_id = next(ti.region_id for ti in dr.task_instances if ti.task_id == "tg1.t3")
    ti = dr.get_task_instance(task_id="t1", session=session)
    run_task_instance(ti, dag.get_task(ti.task_id))
    dr.task_instance_scheduling_decisions(session=session)
    ti3 = dr.get_task_instance(task_id="tg1.t3", region_id=region_id, session=session)
    assert not ti3.state


def test_one_failed_trigger_rule_in_mapped_task_group_is_per_index(dag_maker):
    """Regression test for #50210.

    A task with the ``ONE_FAILED`` trigger rule inside a mapped task group must
    be evaluated against the upstream instance that shares its own map index,
    not against every upstream instance of the group. Otherwise a single failed
    upstream instance would wrongly trigger the rule for every expanded instance.
    """
    with dag_maker(dag_id="test_one_failed_in_mapped_task_group") as dag:

        @task
        def divide(i):
            return 30 / i

        @task(trigger_rule=TriggerRule.ONE_FAILED)
        def report_failure(i):
            pass

        @task
        def report_success(i):
            pass

        @task
        def gen_examples():
            return [0, 1, 2, 3]

        @task_group
        def divide_and_report(i):
            divide(i) >> [report_success(i), report_failure(i)]

        divide_and_report.expand(i=gen_examples())

    dr = dag.test()

    states: dict[str, dict[int, str | None]] = defaultdict(dict)
    for ti in dr.get_task_instances():
        states[ti.task_id][ti.map_index] = ti.state

    # divide(0) fails (ZeroDivisionError); the rest succeed.
    assert states["divide_and_report.divide"] == {0: "failed", 1: "success", 2: "success", 3: "success"}
    # Only report_failure sharing divide(0)'s map index should run; the rest are skipped.
    assert states["divide_and_report.report_failure"] == {
        0: "success",
        1: "skipped",
        2: "skipped",
        3: "skipped",
    }
    # report_success mirrors the opposite: it is upstream_failed only where divide failed.
    assert states["divide_and_report.report_success"] == {
        0: "upstream_failed",
        1: "success",
        2: "success",
        3: "success",
    }


def test_one_failed_trigger_rule_runs_on_indirect_failure_in_mapped_task_group(dag_maker):
    """Regression test for #34023.

    A ``ONE_FAILED`` task at the end of a chain inside a mapped task group must
    still run for every expanded instance whose (indirect) upstream failed, and
    must not be skipped prematurely before the group has expanded. This guards
    the end-to-end outcome of the fix that the per-index change in #50210 builds on.
    """
    with dag_maker(dag_id="test_one_failed_indirect_in_mapped_task_group") as dag:

        @task
        def get_records():
            return ["a", "b", "c"]

        @task
        def submit_job(record):
            pass

        @task
        def fake_sensor(record):
            raise RuntimeError("boo")

        @task
        def deliver_record(record):
            pass

        @task(trigger_rule=TriggerRule.ONE_FAILED)
        def handle_failed_delivery(record):
            pass

        @task_group(group_id="deliver_records")
        def deliver_record_task_group(record):
            (
                submit_job(record)
                >> fake_sensor(record)
                >> deliver_record(record)
                >> handle_failed_delivery(record)
            )

        deliver_record_task_group.expand(record=get_records())

    dr = dag.test()

    states: dict[str, dict[int, str | None]] = defaultdict(dict)
    for ti in dr.get_task_instances():
        states[ti.task_id][ti.map_index] = ti.state

    # fake_sensor fails for every index, so handle_failed_delivery must run everywhere.
    assert states["deliver_records.fake_sensor"] == {0: "failed", 1: "failed", 2: "failed"}
    assert states["deliver_records.handle_failed_delivery"] == {0: "success", 1: "success", 2: "success"}


@pytest.mark.parametrize("trigger_rule", [TriggerRule.ALL_DONE, TriggerRule.NONE_FAILED])
def test_short_circuit_skips_later_tasks_in_task_group_mapped_over_upstream_output(dag_maker, trigger_rule):
    """
    A short-circuit inside a task group expanded over an upstream task's output skips every
    later task of the same map index, although none of them is expanded when it runs.
    """
    with dag_maker(dag_id="test_short_circuit_in_task_group_mapped_over_output") as dag:

        @task
        def get_values():
            return [True, False]

        @task.short_circuit
        def gate(value):
            return value

        @task
        def a():
            pass

        @task(trigger_rule=trigger_rule)
        def b():
            pass

        @task(trigger_rule=trigger_rule)
        def c():
            pass

        @task_group
        def group(value):
            gate(value) >> a() >> b() >> c()

        group.expand(value=get_values())

    dr = dag.test()

    states: dict[str, dict[int, str | None]] = defaultdict(dict)
    for ti in dr.get_task_instances():
        states[ti.task_id][ti.map_index] = ti.state

    assert states["group.gate"] == {0: "success", 1: "success"}
    for task_id in ("group.a", "group.b", "group.c"):
        assert states[task_id] == {0: "success", 1: "skipped"}


def test_none_failed_min_one_success_trigger_rule_expands_in_mapped_task_group(dag_maker):
    """Regression test for #39801.

    A task with the ``NONE_FAILED_MIN_ONE_SUCCESS`` trigger rule inside a dynamically
    expanded task group must expand and run once its upstream (also inside the group)
    succeeds, instead of being skipped prematurely at its unexpanded (map_index -1)
    summary ti before the task group has expanded.
    """
    with dag_maker(dag_id="test_none_failed_min_one_success_in_mapped_task_group") as dag:

        @task
        def init():
            return ["seize", "the", "day"]

        @task_group(group_id="tg")
        def tg(message):
            @task
            def imsleepy(message):
                return message

            @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
            def imawake(message):
                pass

            imawake(imsleepy(message))

        tg.expand(message=init())

    dr = dag.test()

    states: dict[str, dict[int, str | None]] = defaultdict(dict)
    for ti in dr.get_task_instances():
        states[ti.task_id][ti.map_index] = ti.state

    assert states["tg.imsleepy"] == {0: "success", 1: "success", 2: "success"}
    assert states["tg.imawake"] == {0: "success", 1: "success", 2: "success"}


def test_none_failed_min_one_success_trigger_rule_expands_in_nested_task_group(dag_maker):
    """Regression test for #39801, nested-group shape.

    Same as above, but the ``NONE_FAILED_MIN_ONE_SUCCESS`` task lives in a plain task
    group nested inside the mapped one (the exact shape of the issue's reproducer), so
    its immediate ``task_group`` is not mapped — only an ancestor is.
    """
    with dag_maker(dag_id="test_none_failed_min_one_success_in_nested_task_group") as dag:

        @task
        def init():
            return ["seize", "the", "day"]

        @task_group(group_id="tg")
        def tg(message):
            @task
            def imsleepy(message):
                return message

            @task_group(group_id="inner")
            def inner(message):
                @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
                def imawake(message):
                    pass

                imawake(message)

            inner(imsleepy(message))

        tg.expand(message=init())

    dr = dag.test()

    states: dict[str, dict[int, str | None]] = defaultdict(dict)
    for ti in dr.get_task_instances():
        states[ti.task_id][ti.map_index] = ti.state

    assert states["tg.imsleepy"] == {0: "success", 1: "success", 2: "success"}
    assert states["tg.inner.imawake"] == {0: "success", 1: "success", 2: "success"}


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


def test_placeholder_promotion_keeps_legacy_owner_and_avoids_historical_try_collision(dag_maker, session):
    with dag_maker(session=session, serialized=True) as dag:
        upstream = BaseOperator(task_id="upstream")
        mapped = MockOperator.partial(task_id="mapped").expand(arg2=upstream.output)
    run = dag_maker.create_dagrun()
    serialized = dag.task_dict[mapped.task_id]
    placeholder = run.get_task_instance(mapped.task_id, session=session)
    placeholder_id = placeholder.id
    historical = TaskInstance(
        serialized,
        run_id=run.run_id,
        region_id=placeholder.region_id,
        region_index=0,
        state=TaskInstanceState.FAILED,
        dag_version_id=placeholder.dag_version_id,
    )
    historical.try_number = 3
    session.add(historical)
    session.flush()
    historical.archive(reason="retry", session=session)
    session.add(
        LegacyTaskDataOwner(
            dag_id=run.dag_id,
            run_id=run.run_id,
            task_id=mapped.task_id,
            map_index=-1,
            task_instance_id=placeholder_id,
        )
    )
    session.flush()
    session.add(
        XComModelV1(
            dag_run_id=run.id,
            dag_id=run.dag_id,
            run_id=run.run_id,
            task_id=mapped.task_id,
            map_index=-1,
            key="legacy",
            value={"owner": "placeholder"},
        )
    )
    push_mapped_length(run.get_task_instance(upstream.task_id, session=session), [1, 2], session=session)

    expanded, maximum = expand_mapped_task_instances(serialized, run.run_id, session=session)

    assert maximum == 1
    assert expanded[0].id == placeholder_id
    assert expanded[0].map_index == 0
    assert expanded[0].try_number == 4
    assert historical.map_index == 0
    assert historical.try_number == 3
    assert historical.working_set is None
    owner = session.scalar(
        select(LegacyTaskDataOwner).where(LegacyTaskDataOwner.task_instance_id == placeholder_id)
    )
    assert owner.map_index == -1
    read = build_xcom_read_query(
        producer_ids=select(TaskInstance.id).where(TaskInstance.id == placeholder_id)
    )
    value = session.scalars(read).one()
    assert value.map_index == 0
    assert value.value == {"owner": "placeholder"}


def _live_tis(session, dr, task_id):
    return session.scalars(
        select(TaskInstance)
        .where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.run_id == dr.run_id,
            TaskInstance.task_id == task_id,
        )
        .order_by(TaskInstance.region_index)
    ).all()


def _node_regions(session, dr, task_id):
    return session.scalars(
        select(DynamicRegion).where(
            DynamicRegion.dag_id == dr.dag_id,
            DynamicRegion.run_id == dr.run_id,
            DynamicRegion.node_id == task_id,
        )
    ).all()


def _make_legacy_placeholder(session, dr, serialized):
    """Replace the regional placeholder with one that predates regions."""
    current = _live_tis(session, dr, serialized.task_id)[0]
    version = current.dag_version_id
    session.delete(current)
    session.flush()
    session.execute(delete(DynamicRegion).where(DynamicRegion.node_id == serialized.task_id))
    placeholder = TaskInstance(serialized, dag_version_id=version, run_id=dr.run_id)
    session.add(placeholder)
    session.flush()
    return placeholder


@pytest.fixture
def mapped_dag(dag_maker, session):
    with dag_maker(session=session, serialized=True) as dag:
        upstream = BaseOperator(task_id="upstream")
        mapped = MockOperator.partial(task_id="mapped").expand(arg2=upstream.output)
    dr = dag_maker.create_dagrun()
    return dr, dag.task_dict[mapped.task_id], upstream.task_id


def test_mapped_task_region_is_born_with_its_placeholder(mapped_dag, session):
    dr, serialized, _ = mapped_dag

    (placeholder,) = _live_tis(session, dr, serialized.task_id)
    (region,) = _node_regions(session, dr, serialized.task_id)

    assert placeholder.region_id == region.id
    assert placeholder.region_index == -1
    assert placeholder.map_index == -1
    assert region.parent_region_id is None
    assert region.forked_from_region_id is None


def test_expansion_grows_and_shrinks_within_one_region(mapped_dag, session):
    dr, serialized, upstream_id = mapped_dag
    placeholder_id = _live_tis(session, dr, serialized.task_id)[0].id
    (region,) = _node_regions(session, dr, serialized.task_id)

    expand_mapped_task(serialized, dr.run_id, upstream_id, 3, session)
    first = _live_tis(session, dr, serialized.task_id)
    assert [ti.region_index for ti in first] == [0, 1, 2]
    assert {ti.region_id for ti in first} == {region.id}
    assert first[0].id == placeholder_id

    push_mapped_length(dr.get_task_instance(upstream_id, session=session), list(range(5)), session=session)
    first[0].task = serialized
    assert [ti.region_index for ti in dr._revise_map_indexes_if_mapped(first[0], session=session)] == [3, 4]

    push_mapped_length(dr.get_task_instance(upstream_id, session=session), list(range(2)), session=session)
    assert dr._revise_map_indexes_if_mapped(first[0], session=session) == []
    session.expire_all()
    states = {ti.region_index: ti.state for ti in _live_tis(session, dr, serialized.task_id)}
    assert states[0] is None
    assert states[2] == states[3] == states[4] == TaskInstanceState.REMOVED
    assert {ti.region_id for ti in _live_tis(session, dr, serialized.task_id)} == {region.id}
    assert _node_regions(session, dr, serialized.task_id) == [region]


def test_zero_length_expansion_leaves_a_region_holding_only_its_skipped_placeholder(mapped_dag, session):
    dr, serialized, upstream_id = mapped_dag
    (region,) = _node_regions(session, dr, serialized.task_id)

    expand_mapped_task(serialized, dr.run_id, upstream_id, 0, session)

    (placeholder,) = _live_tis(session, dr, serialized.task_id)
    assert (placeholder.region_id, placeholder.region_index) == (region.id, -1)
    assert placeholder.state == TaskInstanceState.SKIPPED


def test_promotion_leaves_earlier_placeholder_task_instances_at_their_coordinates(mapped_dag, session):
    dr, serialized, upstream_id = mapped_dag
    placeholder = _live_tis(session, dr, serialized.task_id)[0]
    region_id = placeholder.region_id
    placeholder.task = serialized
    placeholder.try_number = 1
    placeholder.state = TaskInstanceState.FAILED
    archived_id = placeholder.id
    (current,) = clear_task_instances([placeholder], session)
    session.flush()
    assert current.id != archived_id

    expand_mapped_task(serialized, dr.run_id, upstream_id, 2, session)

    archived = session.get(TaskInstance, archived_id)
    assert (archived.region_id, archived.region_index, archived.try_number) == (region_id, -1, 1)
    assert archived.working_set is None
    live = _live_tis(session, dr, serialized.task_id)
    assert [ti.region_index for ti in live] == [0, 1]
    assert live[0].id == current.id
    assert live[0].try_number == current.try_number


@pytest.fixture
def twin_mapped_dag(dag_maker, session):
    with dag_maker(session=session, serialized=True) as dag:
        upstream = BaseOperator(task_id="upstream")
        MockOperator.partial(task_id="fresh").expand(arg2=upstream.output)
        MockOperator.partial(task_id="legacy").expand(arg2=upstream.output)
    dr = dag_maker.create_dagrun()
    return dr, dag, upstream.task_id


def _expansion_outcome(session, dr, task_id, placeholder_id):
    live = _live_tis(session, dr, task_id)
    archived = session.scalars(
        select(TaskInstance)
        .where(
            TaskInstance.working_set.is_(None),
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.run_id == dr.run_id,
            TaskInstance.task_id == task_id,
        )
        .execution_options(include_all_attempts=True)
    ).all()
    return (
        [(ti.region_index, ti.state, ti.try_number, ti.id == placeholder_id) for ti in live],
        len({ti.region_id for ti in live}),
        [(ti.state, ti.archived_reason) for ti in archived],
    )


@pytest.mark.parametrize("length", [0, 3])
def test_legacy_placeholder_moves_into_its_new_region_without_being_archived(
    twin_mapped_dag, session, length
):
    dr, dag, upstream_id = twin_mapped_dag
    fresh = dag.task_dict["fresh"]
    serialized = dag.task_dict["legacy"]
    fresh_placeholder_id = _live_tis(session, dr, "fresh")[0].id
    legacy = _make_legacy_placeholder(session, dr, serialized)
    legacy_id = legacy.id

    push_mapped_length(
        dr.get_task_instance(upstream_id, session=session), list(range(length)), session=session
    )
    expand_mapped_task_instances(fresh, dr.run_id, session=session)
    expand_mapped_task_instances(serialized, dr.run_id, session=session)

    (region,) = _node_regions(session, dr, serialized.task_id)
    live = _live_tis(session, dr, serialized.task_id)
    assert {ti.region_id for ti in live} == {region.id}
    assert live[0].id == legacy_id
    assert live[0].region_index == (0 if length else -1)
    assert (
        session.scalar(select(TaskInstance.id).where(TaskInstance.archived_reason == "mapped_placeholder"))
        is None
    )
    assert _expansion_outcome(session, dr, "legacy", legacy_id) == _expansion_outcome(
        session, dr, "fresh", fresh_placeholder_id
    )
    assert _expansion_outcome(session, dr, "legacy", legacy_id)[2] == []


def test_legacy_placeholder_keeps_its_earlier_tries_and_is_promoted_in_place(twin_mapped_dag, session):
    dr, dag, upstream_id = twin_mapped_dag
    serialized = dag.task_dict["legacy"]
    earlier = _make_legacy_placeholder(session, dr, serialized)
    earlier.try_number = 1
    earlier.state = TaskInstanceState.UPSTREAM_FAILED
    earlier.archive(reason="retry", session=session)
    legacy = TaskInstance(serialized, dag_version_id=earlier.dag_version_id, run_id=dr.run_id)
    legacy.try_number = 2
    session.add(legacy)
    session.flush()
    legacy_id = legacy.id

    push_mapped_length(dr.get_task_instance(upstream_id, session=session), [1, 2], session=session)
    expand_mapped_task_instances(serialized, dr.run_id, session=session)

    (region,) = _node_regions(session, dr, serialized.task_id)
    live = _live_tis(session, dr, serialized.task_id)
    assert [(ti.region_id, ti.region_index) for ti in live] == [(region.id, 0), (region.id, 1)]
    assert (live[0].id, live[0].try_number) == (legacy_id, 2)
    assert (earlier.region_id, earlier.region_index, earlier.try_number) == (UUID(int=0), -1, 1)
    assert earlier.archived_reason == "retry"


def test_clearing_a_legacy_expansion_keeps_it_in_the_sentinel_region(mapped_dag, session):
    dr, serialized, _ = mapped_dag
    _make_legacy_placeholder(session, dr, serialized)
    legacy = [
        TaskInstance(
            serialized,
            dag_version_id=dr.created_dag_version_id,
            run_id=dr.run_id,
            region_index=index,
            state=TaskInstanceState.SUCCESS,
        )
        for index in range(2)
    ]
    session.add_all(legacy)
    session.flush()
    for ti in legacy:
        ti.task = serialized

    (cleared,) = clear_task_instances([legacy[0]], session)
    session.flush()

    assert cleared.region_id == UUID(int=0)
    assert (cleared.region_index, cleared.try_number) == (0, 1)
    assert cleared.state is None
    assert legacy[1].state == TaskInstanceState.SUCCESS
    assert _node_regions(session, dr, serialized.task_id) == []
    archived = session.scalars(
        select(TaskInstance)
        .where(TaskInstance.working_set.is_(None), TaskInstance.task_id == serialized.task_id)
        .execution_options(include_all_attempts=True)
    ).one()
    assert (archived.region_id, archived.region_index, archived.try_number) == (UUID(int=0), 0, 0)


def test_expansion_nested_under_a_loop_pass_is_a_child_region_that_revises_in_place(dag_maker, session):
    with dag_maker(session=session, serialized=True):
        mapped = MockOperator.partial(task_id="mapped").expand(arg2=[1, 2, 3])
    dr = dag_maker.create_dagrun()
    serialized = dag_maker.serialized_dag.get_task(mapped.task_id)
    session.execute(delete(TaskInstance).where(TaskInstance.task_id == mapped.task_id))
    session.execute(delete(DynamicRegion))
    loop_region = DynamicRegion.get_or_create(
        dag_id=dr.dag_id, run_id=dr.run_id, node_id="loop", session=session
    )
    session.add(loop_region)
    session.flush()
    created: list[TaskInstance] = []

    def creator(task, indexes, region_id):
        for index in indexes:
            ti = TaskInstance(
                task,
                dag_version_id=dr.created_dag_version_id,
                run_id=dr.run_id,
                region_id=region_id,
                region_index=index,
            )
            created.append(ti)
            yield ti

    session.add_all(
        list(dr._create_tasks([serialized], creator, session=session, parent_region=(loop_region.id, 2)))
    )
    session.flush()
    (child,) = _node_regions(session, dr, mapped.task_id)
    assert (child.parent_region_id, child.parent_region_index) == (loop_region.id, 2)
    (placeholder,) = created
    assert (placeholder.region_id, placeholder.region_index) == (child.id, -1)

    placeholder.expand_mapped_task(session=session)

    live = _live_tis(session, dr, mapped.task_id)
    assert [ti.region_index for ti in live] == [0, 1, 2]
    assert {ti.region_id for ti in live} == {child.id}
    assert _node_regions(session, dr, mapped.task_id) == [child]
