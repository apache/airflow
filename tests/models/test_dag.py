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
import itertools
import logging
import os
import pickle
import re
import weakref
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import patch

import jinja2
import pendulum
import pytest
import time_machine
from sqlalchemy import inspect, select

from airflow import settings
from airflow.assets import Asset, AssetAlias, AssetAll, AssetAny
from airflow.configuration import conf
from airflow.decorators import setup, task as task_decorator, teardown
from airflow.exceptions import (
    AirflowException,
    DuplicateTaskIdFound,
    ParamValidationError,
    UnknownExecutorException,
)
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    TaskOutletAssetReference,
)
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import (
    DAG,
    DAG_ARGS_EXPECTED_TYPES,
    DagModel,
    DagOwnerAttributes,
    DagTag,
    ExecutorLoader,
    dag as dag_decorator,
    get_asset_triggered_next_run_info,
)
from airflow.models.dagrun import DagRun
from airflow.models.param import DagParam, Param, ParamsDict
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskfail import TaskFail
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.security import permissions
from airflow.templates import NativeEnvironment, SandboxedEnvironment
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.simple import (
    AssetTriggeredTimetable,
    ContinuousTimetable,
    NullTimetable,
    OnceTimetable,
)
from airflow.utils import timezone
from airflow.utils.file import list_py_file_paths
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.task_group import TaskGroup, TaskGroupContext
from airflow.utils.timezone import datetime as datetime_tz
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType
from airflow.utils.weight_rule import WeightRule
from tests.models import DEFAULT_DATE
from tests.plugins.priority_weight_strategy import (
    FactorPriorityWeightStrategy,
    NotRegisteredPriorityWeightStrategy,
    StaticTestPriorityWeightStrategy,
    TestPriorityWeightStrategyPlugin,
)

from dev.tests_common.test_utils.asserts import assert_queries_count
from dev.tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from dev.tests_common.test_utils.config import conf_vars
from dev.tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)
from dev.tests_common.test_utils.mapping import expand_mapped_task
from dev.tests_common.test_utils.mock_plugins import mock_plugin_manager
from dev.tests_common.test_utils.timetables import cron_timetable, delta_timetable

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

TEST_DATE = datetime_tz(2015, 1, 2, 0, 0)

repo_root = Path(__file__).parents[2]


@pytest.fixture
def clear_dags():
    clear_db_dags()
    clear_db_serialized_dags()
    yield
    clear_db_dags()
    clear_db_serialized_dags()


@pytest.fixture
def clear_assets():
    clear_db_assets()
    yield
    clear_db_assets()


class TestDag:
    def setup_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_assets()
        self.patcher_dag_code = mock.patch("airflow.models.dag.DagCode.bulk_sync_to_db")
        self.patcher_dag_code.start()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_assets()
        self.patcher_dag_code.stop()

    @staticmethod
    def _clean_up(dag_id: str):
        with create_session() as session:
            session.query(DagRun).filter(DagRun.dag_id == dag_id).delete(synchronize_session=False)
            session.query(TI).filter(TI.dag_id == dag_id).delete(synchronize_session=False)
            session.query(TaskFail).filter(TaskFail.dag_id == dag_id).delete(synchronize_session=False)

    @staticmethod
    def _occur_before(a, b, list_):
        """
        Assert that a occurs before b in the list.
        """
        a_index = -1
        b_index = -1
        for i, e in enumerate(list_):
            if e.task_id == a:
                a_index = i
            if e.task_id == b:
                b_index = i
        return 0 <= a_index < b_index

    def test_params_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = DAG("test-dag", schedule=None)

        assert isinstance(dag.params, ParamsDict)
        assert 0 == len(dag.params)

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
        Test if there is a DAG with not None schedule and have some params that
        don't have a default value raise a error while DAG parsing
        """
        params = {"param1": Param(type="string")}

        with pytest.raises(AirflowException):
            DAG("dummy-dag", schedule=timedelta(days=1), start_date=DEFAULT_DATE, params=params)

    def test_dag_invalid_default_view(self):
        """
        Test invalid `default_view` of DAG initialization
        """
        with pytest.raises(AirflowException, match="Invalid values of dag.default_view: only support"):
            DAG(dag_id="test-invalid-default_view", schedule=None, default_view="airflow")

    def test_dag_default_view_default_value(self):
        """
        Test `default_view` default value of DAG initialization
        """
        dag = DAG(dag_id="test-default_default_view", schedule=None)
        assert conf.get("webserver", "dag_default_view").lower() == dag.default_view

    def test_dag_invalid_orientation(self):
        """
        Test invalid `orientation` of DAG initialization
        """
        with pytest.raises(AirflowException, match="Invalid values of dag.orientation: only support"):
            DAG(dag_id="test-invalid-orientation", schedule=None, orientation="airflow")

    def test_dag_orientation_default_value(self):
        """
        Test `orientation` default value of DAG initialization
        """
        dag = DAG(dag_id="test-default_orientation", schedule=None)
        assert conf.get("webserver", "dag_orientation") == dag.orientation

    def test_dag_as_context_manager(self):
        """
        Test DAG as a context manager.
        When used as a context manager, Operators are automatically added to
        the DAG (unless they specify a different DAG)
        """
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})
        dag2 = DAG("dag2", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner2"})

        with dag:
            op1 = EmptyOperator(task_id="op1")
            op2 = EmptyOperator(task_id="op2", dag=dag2)

        assert op1.dag is dag
        assert op1.owner == "owner1"
        assert op2.dag is dag2
        assert op2.owner == "owner2"

        with dag2:
            op3 = EmptyOperator(task_id="op3")

        assert op3.dag is dag2
        assert op3.owner == "owner2"

        with dag:
            with dag2:
                op4 = EmptyOperator(task_id="op4")
            op5 = EmptyOperator(task_id="op5")

        assert op4.dag is dag2
        assert op5.dag is dag
        assert op4.owner == "owner2"
        assert op5.owner == "owner1"

        with DAG("creating_dag_in_cm", schedule=None, start_date=DEFAULT_DATE) as dag:
            EmptyOperator(task_id="op6")

        assert dag.dag_id == "creating_dag_in_cm"
        assert dag.tasks[0].task_id == "op6"

        with dag:
            with dag:
                op7 = EmptyOperator(task_id="op7")
            op8 = EmptyOperator(task_id="op8")
        op9 = EmptyOperator(task_id="op8")
        op9.dag = dag2

        assert op7.dag == dag
        assert op8.dag == dag
        assert op9.dag == dag2

    def test_dag_topological_sort_dag_without_tasks(self):
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert () == dag.topological_sort()

    def test_dag_naive_start_date_string(self):
        DAG("DAG", schedule=None, default_args={"start_date": "2019-06-01"})

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

    def test_dag_naive_default_args_start_date(self):
        dag = DAG("DAG", schedule=None, default_args={"start_date": datetime.datetime(2018, 1, 1)})
        assert dag.timezone == settings.TIMEZONE
        dag = DAG("DAG", schedule=None, start_date=datetime.datetime(2018, 1, 1))
        assert dag.timezone == settings.TIMEZONE

    def test_dag_none_default_args_start_date(self):
        """
        Tests if a start_date of None in default_args
        works.
        """
        dag = DAG("DAG", schedule=None, default_args={"start_date": None})
        assert dag.timezone == settings.TIMEZONE

    def test_dag_task_priority_weight_total(self):
        width = 5
        depth = 5
        weight = 5
        pattern = re.compile("stage(\\d*).(\\d*)")
        # Fully connected parallel tasks. i.e. every task at each parallel
        # stage is dependent on every task in the previous stage.
        # Default weight should be calculated using downstream descendants
        with DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}) as dag:
            pipeline = [
                [EmptyOperator(task_id=f"stage{i}.{j}", priority_weight=weight) for j in range(width)]
                for i in range(depth)
            ]
            for upstream, downstream in zip(pipeline, pipeline[1:]):
                for up_task, down_task in itertools.product(upstream, downstream):
                    down_task.set_upstream(up_task)

            for task in dag.task_dict.values():
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = ((depth - (task_depth + 1)) * width + 1) * weight

                calculated_weight = task.priority_weight_total
                assert calculated_weight == correct_weight

    def test_dag_task_priority_weight_total_using_upstream(self):
        # Same test as above except use 'upstream' for weight calculation
        weight = 3
        width = 5
        depth = 5
        pattern = re.compile("stage(\\d*).(\\d*)")
        with DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}) as dag:
            pipeline = [
                [
                    EmptyOperator(
                        task_id=f"stage{i}.{j}",
                        priority_weight=weight,
                        weight_rule=WeightRule.UPSTREAM,
                    )
                    for j in range(width)
                ]
                for i in range(depth)
            ]
            for upstream, downstream in zip(pipeline, pipeline[1:]):
                for up_task, down_task in itertools.product(upstream, downstream):
                    down_task.set_upstream(up_task)

            for task in dag.task_dict.values():
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = (task_depth * width + 1) * weight

                calculated_weight = task.priority_weight_total
                assert calculated_weight == correct_weight

    def test_dag_task_priority_weight_total_using_absolute(self):
        # Same test as above except use 'absolute' for weight calculation
        weight = 10
        width = 5
        depth = 5
        with DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}) as dag:
            pipeline = [
                [
                    EmptyOperator(
                        task_id=f"stage{i}.{j}",
                        priority_weight=weight,
                        weight_rule=WeightRule.ABSOLUTE,
                    )
                    for j in range(width)
                ]
                for i in range(depth)
            ]
            for upstream, downstream in zip(pipeline, pipeline[1:]):
                for up_task, down_task in itertools.product(upstream, downstream):
                    down_task.set_upstream(up_task)

            for task in dag.task_dict.values():
                # the sum of each stages after this task + itself
                correct_weight = weight
                calculated_weight = task.priority_weight_total
                assert calculated_weight == correct_weight

    def test_dag_task_invalid_weight_rule(self):
        # Test if we enter an invalid weight rule
        with DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}):
            with pytest.raises(AirflowException):
                EmptyOperator(task_id="should_fail", weight_rule="no rule")

    @pytest.mark.parametrize(
        "cls, expected",
        [
            (StaticTestPriorityWeightStrategy, 99),
            (FactorPriorityWeightStrategy, 3),
        ],
    )
    def test_dag_task_custom_weight_strategy(self, cls, expected):
        with mock_plugin_manager(plugins=[TestPriorityWeightStrategyPlugin]), DAG(
            "dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}
        ) as dag:
            task = EmptyOperator(
                task_id="empty_task",
                weight_rule=cls(),
            )
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            state=None,
            run_id="test",
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        ti = dr.get_task_instance(task.task_id)
        assert ti.priority_weight == expected

    def test_dag_task_not_registered_weight_strategy(self):
        with mock_plugin_manager(plugins=[TestPriorityWeightStrategyPlugin]), DAG(
            "dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}
        ):
            with pytest.raises(AirflowException, match="Unknown priority strategy"):
                EmptyOperator(
                    task_id="empty_task",
                    weight_rule=NotRegisteredPriorityWeightStrategy(),
                )

    def test_get_num_task_instances(self):
        test_dag_id = "test_get_num_task_instances_dag"
        test_task_id = "task_1"

        test_dag = DAG(dag_id=test_dag_id, schedule=None, start_date=DEFAULT_DATE)
        test_task = EmptyOperator(task_id=test_task_id, dag=test_dag)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr1 = test_dag.create_dagrun(
            state=None,
            run_id="test1",
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        dr2 = test_dag.create_dagrun(
            state=None,
            run_id="test2",
            execution_date=DEFAULT_DATE + datetime.timedelta(days=1),
            data_interval=(
                DEFAULT_DATE + datetime.timedelta(days=1),
                DEFAULT_DATE + datetime.timedelta(days=1),
            ),
            **triggered_by_kwargs,
        )
        dr3 = test_dag.create_dagrun(
            state=None,
            run_id="test3",
            execution_date=DEFAULT_DATE + datetime.timedelta(days=2),
            data_interval=(
                DEFAULT_DATE + datetime.timedelta(days=2),
                DEFAULT_DATE + datetime.timedelta(days=2),
            ),
            **triggered_by_kwargs,
        )
        dr4 = test_dag.create_dagrun(
            state=None,
            run_id="test4",
            execution_date=DEFAULT_DATE + datetime.timedelta(days=3),
            data_interval=(
                DEFAULT_DATE + datetime.timedelta(days=2),
                DEFAULT_DATE + datetime.timedelta(days=2),
            ),
            **triggered_by_kwargs,
        )

        ti1 = TI(task=test_task, run_id=dr1.run_id)
        ti1.state = None
        ti2 = TI(task=test_task, run_id=dr2.run_id)
        ti2.state = State.RUNNING
        ti3 = TI(task=test_task, run_id=dr3.run_id)
        ti3.state = State.QUEUED
        ti4 = TI(task=test_task, run_id=dr4.run_id)
        ti4.state = State.RUNNING
        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)
        session.merge(ti4)
        session.commit()

        assert 0 == DAG.get_num_task_instances(test_dag_id, task_ids=["fakename"], session=session)
        assert 4 == DAG.get_num_task_instances(test_dag_id, task_ids=[test_task_id], session=session)
        assert 4 == DAG.get_num_task_instances(
            test_dag_id, task_ids=["fakename", test_task_id], session=session
        )
        assert 1 == DAG.get_num_task_instances(
            test_dag_id, task_ids=[test_task_id], states=[None], session=session
        )
        assert 2 == DAG.get_num_task_instances(
            test_dag_id, task_ids=[test_task_id], states=[State.RUNNING], session=session
        )
        assert 3 == DAG.get_num_task_instances(
            test_dag_id, task_ids=[test_task_id], states=[None, State.RUNNING], session=session
        )
        assert 4 == DAG.get_num_task_instances(
            test_dag_id, task_ids=[test_task_id], states=[None, State.QUEUED, State.RUNNING], session=session
        )
        session.close()

    def test_get_task_instances_before(self):
        BASE_DATE = timezone.datetime(2022, 7, 20, 20)

        test_dag_id = "test_get_task_instances_before"
        test_task_id = "the_task"

        test_dag = DAG(dag_id=test_dag_id, schedule=None, start_date=BASE_DATE)
        EmptyOperator(task_id=test_task_id, dag=test_dag)

        session = settings.Session()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        def dag_run_before(delta_h=0, type=DagRunType.SCHEDULED):
            dagrun = test_dag.create_dagrun(
                state=State.SUCCESS,
                run_type=type,
                run_id=f"test_{delta_h}",
                session=session,
                **triggered_by_kwargs,
            )
            dagrun.start_date = BASE_DATE + timedelta(hours=delta_h)
            dagrun.execution_date = BASE_DATE + timedelta(hours=delta_h)
            return dagrun

        dr1 = dag_run_before(delta_h=-1, type=DagRunType.MANUAL)  # H19
        dr2 = dag_run_before(delta_h=-2, type=DagRunType.MANUAL)  # H18
        dr3 = dag_run_before(delta_h=-3, type=DagRunType.MANUAL)  # H17
        dr4 = dag_run_before(delta_h=-4, type=DagRunType.MANUAL)  # H16
        dr5 = dag_run_before(delta_h=-5)  # H15
        dr6 = dag_run_before(delta_h=-6)  # H14
        dr7 = dag_run_before(delta_h=-7)  # H13
        dr8 = dag_run_before(delta_h=-8)  # H12

        session.commit()

        REF_DATE = BASE_DATE

        assert set([dr.run_id for dr in [dr1]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=1, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr1, dr2, dr3]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=3, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr1, dr2, dr3, dr4, dr5]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=5, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr1, dr2, dr3, dr4, dr5, dr6, dr7]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=7, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr1, dr2, dr3, dr4, dr5, dr6, dr7, dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=9, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr1, dr2, dr3, dr4, dr5, dr6, dr7, dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=10, session=session)
            ]
        )  # stays constrained to available ones

        REF_DATE = BASE_DATE + timedelta(hours=-3.5)

        assert set([dr.run_id for dr in [dr4]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=1, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr4, dr5, dr6]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=3, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr4, dr5, dr6, dr7, dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=5, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr4, dr5, dr6, dr7, dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=6, session=session)
            ]
        )  # stays constrained to available ones

        REF_DATE = BASE_DATE + timedelta(hours=-8)

        assert set([dr.run_id for dr in [dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=0, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=1, session=session)
            ]
        )
        assert set([dr.run_id for dr in [dr8]]) == set(
            [
                ti.run_id
                for ti in test_dag.get_task_instances_before(base_date=REF_DATE, num=10, session=session)
            ]
        )

        session.close()

    def test_user_defined_filters_macros(self):
        def jinja_udf(name):
            return f"Hello {name}"

        dag = DAG(
            "test-dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            user_defined_filters={"hello": jinja_udf},
            user_defined_macros={"foo": "bar"},
        )
        jinja_env = dag.get_template_env()

        assert "hello" in jinja_env.filters
        assert jinja_env.filters["hello"] == jinja_udf
        assert jinja_env.globals["foo"] == "bar"

    def test_set_jinja_env_additional_option(self):
        dag = DAG(
            dag_id="test-dag",
            schedule=None,
            jinja_environment_kwargs={"keep_trailing_newline": True, "cache_size": 50},
        )
        jinja_env = dag.get_template_env()
        assert jinja_env.keep_trailing_newline is True
        assert jinja_env.cache.capacity == 50

        assert jinja_env.undefined is jinja2.StrictUndefined

    def test_template_undefined(self):
        dag = DAG("test-dag", schedule=None, template_undefined=jinja2.Undefined)
        jinja_env = dag.get_template_env()
        assert jinja_env.undefined is jinja2.Undefined

    @pytest.mark.parametrize(
        "use_native_obj, force_sandboxed, expected_env",
        [
            (False, True, SandboxedEnvironment),
            (False, False, SandboxedEnvironment),
            (True, False, NativeEnvironment),
            (True, True, SandboxedEnvironment),
        ],
    )
    def test_template_env(self, use_native_obj, force_sandboxed, expected_env):
        dag = DAG("test-dag", schedule=None, render_template_as_native_obj=use_native_obj)
        jinja_env = dag.get_template_env(force_sandboxed=force_sandboxed)
        assert isinstance(jinja_env, expected_env)

    def test_resolve_template_files_value(self, tmp_path):
        path = tmp_path / "testfile.template"
        path.write_text("{{ ds }}")

        with DAG(
            dag_id="test-dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            template_searchpath=os.fspath(path.parent),
        ):
            task = EmptyOperator(task_id="op1")

        task.test_field = path.name
        task.template_fields = ("test_field",)
        task.template_ext = (".template",)
        task.resolve_template_files()

        assert task.test_field == "{{ ds }}"

    def test_resolve_template_files_list(self, tmp_path):
        path = tmp_path / "testfile.template"
        path.write_text("{{ ds }}")

        with DAG(
            dag_id="test-dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            template_searchpath=os.fspath(path.parent),
        ):
            task = EmptyOperator(task_id="op1")

        task.test_field = [path.name, "some_string"]
        task.template_fields = ("test_field",)
        task.template_ext = (".template",)
        task.resolve_template_files()

        assert task.test_field == ["{{ ds }}", "some_string"]

    def test_create_dagrun_when_schedule_is_none_and_empty_start_date(self):
        # Check that we don't get an AttributeError 'start_date' for self.start_date when schedule is none
        dag = DAG("dag_with_none_schedule_and_empty_start_date", schedule=None)
        dag.add_task(BaseOperator(task_id="task_without_start_date"))
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun = dag.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        assert dagrun is not None

    def test_fail_dag_when_schedule_is_non_none_and_empty_start_date(self):
        # Check that we get a ValueError 'start_date' for self.start_date when schedule is non-none
        with pytest.raises(ValueError, match="start_date is required when catchup=True"):
            DAG(dag_id="dag_with_non_none_schedule_and_empty_start_date", schedule="@hourly", catchup=True)

    def test_dagtag_repr(self):
        clear_db_dags()
        dag = DAG("dag-test-dagtag", schedule=None, start_date=DEFAULT_DATE, tags=["tag-1", "tag-2"])
        dag.sync_to_db()
        with create_session() as session:
            assert {"tag-1", "tag-2"} == {
                repr(t) for t in session.query(DagTag).filter(DagTag.dag_id == "dag-test-dagtag").all()
            }

    def test_bulk_write_to_db(self):
        clear_db_dags()
        dags = [
            DAG(f"dag-bulk-sync-{i}", schedule=None, start_date=DEFAULT_DATE, tags=["test-dag"])
            for i in range(4)
        ]

        with assert_queries_count(5):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag"),
                ("dag-bulk-sync-1", "test-dag"),
                ("dag-bulk-sync-2", "test-dag"),
                ("dag-bulk-sync-3", "test-dag"),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(8):
            DAG.bulk_write_to_db(dags)
        with assert_queries_count(8):
            DAG.bulk_write_to_db(dags)
        # Adding tags
        for dag in dags:
            dag.tags.add("test-dag2")
        with assert_queries_count(9):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag"),
                ("dag-bulk-sync-0", "test-dag2"),
                ("dag-bulk-sync-1", "test-dag"),
                ("dag-bulk-sync-1", "test-dag2"),
                ("dag-bulk-sync-2", "test-dag"),
                ("dag-bulk-sync-2", "test-dag2"),
                ("dag-bulk-sync-3", "test-dag"),
                ("dag-bulk-sync-3", "test-dag2"),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())
        # Removing tags
        for dag in dags:
            dag.tags.remove("test-dag")
        with assert_queries_count(9):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag2"),
                ("dag-bulk-sync-1", "test-dag2"),
                ("dag-bulk-sync-2", "test-dag2"),
                ("dag-bulk-sync-3", "test-dag2"),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

        # Removing all tags
        for dag in dags:
            dag.tags = set()
        with assert_queries_count(9):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert not set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

    def test_bulk_write_to_db_single_dag(self):
        """
        Test bulk_write_to_db for a single dag using the index optimized query
        """
        clear_db_dags()
        dags = [
            DAG(f"dag-bulk-sync-{i}", schedule=None, start_date=DEFAULT_DATE, tags=["test-dag"])
            for i in range(1)
        ]

        with assert_queries_count(5):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0"} == {row[0] for row in session.query(DagModel.dag_id).all()}
            assert {
                ("dag-bulk-sync-0", "test-dag"),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(8):
            DAG.bulk_write_to_db(dags)
        with assert_queries_count(8):
            DAG.bulk_write_to_db(dags)

    def test_bulk_write_to_db_multiple_dags(self):
        """
        Test bulk_write_to_db for multiple dags which does not use the index optimized query
        """
        clear_db_dags()
        dags = [
            DAG(f"dag-bulk-sync-{i}", schedule=None, start_date=DEFAULT_DATE, tags=["test-dag"])
            for i in range(4)
        ]

        with assert_queries_count(5):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag"),
                ("dag-bulk-sync-1", "test-dag"),
                ("dag-bulk-sync-2", "test-dag"),
                ("dag-bulk-sync-3", "test-dag"),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(8):
            DAG.bulk_write_to_db(dags)
        with assert_queries_count(8):
            DAG.bulk_write_to_db(dags)

    @pytest.mark.parametrize("interval", [None, "@daily"])
    def test_bulk_write_to_db_interval_save_runtime(self, interval):
        mock_active_runs_of_dags = mock.MagicMock(side_effect=DagRun.active_runs_of_dags)
        with mock.patch.object(DagRun, "active_runs_of_dags", mock_active_runs_of_dags):
            dags_null_timetable = [
                DAG("dag-interval-None", schedule=None, start_date=TEST_DATE),
                DAG("dag-interval-test", schedule=interval, start_date=TEST_DATE),
            ]
            DAG.bulk_write_to_db(dags_null_timetable, session=settings.Session())
            if interval:
                mock_active_runs_of_dags.assert_called_once()
            else:
                mock_active_runs_of_dags.assert_not_called()

    @pytest.mark.parametrize("state", [DagRunState.RUNNING, DagRunState.QUEUED])
    def test_bulk_write_to_db_max_active_runs(self, state):
        """
        Test that DagModel.next_dagrun_create_after is set to NULL when the dag cannot be created due to max
        active runs being hit.
        """
        dag = DAG(
            dag_id="test_scheduler_verify_max_active_runs",
            schedule=timedelta(days=1),
            start_date=DEFAULT_DATE,
        )
        dag.max_active_runs = 1

        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        dag.clear()
        DAG.bulk_write_to_db([dag], session=session)

        model = session.get(DagModel, dag.dag_id)

        assert model.next_dagrun == DEFAULT_DATE
        assert model.next_dagrun_create_after == DEFAULT_DATE + timedelta(days=1)

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            state=state,
            execution_date=model.next_dagrun,
            run_type=DagRunType.SCHEDULED,
            session=session,
            data_interval=(model.next_dagrun, model.next_dagrun),
            **triggered_by_kwargs,
        )
        assert dr is not None
        DAG.bulk_write_to_db([dag])

        model = session.get(DagModel, dag.dag_id)
        # We signal "at max active runs" by saying this run is never eligible to be created
        assert model.next_dagrun_create_after is None
        # test that bulk_write_to_db again doesn't update next_dagrun_create_after
        DAG.bulk_write_to_db([dag])
        model = session.get(DagModel, dag.dag_id)
        assert model.next_dagrun_create_after is None

    def test_bulk_write_to_db_has_import_error(self):
        """
        Test that DagModel.has_import_error is set to false if no import errors.
        """
        dag = DAG(dag_id="test_has_import_error", schedule=None, start_date=DEFAULT_DATE)

        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        dag.clear()
        DAG.bulk_write_to_db([dag], session=session)

        model = session.get(DagModel, dag.dag_id)

        assert not model.has_import_errors

        # Simulate Dagfileprocessor setting the import error to true
        model.has_import_errors = True
        session.merge(model)
        session.flush()
        model = session.get(DagModel, dag.dag_id)
        # assert
        assert model.has_import_errors
        # parse
        DAG.bulk_write_to_db([dag])

        model = session.get(DagModel, dag.dag_id)
        # assert that has_import_error is now false
        assert not model.has_import_errors
        session.close()

    def test_bulk_write_to_db_assets(self):
        """
        Ensure that assets referenced in a dag are correctly loaded into the database.
        """
        dag_id1 = "test_asset_dag1"
        dag_id2 = "test_asset_dag2"
        task_id = "test_asset_task"
        uri1 = "s3://asset/1"
        d1 = Asset(uri1, extra={"not": "used"})
        d2 = Asset("s3://asset/2")
        d3 = Asset("s3://asset/3")
        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=[d1])
        EmptyOperator(task_id=task_id, dag=dag1, outlets=[d2, d3])
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(task_id=task_id, dag=dag2, outlets=[Asset(uri1, extra={"should": "be used"})])
        session = settings.Session()
        dag1.clear()
        DAG.bulk_write_to_db([dag1, dag2], session=session)
        session.commit()
        stored_assets = {x.uri: x for x in session.query(AssetModel).all()}
        asset1_orm = stored_assets[d1.uri]
        asset2_orm = stored_assets[d2.uri]
        asset3_orm = stored_assets[d3.uri]
        assert stored_assets[uri1].extra == {"should": "be used"}
        assert [x.dag_id for x in asset1_orm.consuming_dags] == [dag_id1]
        assert [(x.task_id, x.dag_id) for x in asset1_orm.producing_tasks] == [(task_id, dag_id2)]
        assert set(
            session.query(
                TaskOutletAssetReference.task_id,
                TaskOutletAssetReference.dag_id,
                TaskOutletAssetReference.dataset_id,
            )
            .filter(TaskOutletAssetReference.dag_id.in_((dag_id1, dag_id2)))
            .all()
        ) == {
            (task_id, dag_id1, asset2_orm.id),
            (task_id, dag_id1, asset3_orm.id),
            (task_id, dag_id2, asset1_orm.id),
        }

        # now that we have verified that a new dag has its asset references recorded properly,
        # we need to verify that *changes* are recorded properly.
        # so if any references are *removed*, they should also be deleted from the DB
        # so let's remove some references and see what happens
        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(task_id=task_id, dag=dag1, outlets=[d2])
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(task_id=task_id, dag=dag2)
        DAG.bulk_write_to_db([dag1, dag2], session=session)
        session.commit()
        session.expunge_all()
        stored_assets = {x.uri: x for x in session.query(AssetModel).all()}
        asset1_orm = stored_assets[d1.uri]
        asset2_orm = stored_assets[d2.uri]
        assert [x.dag_id for x in asset1_orm.consuming_dags] == []
        assert set(
            session.query(
                TaskOutletAssetReference.task_id,
                TaskOutletAssetReference.dag_id,
                TaskOutletAssetReference.dataset_id,
            )
            .filter(TaskOutletAssetReference.dag_id.in_((dag_id1, dag_id2)))
            .all()
        ) == {(task_id, dag_id1, asset2_orm.id)}

    def test_bulk_write_to_db_unorphan_assets(self):
        """
        Assets can lose their last reference and be orphaned, but then if a reference to them reappears, we
        need to un-orphan those assets
        """
        with create_session() as session:
            # Create four assets - two that have references and two that are unreferenced and marked as
            # orphans
            asset1 = Asset(uri="ds1")
            asset2 = Asset(uri="ds2")
            session.add(AssetModel(uri=asset2.uri))
            asset3 = Asset(uri="ds3")
            asset4 = Asset(uri="ds4")
            session.add(AssetModel(uri=asset4.uri))
            session.flush()

            dag1 = DAG(dag_id="assets-1", start_date=DEFAULT_DATE, schedule=[asset1])
            BashOperator(dag=dag1, task_id="task", bash_command="echo 1", outlets=[asset3])

            DAG.bulk_write_to_db([dag1], session=session)

            # Double check
            non_orphaned_assets = [
                asset.uri
                for asset in session.query(AssetModel.uri)
                .filter(AssetModel.active.any())
                .order_by(AssetModel.uri)
            ]
            assert non_orphaned_assets == ["ds1", "ds3"]
            orphaned_assets = [
                asset.uri
                for asset in session.query(AssetModel.uri)
                .filter(~AssetModel.active.any())
                .order_by(AssetModel.uri)
            ]
            assert orphaned_assets == ["ds2", "ds4"]

            # Now add references to the two unreferenced assets
            dag1 = DAG(dag_id="assets-1", start_date=DEFAULT_DATE, schedule=[asset1, asset2])
            BashOperator(dag=dag1, task_id="task", bash_command="echo 1", outlets=[asset3, asset4])

            DAG.bulk_write_to_db([dag1], session=session)

            # and count the orphans and non-orphans
            non_orphaned_asset_count = session.query(AssetModel).filter(AssetModel.active.any()).count()
            assert non_orphaned_asset_count == 4
            orphaned_asset_count = session.query(AssetModel).filter(~AssetModel.active.any()).count()
            assert orphaned_asset_count == 0

    def test_bulk_write_to_db_asset_aliases(self):
        """
        Ensure that asset aliases referenced in a dag are correctly loaded into the database.
        """
        dag_id1 = "test_asset_alias_dag1"
        dag_id2 = "test_asset_alias_dag2"
        task_id = "test_asset_task"
        asset_alias_1 = AssetAlias(name="asset_alias_1")
        asset_alias_2 = AssetAlias(name="asset_alias_2")
        asset_alias_2_2 = AssetAlias(name="asset_alias_2")
        asset_alias_3 = AssetAlias(name="asset_alias_3")
        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(task_id=task_id, dag=dag1, outlets=[asset_alias_1, asset_alias_2, asset_alias_3])
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(task_id=task_id, dag=dag2, outlets=[asset_alias_2_2, asset_alias_3])
        session = settings.Session()
        DAG.bulk_write_to_db([dag1, dag2], session=session)
        session.commit()

        stored_asset_alias_models = {x.name: x for x in session.query(AssetAliasModel).all()}
        asset_alias_1_orm = stored_asset_alias_models[asset_alias_1.name]
        asset_alias_2_orm = stored_asset_alias_models[asset_alias_2.name]
        asset_alias_3_orm = stored_asset_alias_models[asset_alias_3.name]
        assert asset_alias_1_orm.name == "asset_alias_1"
        assert asset_alias_2_orm.name == "asset_alias_2"
        assert asset_alias_3_orm.name == "asset_alias_3"
        assert len(stored_asset_alias_models) == 3

    def test_sync_to_db(self):
        dag = DAG("dag", start_date=DEFAULT_DATE, schedule=None)
        with dag:
            EmptyOperator(task_id="task", owner="owner1")
            EmptyOperator(task_id="task2", owner="owner2")
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == "dag").one()
        assert set(orm_dag.owners.split(", ")) == {"owner1", "owner2"}
        assert orm_dag.is_active
        assert orm_dag.default_view is not None
        assert orm_dag.default_view == conf.get("webserver", "dag_default_view").lower()
        assert orm_dag.safe_dag_id == "dag"
        session.close()

    def test_sync_to_db_default_view(self):
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_view="graph")
        with dag:
            EmptyOperator(task_id="task", owner="owner1")
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == "dag").one()
        assert orm_dag.default_view is not None
        assert orm_dag.default_view == "graph"
        session.close()

    def test_existing_dag_is_paused_upon_creation(self):
        dag = DAG("dag_paused", schedule=None)
        dag.sync_to_db()
        assert not dag.get_is_paused()

        dag = DAG("dag_paused", schedule=None, is_paused_upon_creation=True)
        dag.sync_to_db()
        # Since the dag existed before, it should not follow the pause flag upon creation
        assert not dag.get_is_paused()

    def test_new_dag_is_paused_upon_creation(self):
        dag = DAG("new_nonexisting_dag", schedule=None, is_paused_upon_creation=True)
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == "new_nonexisting_dag").one()
        # Since the dag didn't exist before, it should follow the pause flag upon creation
        assert orm_dag.is_paused
        session.close()

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CORE__MAX_CONSECUTIVE_FAILED_DAG_RUNS_PER_DAG": "4",
        },
    )
    def test_existing_dag_is_paused_config(self):
        # config should be set properly
        assert conf.getint("core", "max_consecutive_failed_dag_runs_per_dag") == 4
        # checking the default value is coming from config
        dag = DAG("test_dag", schedule=None)
        assert dag.max_consecutive_failed_dag_runs == 4
        # but we can override the value using params
        dag = DAG("test_dag2", schedule=None, max_consecutive_failed_dag_runs=2)
        assert dag.max_consecutive_failed_dag_runs == 2

    def test_existing_dag_is_paused_after_limit(self):
        def add_failed_dag_run(id, execution_date):
            triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            dr = dag.create_dagrun(
                run_type=DagRunType.MANUAL,
                run_id="run_id_" + id,
                execution_date=execution_date,
                state=State.FAILED,
                data_interval=(execution_date, execution_date),
                **triggered_by_kwargs,
            )
            ti_op1 = dr.get_task_instance(task_id=op1.task_id, session=session)
            ti_op1.set_state(state=TaskInstanceState.FAILED, session=session)
            dr.update_state(session=session)

        dag_id = "dag_paused_after_limit"
        dag = DAG(dag_id, schedule=None, is_paused_upon_creation=False, max_consecutive_failed_dag_runs=2)
        op1 = BashOperator(task_id="task", bash_command="exit 1;")
        dag.add_task(op1)
        session = settings.Session()
        dag.sync_to_db(session=session)
        assert not dag.get_is_paused()

        # dag should be paused after 2 failed dag_runs
        add_failed_dag_run(
            "1",
            TEST_DATE,
        )
        add_failed_dag_run("2", TEST_DATE + timedelta(days=1))
        assert dag.get_is_paused()
        dag.clear()
        self._clean_up(dag_id)

    def test_existing_dag_default_view(self):
        with create_session() as session:
            session.add(DagModel(dag_id="dag_default_view_old", default_view=None))
            session.commit()
            orm_dag = session.query(DagModel).filter(DagModel.dag_id == "dag_default_view_old").one()
        assert orm_dag.default_view is None
        assert orm_dag.get_default_view() == conf.get("webserver", "dag_default_view").lower()

    def test_dag_is_deactivated_upon_dagfile_deletion(self):
        dag_id = "old_existing_dag"
        dag_fileloc = "/usr/local/airflow/dags/non_existing_path.py"
        dag = DAG(dag_id, schedule=None, is_paused_upon_creation=True)
        dag.fileloc = dag_fileloc
        session = settings.Session()
        with mock.patch("airflow.models.dag.DagCode.bulk_sync_to_db"):
            dag.sync_to_db(session=session, processor_subdir="/usr/local/airflow/dags/")

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one()

        assert orm_dag.is_active
        assert orm_dag.fileloc == dag_fileloc

        DagModel.deactivate_deleted_dags(
            list_py_file_paths(settings.DAGS_FOLDER),
            processor_subdir="/usr/local/airflow/dags/",
        )

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one()
        assert not orm_dag.is_active

        session.execute(DagModel.__table__.delete().where(DagModel.dag_id == dag_id))
        session.close()

    def test_dag_naive_default_args_start_date_with_timezone(self):
        local_tz = pendulum.timezone("Europe/Zurich")
        default_args = {"start_date": datetime.datetime(2018, 1, 1, tzinfo=local_tz)}

        dag = DAG("DAG", schedule=None, default_args=default_args)
        assert dag.timezone.name == local_tz.name

        dag = DAG("DAG", schedule=None, default_args=default_args)
        assert dag.timezone.name == local_tz.name

    def test_roots(self):
        """Verify if dag.roots returns the root tasks of a DAG."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = EmptyOperator(task_id="t1")
            op2 = EmptyOperator(task_id="t2")
            op3 = EmptyOperator(task_id="t3")
            op4 = EmptyOperator(task_id="t4")
            op5 = EmptyOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.roots) == {op1, op2}

    def test_leaves(self):
        """Verify if dag.leaves returns the leaf tasks of a DAG."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = EmptyOperator(task_id="t1")
            op2 = EmptyOperator(task_id="t2")
            op3 = EmptyOperator(task_id="t3")
            op4 = EmptyOperator(task_id="t4")
            op5 = EmptyOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.leaves) == {op4, op5}

    def test_duplicate_task_ids_not_allowed_with_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = EmptyOperator(task_id="t1")
            with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
                BashOperator(task_id="t1", bash_command="sleep 1")

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_not_allowed_without_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        dag = DAG("test_dag", schedule=None, start_date=DEFAULT_DATE)
        op1 = EmptyOperator(task_id="t1", dag=dag)
        with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
            EmptyOperator(task_id="t1", dag=dag)

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_for_same_task_is_allowed(self):
        """Verify that same tasks with Duplicate task_id do not raise error"""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = op2 = EmptyOperator(task_id="t1")
            op3 = EmptyOperator(task_id="t3")
            op1 >> op3
            op2 >> op3

        assert op1 == op2
        assert dag.task_dict == {op1.task_id: op1, op3.task_id: op3}
        assert dag.task_dict == {op2.task_id: op2, op3.task_id: op3}

    def test_partial_subset_updates_all_references_while_deepcopy(self):
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = EmptyOperator(task_id="t1")
            op2 = EmptyOperator(task_id="t2")
            op3 = EmptyOperator(task_id="t3")
            op1 >> op2
            op2 >> op3

        partial = dag.partial_subset("t2", include_upstream=True, include_downstream=False)
        assert id(partial.task_dict["t1"].downstream_list[0].dag) == id(partial)

        # Copied DAG should not include unused task IDs in used_group_ids
        assert "t3" not in partial.task_group.used_group_ids

    def test_partial_subset_taskgroup_join_ids(self):
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            start = EmptyOperator(task_id="start")
            with TaskGroup(group_id="outer", prefix_group_id=False) as outer_group:
                with TaskGroup(group_id="tg1", prefix_group_id=False) as tg1:
                    EmptyOperator(task_id="t1")
                with TaskGroup(group_id="tg2", prefix_group_id=False) as tg2:
                    EmptyOperator(task_id="t2")

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

    def test_schedule_dag_no_previous_runs(self):
        """
        Tests scheduling a dag with no previous runs
        """
        dag_id = "test_schedule_dag_no_previous_runs"
        dag = DAG(dag_id=dag_id, schedule=None)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=TEST_DATE))
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dag_run = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=TEST_DATE,
            state=State.RUNNING,
            data_interval=(TEST_DATE, TEST_DATE),
            **triggered_by_kwargs,
        )
        assert dag_run is not None
        assert dag.dag_id == dag_run.dag_id
        assert dag_run.run_id is not None
        assert "" != dag_run.run_id
        assert (
            TEST_DATE == dag_run.execution_date
        ), f"dag_run.execution_date did not match expectation: {dag_run.execution_date}"
        assert State.RUNNING == dag_run.state
        assert not dag_run.external_trigger
        dag.clear()
        self._clean_up(dag_id)

    @patch("airflow.models.dag.Stats")
    def test_dag_handle_callback_crash(self, mock_stats):
        """
        Tests avoid crashes from calling dag callbacks exceptions
        """
        dag_id = "test_dag_callback_crash"
        mock_callback_with_exception = mock.MagicMock()
        mock_callback_with_exception.side_effect = Exception
        dag = DAG(
            dag_id=dag_id,
            schedule=None,
            # callback with invalid signature should not cause crashes
            on_success_callback=lambda: 1,
            on_failure_callback=mock_callback_with_exception,
        )
        when = TEST_DATE
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=when))
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        with create_session() as session:
            dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=when,
                run_type=DagRunType.MANUAL,
                session=session,
                data_interval=(when, when),
                **triggered_by_kwargs,
            )

            # should not raise any exception
            dag.handle_callback(dag_run, success=False)
            dag.handle_callback(dag_run, success=True)

        mock_stats.incr.assert_called_with(
            "dag.callback_exceptions",
            tags={"dag_id": "test_dag_callback_crash"},
        )

        dag.clear()
        self._clean_up(dag_id)

    def test_dag_handle_callback_with_removed_task(self, dag_maker, session):
        """
        Tests avoid crashes when a removed task is the last one in the list of task instance
        """
        dag_id = "test_dag_callback_with_removed_task"
        mock_callback = mock.MagicMock()
        with DAG(
            dag_id=dag_id,
            schedule=None,
            on_success_callback=mock_callback,
            on_failure_callback=mock_callback,
        ) as dag:
            EmptyOperator(task_id="faketastic")
            task_removed = EmptyOperator(task_id="removed_task")

        with create_session() as session:
            triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=TEST_DATE,
                run_type=DagRunType.MANUAL,
                session=session,
                data_interval=(TEST_DATE, TEST_DATE),
                **triggered_by_kwargs,
            )
            dag._remove_task(task_removed.task_id)
            tis = dag_run.get_task_instances(session=session)
            tis[-1].state = TaskInstanceState.REMOVED
            assert dag_run.get_task_instance(task_removed.task_id).state == TaskInstanceState.REMOVED

            # should not raise any exception
            dag.handle_callback(dag_run, success=True)
            dag.handle_callback(dag_run, success=False)

        dag.clear()
        self._clean_up(dag_id)

    def test_next_dagrun_after_fake_scheduled_previous(self):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have
        """
        delta = datetime.timedelta(hours=1)
        dag_id = "test_schedule_dag_fake_scheduled_previous"
        dag = DAG(dag_id=dag_id, schedule=delta, start_date=DEFAULT_DATE)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=DEFAULT_DATE))
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
            external_trigger=True,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        dag.sync_to_db()
        with create_session() as session:
            model = session.get(DagModel, dag.dag_id)

        # Even though there is a run for this date already, it is marked as manual/external, so we should
        # create a scheduled one anyway!
        assert model.next_dagrun == DEFAULT_DATE
        assert model.next_dagrun_create_after == DEFAULT_DATE + delta

        self._clean_up(dag_id)

    def test_schedule_dag_once(self):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag_id = "test_schedule_dag_once"
        dag = DAG(dag_id=dag_id, schedule="@once", start_date=TEST_DATE)
        assert isinstance(dag.timetable, OnceTimetable)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=TEST_DATE))

        # Sync once to create the DagModel
        dag.sync_to_db()

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=TEST_DATE,
            state=State.SUCCESS,
            data_interval=(TEST_DATE, TEST_DATE),
            **triggered_by_kwargs,
        )

        # Then sync again after creating the dag run -- this should update next_dagrun
        dag.sync_to_db()
        with create_session() as session:
            model = session.get(DagModel, dag.dag_id)

        assert model.next_dagrun is None
        assert model.next_dagrun_create_after is None
        self._clean_up(dag_id)

    def test_fractional_seconds(self):
        """
        Tests if fractional seconds are stored in the database
        """
        dag_id = "test_fractional_seconds"
        dag = DAG(dag_id=dag_id, schedule="@once", start_date=TEST_DATE)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=TEST_DATE))

        start_date = timezone.utcnow()

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        run = dag.create_dagrun(
            run_id="test_" + start_date.isoformat(),
            execution_date=start_date,
            start_date=start_date,
            state=State.RUNNING,
            external_trigger=False,
            data_interval=(start_date, start_date),
            **triggered_by_kwargs,
        )

        run.refresh_from_db()

        assert start_date == run.execution_date, "dag run execution_date loses precision"
        assert start_date == run.start_date, "dag run start_date loses precision "
        self._clean_up(dag_id)

    def test_pickling(self):
        test_dag_id = "test_pickling"
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(test_dag_id, schedule=None, default_args=args)
        dag_pickle = dag.pickle()
        assert dag_pickle.pickle.dag_id == dag.dag_id

    def test_rich_comparison_ops(self):
        test_dag_id = "test_rich_comparison_ops"

        class DAGsubclass(DAG):
            pass

        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(test_dag_id, schedule=None, default_args=args)

        dag_eq = DAG(test_dag_id, schedule=None, default_args=args)

        dag_diff_load_time = DAG(test_dag_id, schedule=None, default_args=args)
        dag_diff_name = DAG(test_dag_id + "_neq", schedule=None, default_args=args)

        dag_subclass = DAGsubclass(test_dag_id, schedule=None, default_args=args)
        dag_subclass_diff_name = DAGsubclass(test_dag_id + "2", schedule=None, default_args=args)

        for dag_ in [dag_eq, dag_diff_name, dag_subclass, dag_subclass_diff_name]:
            dag_.last_loaded = dag.last_loaded

        # test identity equality
        assert dag == dag

        # test dag (in)equality based on _comps
        assert dag_eq == dag
        assert dag_diff_name != dag
        assert dag_diff_load_time != dag

        # test dag inequality based on type even if _comps happen to match
        assert dag_subclass != dag

        # a dag should equal an unpickled version of itself
        dump = pickle.dumps(dag)
        assert pickle.loads(dump) == dag

        # dags are ordered based on dag_id no matter what the type is
        assert dag < dag_diff_name
        assert dag > dag_diff_load_time
        assert dag < dag_subclass_diff_name

        # greater than should have been created automatically by functools
        assert dag_diff_name > dag

        # hashes are non-random and match equality
        assert hash(dag) == hash(dag)
        assert hash(dag_eq) == hash(dag)
        assert hash(dag_diff_name) != hash(dag)
        assert hash(dag_subclass) != hash(dag)

    def test_get_paused_dag_ids(self):
        dag_id = "test_get_paused_dag_ids"
        dag = DAG(dag_id, schedule=None, is_paused_upon_creation=True)
        dag.sync_to_db()
        assert DagModel.get_dagmodel(dag_id) is not None

        paused_dag_ids = DagModel.get_paused_dag_ids([dag_id])
        assert paused_dag_ids == {dag_id}

        with create_session() as session:
            session.query(DagModel).filter(DagModel.dag_id == dag_id).delete(synchronize_session=False)

    @pytest.mark.parametrize(
        "schedule_arg, expected_timetable, interval_description",
        [
            (None, NullTimetable(), "Never, external triggers only"),
            ("@daily", cron_timetable("0 0 * * *"), "At 00:00"),
            ("@weekly", cron_timetable("0 0 * * 0"), "At 00:00, only on Sunday"),
            ("@monthly", cron_timetable("0 0 1 * *"), "At 00:00, on day 1 of the month"),
            ("@quarterly", cron_timetable("0 0 1 */3 *"), "At 00:00, on day 1 of the month, every 3 months"),
            ("@yearly", cron_timetable("0 0 1 1 *"), "At 00:00, on day 1 of the month, only in January"),
            ("5 0 * 8 *", cron_timetable("5 0 * 8 *"), "At 00:05, only in August"),
            ("@once", OnceTimetable(), "Once, as soon as possible"),
            (datetime.timedelta(days=1), delta_timetable(datetime.timedelta(days=1)), ""),
            ("30 21 * * 5 1", cron_timetable("30 21 * * 5 1"), ""),
        ],
    )
    def test_timetable_and_description_from_schedule_arg(
        self, schedule_arg, expected_timetable, interval_description
    ):
        dag = DAG("test_schedule_arg", schedule=schedule_arg, start_date=TEST_DATE)
        assert dag.timetable == expected_timetable
        assert dag.timetable.description == interval_description

    def test_timetable_and_description_from_asset(self):
        dag = DAG("test_schedule_interval_arg", schedule=[Asset(uri="hello")], start_date=TEST_DATE)
        assert dag.timetable == AssetTriggeredTimetable(Asset(uri="hello"))
        assert dag.timetable.description == "Triggered by assets"

    @pytest.mark.parametrize(
        "timetable, expected_description",
        [
            (NullTimetable(), "Never, external triggers only"),
            (cron_timetable("0 0 * * *"), "At 00:00"),
            (cron_timetable("@daily"), "At 00:00"),
            (cron_timetable("0 0 * * 0"), "At 00:00, only on Sunday"),
            (cron_timetable("@weekly"), "At 00:00, only on Sunday"),
            (cron_timetable("0 0 1 * *"), "At 00:00, on day 1 of the month"),
            (cron_timetable("@monthly"), "At 00:00, on day 1 of the month"),
            (cron_timetable("0 0 1 */3 *"), "At 00:00, on day 1 of the month, every 3 months"),
            (cron_timetable("@quarterly"), "At 00:00, on day 1 of the month, every 3 months"),
            (cron_timetable("0 0 1 1 *"), "At 00:00, on day 1 of the month, only in January"),
            (cron_timetable("@yearly"), "At 00:00, on day 1 of the month, only in January"),
            (cron_timetable("5 0 * 8 *"), "At 00:05, only in August"),
            (OnceTimetable(), "Once, as soon as possible"),
            (delta_timetable(datetime.timedelta(days=1)), ""),
            (cron_timetable("30 21 * * 5 1"), ""),
        ],
    )
    def test_description_from_timetable(self, timetable, expected_description):
        dag = DAG("test_schedule_description", schedule=timetable, start_date=TEST_DATE)
        assert dag.timetable == timetable
        assert dag.timetable.description == expected_description

    def test_create_dagrun_run_id_is_generated(self):
        dag = DAG(dag_id="run_id_is_generated", schedule=None)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            state=State.NONE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        assert dr.run_id == f"manual__{DEFAULT_DATE.isoformat()}"

    def test_create_dagrun_run_type_is_obtained_from_run_id(self):
        dag = DAG(dag_id="run_type_is_obtained_from_run_id", schedule=None)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(run_id="scheduled__", state=State.NONE, **triggered_by_kwargs)
        assert dr.run_type == DagRunType.SCHEDULED

        dr = dag.create_dagrun(
            run_id="custom_is_set_to_manual",
            state=State.NONE,
            **triggered_by_kwargs,
        )
        assert dr.run_type == DagRunType.MANUAL

    def test_create_dagrun_job_id_is_set(self):
        job_id = 42
        dag = DAG(dag_id="test_create_dagrun_job_id_is_set", schedule=None)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_create_dagrun_job_id_is_set",
            state=State.NONE,
            creating_job_id=job_id,
            **triggered_by_kwargs,
        )
        assert dr.creating_job_id == job_id

    def test_dag_add_task_checks_trigger_rule(self):
        # A non fail stop dag should allow any trigger rule
        from airflow.exceptions import FailStopDagInvalidTriggerRule
        from airflow.utils.trigger_rule import TriggerRule

        task_with_non_default_trigger_rule = EmptyOperator(
            task_id="task_with_non_default_trigger_rule", trigger_rule=TriggerRule.ALWAYS
        )
        non_fail_stop_dag = DAG(
            dag_id="test_dag_add_task_checks_trigger_rule",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_stop=False,
        )
        non_fail_stop_dag.add_task(task_with_non_default_trigger_rule)

        # a fail stop dag should allow default trigger rule
        from airflow.models.abstractoperator import DEFAULT_TRIGGER_RULE

        fail_stop_dag = DAG(
            dag_id="test_dag_add_task_checks_trigger_rule",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_stop=True,
        )
        task_with_default_trigger_rule = EmptyOperator(
            task_id="task_with_default_trigger_rule", trigger_rule=DEFAULT_TRIGGER_RULE
        )
        fail_stop_dag.add_task(task_with_default_trigger_rule)

        # a fail stop dag should not allow a non-default trigger rule
        with pytest.raises(FailStopDagInvalidTriggerRule):
            fail_stop_dag.add_task(task_with_non_default_trigger_rule)

    def test_dag_add_task_sets_default_task_group(self):
        dag = DAG(dag_id="test_dag_add_task_sets_default_task_group", schedule=None, start_date=DEFAULT_DATE)
        task_without_task_group = EmptyOperator(task_id="task_without_group_id")
        default_task_group = TaskGroupContext.get_current_task_group(dag)
        dag.add_task(task_without_task_group)
        assert default_task_group.get_child_by_label("task_without_group_id") == task_without_task_group

        task_group = TaskGroup(group_id="task_group", dag=dag)
        task_with_task_group = EmptyOperator(task_id="task_with_task_group", task_group=task_group)
        dag.add_task(task_with_task_group)
        assert task_group.get_child_by_label("task_with_task_group") == task_with_task_group
        assert dag.get_task("task_group.task_with_task_group") == task_with_task_group

    @pytest.mark.parametrize("dag_run_state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_set_dagrun_state(self, dag_run_state):
        dag_id = "test_clear_set_dagrun_state"
        self._clean_up(dag_id)
        task_id = "t1"
        dag = DAG(dag_id, schedule=None, start_date=DEFAULT_DATE, max_active_runs=1)
        t_1 = EmptyOperator(task_id=task_id, dag=dag)

        session = settings.Session()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun_1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.FAILED,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        session.merge(dagrun_1)

        task_instance_1 = TI(t_1, run_id=dagrun_1.run_id, state=State.RUNNING)
        session.merge(task_instance_1)
        session.commit()

        dag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            dag_run_state=dag_run_state,
            session=session,
        )

        dagruns = session.query(DagRun).filter(DagRun.dag_id == dag_id).all()

        assert len(dagruns) == 1
        dagrun: DagRun = dagruns[0]
        assert dagrun.state == dag_run_state

    @pytest.mark.parametrize("dag_run_state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_set_dagrun_state_for_mapped_task(self, dag_run_state):
        dag_id = "test_clear_set_dagrun_state"
        self._clean_up(dag_id)
        task_id = "t1"

        dag = DAG(dag_id, schedule=None, start_date=DEFAULT_DATE, max_active_runs=1)

        @dag.task
        def make_arg_lists():
            return [[1], [2], [{"a": "b"}]]

        def consumer(value):
            print(value)

        mapped = PythonOperator.partial(task_id=task_id, dag=dag, python_callable=consumer).expand(
            op_args=make_arg_lists()
        )

        session = settings.Session()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun_1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.FAILED,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            session=session,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        expand_mapped_task(mapped, dagrun_1.run_id, "make_arg_lists", length=2, session=session)

        upstream_ti = dagrun_1.get_task_instance("make_arg_lists", session=session)
        ti = dagrun_1.get_task_instance(task_id, map_index=0, session=session)
        ti2 = dagrun_1.get_task_instance(task_id, map_index=1, session=session)
        upstream_ti.state = State.SUCCESS
        ti.state = State.SUCCESS
        ti2.state = State.SUCCESS
        session.flush()

        dag.clear(
            task_ids=[(task_id, 0), ("make_arg_lists")],
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            dag_run_state=dag_run_state,
            session=session,
        )
        session.refresh(upstream_ti)
        session.refresh(ti)
        session.refresh(ti2)
        assert upstream_ti.state is None  # cleared
        assert ti.state is None  # cleared
        assert ti2.state == State.SUCCESS  # not cleared
        dagruns = session.query(DagRun).filter(DagRun.dag_id == dag_id).all()

        assert len(dagruns) == 1
        dagrun: DagRun = dagruns[0]
        assert dagrun.state == dag_run_state

    def test_dag_test_basic(self):
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)
        mock_object = mock.MagicMock()

        @task_decorator
        def check_task():
            # we call a mock object to ensure that this task actually ran.
            mock_object()

        with dag:
            check_task()

        dag.test()
        mock_object.assert_called_once()

    def test_dag_test_with_dependencies(self):
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)
        mock_object = mock.MagicMock()

        @task_decorator
        def check_task():
            return "output of first task"

        @task_decorator
        def check_task_2(my_input):
            # we call a mock object to ensure that this task actually ran.
            mock_object(my_input)

        with dag:
            check_task_2(check_task())

        dag.test()
        mock_object.assert_called_with("output of first task")

    def test_dag_test_with_fail_handler(self):
        mock_handle_object_1 = mock.MagicMock()
        mock_handle_object_2 = mock.MagicMock()

        def handle_task_failure(context):
            ti = context["task_instance"]
            mock_handle_object_1(f"task {ti.task_id} failed...")

        def handle_dag_failure(context):
            ti = context["task_instance"]
            mock_handle_object_2(f"dag {ti.dag_id} run failed...")

        dag = DAG(
            dag_id="test_local_testing_conn_file",
            default_args={"on_failure_callback": handle_task_failure},
            on_failure_callback=handle_dag_failure,
            start_date=DEFAULT_DATE,
            schedule=None,
        )

        mock_task_object_1 = mock.MagicMock()
        mock_task_object_2 = mock.MagicMock()

        @task_decorator
        def check_task():
            mock_task_object_1()
            raise AirflowException("boooom")

        @task_decorator
        def check_task_2(my_input):
            # we call a mock object to ensure that this task actually ran.
            mock_task_object_2(my_input)

        with dag:
            check_task_2(check_task())

        dag.test()

        mock_handle_object_1.assert_called_with("task check_task failed...")
        mock_handle_object_2.assert_called_with("dag test_local_testing_conn_file run failed...")
        mock_task_object_1.assert_called()
        mock_task_object_2.assert_not_called()

    def test_dag_test_with_task_mapping(self):
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)
        mock_object = mock.MagicMock()

        @task_decorator()
        def get_index(current_val, ti=None):
            return ti.map_index

        @task_decorator
        def check_task(my_input):
            # we call a mock object with the combined map to ensure all expected indexes are called
            mock_object(list(my_input))

        with dag:
            mapped_task = get_index.expand(current_val=[1, 1, 1, 1, 1])
            check_task(mapped_task)

        dag.test()
        mock_object.assert_called_with([0, 1, 2, 3, 4])

    def test_dag_connection_file(self, tmp_path):
        test_connections_string = """
---
my_postgres_conn:
  - conn_id: my_postgres_conn
    conn_type: postgres
        """
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)

        @task_decorator
        def check_task():
            from airflow.configuration import secrets_backend_list
            from airflow.secrets.local_filesystem import LocalFilesystemBackend

            assert isinstance(secrets_backend_list[0], LocalFilesystemBackend)
            local_secrets: LocalFilesystemBackend = secrets_backend_list[0]
            assert local_secrets.get_connection("my_postgres_conn").conn_id == "my_postgres_conn"

        with dag:
            check_task()
        path = tmp_path / "testfile.yaml"
        path.write_text(test_connections_string)
        dag.test(conn_file_path=os.fspath(path))

    @pytest.mark.parametrize(
        "ti_state_begin, ti_state_end",
        [
            *((state, None) for state in State.task_states if state != TaskInstanceState.RUNNING),
            (TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING),
        ],
    )
    def test_clear_dag(
        self,
        ti_state_begin: TaskInstanceState | None,
        ti_state_end: TaskInstanceState | None,
    ):
        dag_id = "test_clear_dag"
        self._clean_up(dag_id)
        task_id = "t1"
        dag = DAG(dag_id, schedule=None, start_date=DEFAULT_DATE, max_active_runs=1)
        t_1 = EmptyOperator(task_id=task_id, dag=dag)

        session = settings.Session()  # type: ignore
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun_1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=DagRunState.RUNNING,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,  # type: ignore
        )
        session.merge(dagrun_1)

        task_instance_1 = TI(t_1, run_id=dagrun_1.run_id, state=ti_state_begin)
        task_instance_1.job_id = 123
        session.merge(task_instance_1)
        session.commit()

        dag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            session=session,
        )

        task_instances = session.query(TI).filter(TI.dag_id == dag_id).all()

        assert len(task_instances) == 1
        task_instance: TI = task_instances[0]
        assert task_instance.state == ti_state_end
        self._clean_up(dag_id)

    def test_next_dagrun_info_once(self):
        dag = DAG("test_scheduler_dagrun_once", start_date=timezone.datetime(2015, 1, 1), schedule="@once")

        next_info = dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2015, 1, 1)

        next_info = dag.next_dagrun_info(next_info.data_interval)
        assert next_info is None

    def test_next_dagrun_info_catchup(self):
        """
        Test to check that a DAG with catchup = False only schedules beginning now, not back to the start date
        """

        def make_dag(dag_id, schedule, start_date, catchup):
            default_args = {
                "owner": "airflow",
                "depends_on_past": False,
            }
            dag = DAG(
                dag_id,
                schedule=schedule,
                start_date=start_date,
                catchup=catchup,
                default_args=default_args,
            )

            op1 = EmptyOperator(task_id="t1", dag=dag)
            op2 = EmptyOperator(task_id="t2", dag=dag)
            op3 = EmptyOperator(task_id="t3", dag=dag)
            op1 >> op2 >> op3

            return dag

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(
            minute=0, second=0, microsecond=0
        )
        half_an_hour_ago = now - datetime.timedelta(minutes=30)
        two_hours_ago = now - datetime.timedelta(hours=2)

        dag1 = make_dag(
            dag_id="dag_without_catchup_ten_minute",
            schedule="*/10 * * * *",
            start_date=six_hours_ago_to_the_hour,
            catchup=False,
        )
        next_date, _ = dag1.next_dagrun_info(None)
        # The DR should be scheduled in the last half an hour, not 6 hours ago
        assert next_date > half_an_hour_ago
        assert next_date < timezone.utcnow()

        dag2 = make_dag(
            dag_id="dag_without_catchup_hourly",
            schedule="@hourly",
            start_date=six_hours_ago_to_the_hour,
            catchup=False,
        )

        next_date, _ = dag2.next_dagrun_info(None)
        # The DR should be scheduled in the last 2 hours, not 6 hours ago
        assert next_date > two_hours_ago
        # The DR should be scheduled BEFORE now
        assert next_date < timezone.utcnow()

        dag3 = make_dag(
            dag_id="dag_without_catchup_once",
            schedule="@once",
            start_date=six_hours_ago_to_the_hour,
            catchup=False,
        )

        next_date, _ = dag3.next_dagrun_info(None)
        # The DR should be scheduled in the last 2 hours, not 6 hours ago
        assert next_date == six_hours_ago_to_the_hour

    @time_machine.travel(timezone.datetime(2020, 1, 5))
    @pytest.mark.parametrize("schedule", ("@daily", timedelta(days=1), cron_timetable("0 0 * * *")))
    def test_next_dagrun_info_timedelta_schedule_and_catchup_false(self, schedule):
        """
        Test that the dag file processor does not create multiple dagruns
        if a dag is scheduled with 'timedelta' and catchup=False
        """
        dag = DAG(
            "test_scheduler_dagrun_once_with_timedelta_and_catchup_false",
            start_date=timezone.datetime(2015, 1, 1),
            schedule=schedule,
            catchup=False,
        )

        next_info = dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 1, 4)

        # The date to create is in the future, this is handled by "DagModel.dags_needing_dagruns"
        next_info = dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 1, 5)

    @time_machine.travel(timezone.datetime(2020, 5, 4))
    def test_next_dagrun_info_timedelta_schedule_and_catchup_true(self):
        """
        Test that the dag file processor creates multiple dagruns
        if a dag is scheduled with 'timedelta' and catchup=True
        """
        dag = DAG(
            "test_scheduler_dagrun_once_with_timedelta_and_catchup_true",
            start_date=timezone.datetime(2020, 5, 1),
            schedule=timedelta(days=1),
            catchup=True,
        )

        next_info = dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 1)

        next_info = dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 2)

        next_info = dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 3)

        # The date to create is in the future, this is handled by "DagModel.dags_needing_dagruns"
        next_info = dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 4)

    def test_next_dagrun_info_timetable_exception(self, caplog):
        """Test the DAG does not crash the scheduler if the timetable raises an exception."""

        class FailingTimetable(Timetable):
            def next_dagrun_info(self, last_automated_data_interval, restriction):
                raise RuntimeError("this fails")

        dag = DAG(
            "test_next_dagrun_info_timetable_exception",
            start_date=timezone.datetime(2020, 5, 1),
            schedule=FailingTimetable(),
            catchup=True,
        )

        def _check_logs(records: list[logging.LogRecord], data_interval: DataInterval) -> None:
            assert len(records) == 1
            record = records[0]
            assert record.exc_info is not None, "Should contain exception"
            assert record.getMessage() == (
                f"Failed to fetch run info after data interval {data_interval} "
                f"for DAG 'test_next_dagrun_info_timetable_exception'"
            )

        with caplog.at_level(level=logging.ERROR):
            next_info = dag.next_dagrun_info(None)
        assert next_info is None, "failed next_dagrun_info should return None"
        _check_logs(caplog.records, data_interval=None)
        caplog.clear()
        data_interval = DataInterval(timezone.datetime(2020, 5, 1), timezone.datetime(2020, 5, 2))
        with caplog.at_level(level=logging.ERROR):
            next_info = dag.next_dagrun_info(data_interval)
        assert next_info is None, "failed next_dagrun_info should return None"
        _check_logs(caplog.records, data_interval)

    def test_next_dagrun_after_auto_align(self):
        """
        Test if the schedule will be auto aligned with the start_date
        such that if the start_date coincides with the schedule the first
        execution_date will be start_date, otherwise it will be start_date +
        interval.
        """
        dag = DAG(
            dag_id="test_scheduler_auto_align_1",
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule="4 5 * * *",
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        next_info = dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2016, 1, 2, 5, 4)

        dag = DAG(
            dag_id="test_scheduler_auto_align_2",
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule="10 10 * * *",
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        next_info = dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2016, 1, 1, 10, 10)

    def test_next_dagrun_info_on_29_feb(self):
        dag = DAG(
            "test_scheduler_dagrun_29_feb", start_date=timezone.datetime(2024, 1, 1), schedule="0 0 29 2 *"
        )

        next_info = dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2024, 2, 29)

        next_info = dag.next_dagrun_info(next_info.data_interval)
        assert next_info.logical_date == timezone.datetime(2028, 2, 29)
        assert next_info.data_interval.start == timezone.datetime(2028, 2, 29)
        assert next_info.data_interval.end == timezone.datetime(2032, 2, 29)

    @pytest.mark.parametrize(
        "fab_version, perms, expected_exception, expected_perms",
        [
            pytest.param(
                "1.2.0",
                {
                    "role1": {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT},
                    "role3": {permissions.RESOURCE_DAG_RUN: {permissions.ACTION_CAN_CREATE}},
                    # will raise error in old FAB with new access control format
                },
                AirflowException,
                None,
                id="old_fab_new_access_control_format",
            ),
            pytest.param(
                "1.2.0",
                {
                    "role1": [
                        permissions.ACTION_CAN_READ,
                        permissions.ACTION_CAN_EDIT,
                        permissions.ACTION_CAN_READ,
                    ],
                },
                None,
                {"role1": {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT}},
                id="old_fab_old_access_control_format",
            ),
            pytest.param(
                "1.3.0",
                {
                    "role1": {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT},  # old format
                    "role3": {permissions.RESOURCE_DAG_RUN: {permissions.ACTION_CAN_CREATE}},  # new format
                },
                None,
                {
                    "role1": {
                        permissions.RESOURCE_DAG: {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT}
                    },
                    "role3": {permissions.RESOURCE_DAG_RUN: {permissions.ACTION_CAN_CREATE}},
                },
                id="new_fab_mixed_access_control_format",
            ),
        ],
    )
    def test_access_control_format(self, fab_version, perms, expected_exception, expected_perms):
        if expected_exception:
            with patch("airflow.providers.fab.__version__", fab_version):
                with pytest.raises(
                    expected_exception,
                    match="Please upgrade the FAB provider to a version >= 1.3.0 to allow use the Dag Level Access Control new format.",
                ):
                    DAG(dag_id="dag_test", schedule=None, access_control=perms)
        else:
            with patch("airflow.providers.fab.__version__", fab_version):
                dag = DAG(dag_id="dag_test", schedule=None, access_control=perms)
            assert dag.access_control == expected_perms

    def test_validate_executor_field_executor_not_configured(self):
        dag = DAG("test-dag", schedule=None)
        EmptyOperator(task_id="t1", dag=dag, executor="test.custom.executor")
        with pytest.raises(
            UnknownExecutorException,
            match="The specified executor test.custom.executor for task t1 is not configured",
        ):
            dag.validate()

    def test_validate_executor_field(self):
        with patch.object(ExecutorLoader, "lookup_executor_name_by_str"):
            dag = DAG("test-dag", schedule=None)
            EmptyOperator(task_id="t1", dag=dag, executor="test.custom.executor")
            dag.validate()

    def test_validate_params_on_trigger_dag(self):
        dag = DAG("dummy-dag", schedule=None, params={"param1": Param(type="string")})
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        with pytest.raises(ParamValidationError, match="No value passed and Param has no default value"):
            dag.create_dagrun(
                run_id="test_dagrun_missing_param",
                state=State.RUNNING,
                execution_date=TEST_DATE,
                data_interval=(TEST_DATE, TEST_DATE),
                **triggered_by_kwargs,
            )

        dag = DAG("dummy-dag", schedule=None, params={"param1": Param(type="string")})
        with pytest.raises(
            ParamValidationError, match="Invalid input for param param1: None is not of type 'string'"
        ):
            dag.create_dagrun(
                run_id="test_dagrun_missing_param",
                state=State.RUNNING,
                execution_date=TEST_DATE,
                conf={"param1": None},
                data_interval=(TEST_DATE, TEST_DATE),
                **triggered_by_kwargs,
            )

        dag = DAG("dummy-dag", schedule=None, params={"param1": Param(type="string")})
        dag.create_dagrun(
            run_id="test_dagrun_missing_param",
            state=State.RUNNING,
            execution_date=TEST_DATE,
            conf={"param1": "hello"},
            data_interval=(TEST_DATE, TEST_DATE),
            **triggered_by_kwargs,
        )

    def test_dag_owner_links(self):
        dag = DAG(
            "dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            owner_links={"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"},
        )

        assert dag.owner_links == {"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"}
        session = settings.Session()
        dag.sync_to_db(session=session)

        expected_owners = {"dag": {"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"}}
        orm_dag_owners = DagOwnerAttributes.get_all(session)
        assert orm_dag_owners == expected_owners

        # Test dag owner links are removed completely
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE)
        dag.sync_to_db(session=session)

        orm_dag_owners = session.query(DagOwnerAttributes).all()
        assert not orm_dag_owners

        # Check wrong formatted owner link
        with pytest.raises(AirflowException):
            DAG("dag", schedule=None, start_date=DEFAULT_DATE, owner_links={"owner1": "my-bad-link"})

    def test_continuous_schedule_linmits_max_active_runs(self):
        dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=1)
        assert isinstance(dag.timetable, ContinuousTimetable)
        assert dag.max_active_runs == 1

        dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=0)
        assert isinstance(dag.timetable, ContinuousTimetable)
        assert dag.max_active_runs == 0

        with pytest.raises(AirflowException):
            dag = DAG("continuous", start_date=DEFAULT_DATE, schedule="@continuous", max_active_runs=25)


class TestDagModel:
    def _clean(self):
        clear_db_dags()
        clear_db_assets()
        clear_db_runs()

    def setup_method(self):
        self._clean()

    def teardown_method(self):
        self._clean()

    def test_dags_needing_dagruns_not_too_early(self):
        dag = DAG(dag_id="far_future_dag", schedule=None, start_date=timezone.datetime(2038, 1, 1))
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            max_active_tasks=1,
            has_task_concurrency_limits=False,
            next_dagrun=dag.start_date,
            next_dagrun_create_after=timezone.datetime(2038, 1, 2),
            is_active=True,
        )
        session.add(orm_dag)
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_assets(self, dag_maker, session):
        asset = Asset(uri="hello")
        with dag_maker(
            session=session,
            dag_id="my_dag",
            max_active_runs=1,
            schedule=[asset],
            start_date=pendulum.now().add(days=-2),
        ) as dag:
            EmptyOperator(task_id="dummy")

        # there's no queue record yet, so no runs needed at this time.
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        # add queue records so we'll need a run
        dag_model = session.query(DagModel).filter(DagModel.dag_id == dag.dag_id).one()
        asset_model: AssetModel = dag_model.schedule_datasets[0]
        session.add(AssetDagRunQueue(dataset_id=asset_model.id, target_dag_id=dag_model.dag_id))
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == [dag_model]

        # create run so we don't need a run anymore (due to max active runs)
        dag_maker.create_dagrun(
            run_type=DagRunType.DATASET_TRIGGERED,
            state=DagRunState.QUEUED,
            execution_date=pendulum.now("UTC"),
        )
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        # increase max active runs and we should now need another run
        dag_maker.dag_model.max_active_runs = 2
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == [dag_model]

    def test_dags_needing_dagruns_asset_aliases(self, dag_maker, session):
        # link asset_alias hello_alias to asset hello
        asset_model = AssetModel(uri="hello")
        asset_alias_model = AssetAliasModel(name="hello_alias")
        asset_alias_model.datasets.append(asset_model)
        session.add_all([asset_model, asset_alias_model])
        session.commit()

        with dag_maker(
            session=session,
            dag_id="my_dag",
            max_active_runs=1,
            schedule=[AssetAlias(name="hello_alias")],
            start_date=pendulum.now().add(days=-2),
        ):
            EmptyOperator(task_id="dummy")

        # there's no queue record yet, so no runs needed at this time.
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        # add queue records so we'll need a run
        dag_model = dag_maker.dag_model
        asset_model: AssetModel = dag_model.schedule_datasets[0]
        session.add(AssetDagRunQueue(dataset_id=asset_model.id, target_dag_id=dag_model.dag_id))
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == [dag_model]

        # create run so we don't need a run anymore (due to max active runs)
        dag_maker.create_dagrun(
            run_type=DagRunType.DATASET_TRIGGERED,
            state=DagRunState.QUEUED,
            execution_date=pendulum.now("UTC"),
        )
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        # increase max active runs and we should now need another run
        dag_maker.dag_model.max_active_runs = 2
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == [dag_model]

    def test_max_active_runs_not_none(self):
        dag = DAG(
            dag_id="test_max_active_runs_not_none",
            schedule=None,
            start_date=timezone.datetime(2038, 1, 1),
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            has_task_concurrency_limits=False,
            next_dagrun=None,
            next_dagrun_create_after=None,
            is_active=True,
        )
        # assert max_active_runs updated
        assert orm_dag.max_active_runs == 16
        session.add(orm_dag)
        session.flush()
        assert orm_dag.max_active_runs is not None

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_only_unpaused(self):
        """
        We should never create dagruns for unpaused DAGs
        """
        dag = DAG(dag_id="test_dags", schedule=None, start_date=DEFAULT_DATE)
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            has_task_concurrency_limits=False,
            next_dagrun=DEFAULT_DATE,
            next_dagrun_create_after=DEFAULT_DATE + timedelta(days=1),
            is_active=True,
        )
        session.add(orm_dag)
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        needed = query.all()
        assert needed == [orm_dag]

        orm_dag.is_paused = True
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_doesnot_send_dagmodel_with_import_errors(self, session):
        """
        We check that has_import_error is false for dags
        being set to scheduler to create dagruns
        """
        dag = DAG(dag_id="test_dags", schedule=None, start_date=DEFAULT_DATE)
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        orm_dag = DagModel(
            dag_id=dag.dag_id,
            has_task_concurrency_limits=False,
            next_dagrun=DEFAULT_DATE,
            next_dagrun_create_after=DEFAULT_DATE + timedelta(days=1),
            is_active=True,
        )
        assert not orm_dag.has_import_errors
        session.add(orm_dag)
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        needed = query.all()
        assert needed == [orm_dag]
        orm_dag.has_import_errors = True
        session.merge(orm_dag)
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        needed = query.all()
        assert needed == []

    @pytest.mark.parametrize(
        ("fileloc", "expected_relative"),
        [
            (os.path.join(settings.DAGS_FOLDER, "a.py"), Path("a.py")),
            ("/tmp/foo.py", Path("/tmp/foo.py")),
        ],
    )
    def test_relative_fileloc(self, fileloc, expected_relative):
        dag = DAG(dag_id="test", schedule=None)
        dag.fileloc = fileloc

        assert dag.relative_fileloc == expected_relative

    @pytest.mark.parametrize(
        "reader_dags_folder", [settings.DAGS_FOLDER, str(repo_root / "airflow/example_dags")]
    )
    @pytest.mark.parametrize(
        ("fileloc", "expected_relative"),
        [
            (str(Path(settings.DAGS_FOLDER, "a.py")), Path("a.py")),
            ("/tmp/foo.py", Path("/tmp/foo.py")),
        ],
    )
    def test_relative_fileloc_serialized(
        self, fileloc, expected_relative, session, clear_dags, reader_dags_folder
    ):
        """
        The serialized dag model includes the dags folder as configured on the thing serializing
        the dag.  On the thing deserializing the dag, when determining relative fileloc,
        we should use the dags folder of the processor.  So even if the dags folder of
        the deserializer is different (meaning that the full path is no longer relative to
        the dags folder) then we should still get the relative fileloc as it existed on the
        serializer process.  When the full path is not relative to the configured dags folder,
        then relative fileloc should just be the full path.
        """
        dag = DAG(dag_id="test", schedule=None)
        dag.fileloc = fileloc
        sdm = SerializedDagModel(dag)
        session.add(sdm)
        session.commit()
        session.expunge_all()
        sdm = SerializedDagModel.get(dag.dag_id, session)
        dag = sdm.dag
        with conf_vars({("core", "dags_folder"): reader_dags_folder}):
            assert dag.relative_fileloc == expected_relative

    def test__processor_dags_folder(self, session):
        """Only populated after deserializtion"""
        dag = DAG(dag_id="test", schedule=None)
        dag.fileloc = "/abc/test.py"
        assert dag._processor_dags_folder is None
        sdm = SerializedDagModel(dag)
        assert sdm.dag._processor_dags_folder == settings.DAGS_FOLDER

    @pytest.mark.need_serialized_dag
    def test_dags_needing_dagruns_asset_triggered_dag_info_queued_times(self, session, dag_maker):
        asset1 = Asset(uri="ds1")
        asset2 = Asset(uri="ds2")

        for dag_id, asset in [("assets-1", asset1), ("assets-2", asset2)]:
            with dag_maker(dag_id=dag_id, start_date=timezone.utcnow(), session=session):
                EmptyOperator(task_id="task", outlets=[asset])
            dr = dag_maker.create_dagrun()

            asset_id = session.query(AssetModel.id).filter_by(uri=asset.uri).scalar()

            session.add(
                AssetEvent(
                    dataset_id=asset_id,
                    source_task_id="task",
                    source_dag_id=dr.dag_id,
                    source_run_id=dr.run_id,
                    source_map_index=-1,
                )
            )

        asset1_id = session.query(AssetModel.id).filter_by(uri=asset1.uri).scalar()
        asset2_id = session.query(AssetModel.id).filter_by(uri=asset2.uri).scalar()

        with dag_maker(dag_id="assets-consumer-multiple", schedule=[asset1, asset2]) as dag:
            pass

        session.flush()
        session.add_all(
            [
                AssetDagRunQueue(dataset_id=asset1_id, target_dag_id=dag.dag_id, created_at=DEFAULT_DATE),
                AssetDagRunQueue(
                    dataset_id=asset2_id,
                    target_dag_id=dag.dag_id,
                    created_at=DEFAULT_DATE + timedelta(hours=1),
                ),
            ]
        )
        session.flush()

        query, dataset_triggered_dag_info = DagModel.dags_needing_dagruns(session)
        assert 1 == len(dataset_triggered_dag_info)
        assert dag.dag_id in dataset_triggered_dag_info
        first_queued_time, last_queued_time = dataset_triggered_dag_info[dag.dag_id]
        assert first_queued_time == DEFAULT_DATE
        assert last_queued_time == DEFAULT_DATE + timedelta(hours=1)

    def test_asset_expression(self, session: Session) -> None:
        dag = DAG(
            dag_id="test_dag_asset_expression",
            schedule=AssetAny(
                Asset("s3://dag1/output_1.txt", {"hi": "bye"}),
                AssetAll(
                    Asset("s3://dag2/output_1.txt", {"hi": "bye"}),
                    Asset("s3://dag3/output_3.txt", {"hi": "bye"}),
                ),
                AssetAlias(name="test_name"),
            ),
            start_date=datetime.datetime.min,
        )
        DAG.bulk_write_to_db([dag], session=session)

        expression = session.scalars(select(DagModel.dataset_expression).filter_by(dag_id=dag.dag_id)).one()
        assert expression == {
            "any": [
                "s3://dag1/output_1.txt",
                {"all": ["s3://dag2/output_1.txt", "s3://dag3/output_3.txt"]},
                {"alias": "test_name"},
            ]
        }


class TestQueries:
    def setup_method(self) -> None:
        clear_db_runs()

    def teardown_method(self) -> None:
        clear_db_runs()

    @pytest.mark.parametrize("tasks_count", [3, 12])
    def test_count_number_queries(self, tasks_count):
        dag = DAG("test_dagrun_query_count", schedule=None, start_date=DEFAULT_DATE)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        for i in range(tasks_count):
            EmptyOperator(task_id=f"dummy_task_{i}", owner="test", dag=dag)
        with assert_queries_count(3):
            dag.create_dagrun(
                run_id="test_dagrun_query_count",
                state=State.RUNNING,
                execution_date=TEST_DATE,
                data_interval=(TEST_DATE, TEST_DATE),
                **triggered_by_kwargs,
            )


class TestDagDecorator:
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": timezone.utcnow(),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    DEFAULT_DATE = timezone.datetime(2016, 1, 1)
    VALUE = 42

    def setup_method(self):
        self.operator = None

    def teardown_method(self):
        clear_db_runs()

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
        argnames=["dag_doc_md", "expected_doc_md"],
        argvalues=[
            pytest.param("dag docs.", "dag docs.", id="use_dag_doc_md"),
            pytest.param(None, "Regular DAG documentation", id="use_dag_docstring"),
        ],
    )
    def test_documentation_added(self, dag_doc_md, expected_doc_md):
        """Test that @dag uses function docs as doc_md for DAG object if doc_md is not explicitly set."""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS, doc_md=dag_doc_md)
        def noop_pipeline():
            """Regular DAG documentation"""

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "noop_pipeline"
        assert dag.doc_md == expected_doc_md

    def test_documentation_template_rendered(self):
        """Test that @dag uses function docs as doc_md for DAG object"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline():
            """
            {% if True %}
               Regular DAG documentation
            {% endif %}
            """

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "noop_pipeline"
        assert "Regular DAG documentation" in dag.doc_md

    def test_resolve_documentation_template_file_not_rendered(self, tmp_path):
        """Test that @dag uses function docs as doc_md for DAG object"""

        raw_content = """
        {% if True %}
            External Markdown DAG documentation
        {% endif %}
        """

        path = tmp_path / "testfile.md"
        path.write_text(raw_content)

        @dag_decorator("test-dag", schedule=None, start_date=DEFAULT_DATE, doc_md=str(path))
        def markdown_docs(): ...

        dag = markdown_docs()
        assert isinstance(dag, DAG)
        assert dag.dag_id == "test-dag"
        assert dag.doc_md == raw_content

    def test_fails_if_arg_not_set(self):
        """Test that @dag decorated function fails if positional argument is not set"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def noop_pipeline(value):
            @task_decorator
            def return_num(num):
                return num

            return_num(value)

        # Test that if arg is not passed it raises a type error as expected.
        with pytest.raises(TypeError):
            noop_pipeline()

    def test_dag_param_resolves(self):
        """Test that dag param is correctly resolved by operator"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=self.VALUE):
            @task_decorator
            def return_num(num):
                return num

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator

        dag = xcom_pass_to_op()

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            data_interval=(self.DEFAULT_DATE, self.DEFAULT_DATE),
            state=State.RUNNING,
            **triggered_by_kwargs,
        )

        self.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == self.VALUE

    def test_dag_param_dagrun_parameterized(self):
        """Test that dag param is correctly overwritten when set in dag run"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=self.VALUE):
            @task_decorator
            def return_num(num):
                return num

            assert isinstance(value, DagParam)

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator

        dag = xcom_pass_to_op()
        new_value = 52
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            data_interval=(self.DEFAULT_DATE, self.DEFAULT_DATE),
            state=State.RUNNING,
            conf={"value": new_value},
            **triggered_by_kwargs,
        )

        self.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == new_value

    @pytest.mark.parametrize("value", [VALUE, 0])
    def test_set_params_for_dag(self, value):
        """Test that dag param is correctly set when using dag decorator"""

        @dag_decorator(schedule=None, default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=value):
            @task_decorator
            def return_num(num):
                return num

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator

        dag = xcom_pass_to_op()
        assert dag.params["value"] == value


@pytest.mark.parametrize(
    "run_id, execution_date",
    [
        (None, datetime_tz(2020, 1, 1)),
        ("test-run-id", None),
    ],
)
def test_set_task_instance_state(run_id, execution_date, session, dag_maker):
    """Test that set_task_instance_state updates the TaskInstance state and clear downstream failed"""

    start_date = datetime_tz(2020, 1, 1)
    with dag_maker("test_set_task_instance_state", start_date=start_date, session=session) as dag:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = EmptyOperator(task_id="task_2")
        task_3 = EmptyOperator(task_id="task_3")
        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")

        task_1 >> [task_2, task_3, task_4, task_5]

    dagrun = dag_maker.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.FAILED,
        run_type=DagRunType.SCHEDULED,
    )

    def get_ti_from_db(task):
        return (
            session.query(TI)
            .filter(
                TI.dag_id == dag.dag_id,
                TI.task_id == task.task_id,
                TI.run_id == dagrun.run_id,
            )
            .one()
        )

    get_ti_from_db(task_1).state = State.FAILED
    get_ti_from_db(task_2).state = State.SUCCESS
    get_ti_from_db(task_3).state = State.UPSTREAM_FAILED
    get_ti_from_db(task_4).state = State.FAILED
    get_ti_from_db(task_5).state = State.SKIPPED

    session.flush()

    altered = dag.set_task_instance_state(
        task_id=task_1.task_id,
        run_id=run_id,
        execution_date=execution_date,
        state=State.SUCCESS,
        session=session,
    )

    # After _mark_task_instance_state, task_1 is marked as SUCCESS
    ti1 = get_ti_from_db(task_1)
    assert ti1.state == State.SUCCESS
    # TIs should have DagRun pre-loaded
    assert isinstance(inspect(ti1).attrs.dag_run.loaded_value, DagRun)
    # task_2 remains as SUCCESS
    assert get_ti_from_db(task_2).state == State.SUCCESS
    # task_3 and task_4 are cleared because they were in FAILED/UPSTREAM_FAILED state
    assert get_ti_from_db(task_3).state == State.NONE
    assert get_ti_from_db(task_4).state == State.NONE
    # task_5 remains as SKIPPED
    assert get_ti_from_db(task_5).state == State.SKIPPED
    dagrun.refresh_from_db(session=session)
    # dagrun should be set to QUEUED
    assert dagrun.get_state() == State.QUEUED

    assert {tuple(t.key) for t in altered} == {
        ("test_set_task_instance_state", "task_1", dagrun.run_id, 0, -1)
    }


def test_set_task_instance_state_mapped(dag_maker, session):
    """Test that when setting an individual mapped TI that the other TIs are not affected"""
    task_id = "t1"

    with dag_maker(session=session) as dag:

        @dag.task
        def make_arg_lists():
            return [[1], [2], [{"a": "b"}]]

        def consumer(value):
            print(value)

        mapped = PythonOperator.partial(task_id=task_id, dag=dag, python_callable=consumer).expand(
            op_args=make_arg_lists()
        )

        mapped >> BaseOperator(task_id="downstream")

    dr1 = dag_maker.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.FAILED,
    )
    expand_mapped_task(mapped, dr1.run_id, "make_arg_lists", length=2, session=session)

    # set_state(future=True) only applies to scheduled runs
    dr2 = dag_maker.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.FAILED,
        execution_date=DEFAULT_DATE + datetime.timedelta(days=1),
    )
    expand_mapped_task(mapped, dr2.run_id, "make_arg_lists", length=2, session=session)

    session.query(TI).filter_by(dag_id=dag.dag_id).update({"state": TaskInstanceState.FAILED})

    ti_query = (
        session.query(TI.task_id, TI.map_index, TI.run_id, TI.state)
        .filter(TI.dag_id == dag.dag_id, TI.task_id.in_([task_id, "downstream"]))
        .order_by(TI.run_id, TI.task_id, TI.map_index)
    )

    # Check pre-conditions
    assert ti_query.all() == [
        ("downstream", -1, dr1.run_id, TaskInstanceState.FAILED),
        (task_id, 0, dr1.run_id, TaskInstanceState.FAILED),
        (task_id, 1, dr1.run_id, TaskInstanceState.FAILED),
        ("downstream", -1, dr2.run_id, TaskInstanceState.FAILED),
        (task_id, 0, dr2.run_id, TaskInstanceState.FAILED),
        (task_id, 1, dr2.run_id, TaskInstanceState.FAILED),
    ]

    dag.set_task_instance_state(
        task_id=task_id,
        map_indexes=[1],
        future=True,
        run_id=dr1.run_id,
        state=TaskInstanceState.SUCCESS,
        session=session,
    )
    assert dr1 in session, "Check session is passed down all the way"

    assert ti_query.all() == [
        ("downstream", -1, dr1.run_id, None),
        (task_id, 0, dr1.run_id, TaskInstanceState.FAILED),
        (task_id, 1, dr1.run_id, TaskInstanceState.SUCCESS),
        ("downstream", -1, dr2.run_id, None),
        (task_id, 0, dr2.run_id, TaskInstanceState.FAILED),
        (task_id, 1, dr2.run_id, TaskInstanceState.SUCCESS),
    ]


@pytest.mark.parametrize("run_id, execution_date", [(None, datetime_tz(2020, 1, 1)), ("test-run-id", None)])
def test_set_task_group_state(run_id, execution_date, session, dag_maker):
    """Test that set_task_group_state updates the TaskGroup state and clear downstream failed"""

    start_date = datetime_tz(2020, 1, 1)
    with dag_maker("test_set_task_group_state", start_date=start_date, session=session) as dag:
        start = EmptyOperator(task_id="start")

        with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
            task_1 = EmptyOperator(task_id="task_1")
            task_2 = EmptyOperator(task_id="task_2")
            task_3 = EmptyOperator(task_id="task_3")

            task_1 >> [task_2, task_3]

        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")
        task_6 = EmptyOperator(task_id="task_6")
        task_7 = EmptyOperator(task_id="task_7")
        task_8 = EmptyOperator(task_id="task_8")

        start >> section_1 >> [task_4, task_5, task_6, task_7, task_8]

    dagrun = dag_maker.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.FAILED,
        run_type=DagRunType.SCHEDULED,
    )

    def get_ti_from_db(task):
        return (
            session.query(TI)
            .filter(
                TI.dag_id == dag.dag_id,
                TI.task_id == task.task_id,
                TI.run_id == dagrun.run_id,
            )
            .one()
        )

    get_ti_from_db(task_1).state = State.FAILED
    get_ti_from_db(task_2).state = State.SUCCESS
    get_ti_from_db(task_3).state = State.UPSTREAM_FAILED
    get_ti_from_db(task_4).state = State.SUCCESS
    get_ti_from_db(task_5).state = State.UPSTREAM_FAILED
    get_ti_from_db(task_6).state = State.FAILED
    get_ti_from_db(task_7).state = State.SKIPPED

    session.flush()

    altered = dag.set_task_group_state(
        group_id=section_1.group_id,
        run_id=run_id,
        execution_date=execution_date,
        state=State.SUCCESS,
        session=session,
    )

    # After _mark_task_instance_state, task_1 is marked as SUCCESS
    assert get_ti_from_db(task_1).state == State.SUCCESS
    # task_2 remains as SUCCESS
    assert get_ti_from_db(task_2).state == State.SUCCESS
    # task_3 should be marked as SUCCESS
    assert get_ti_from_db(task_3).state == State.SUCCESS
    # task_4 should remain as SUCCESS
    assert get_ti_from_db(task_4).state == State.SUCCESS
    # task_5 and task_6 are cleared because they were in FAILED/UPSTREAM_FAILED state
    assert get_ti_from_db(task_5).state == State.NONE
    assert get_ti_from_db(task_6).state == State.NONE
    # task_7 remains as SKIPPED
    assert get_ti_from_db(task_7).state == State.SKIPPED
    dagrun.refresh_from_db(session=session)
    # dagrun should be set to QUEUED
    assert dagrun.get_state() == State.QUEUED

    assert {t.key for t in altered} == {
        ("test_set_task_group_state", "section_1.task_1", dagrun.run_id, 0, -1),
        ("test_set_task_group_state", "section_1.task_3", dagrun.run_id, 0, -1),
    }


def test_dag_teardowns_property_lists_all_teardown_tasks(dag_maker):
    @setup
    def setup_task():
        return 1

    @teardown
    def teardown_task():
        return 1

    @teardown
    def teardown_task2():
        return 1

    @teardown
    def teardown_task3():
        return 1

    @task_decorator
    def mytask():
        return 1

    with dag_maker() as dag:
        t1 = setup_task()
        t2 = teardown_task()
        t3 = teardown_task2()
        t4 = teardown_task3()
        with t1 >> t2:
            with t3:
                with t4:
                    mytask()

    assert {t.task_id for t in dag.teardowns} == {"teardown_task", "teardown_task2", "teardown_task3"}
    assert {t.task_id for t in dag.tasks_upstream_of_teardowns} == {"setup_task", "mytask"}


@pytest.mark.parametrize(
    "start_date, expected_infos",
    [
        (
            DEFAULT_DATE,
            [DagRunInfo.interval(DEFAULT_DATE, DEFAULT_DATE + datetime.timedelta(hours=1))],
        ),
        (
            DEFAULT_DATE - datetime.timedelta(hours=3),
            [
                DagRunInfo.interval(
                    DEFAULT_DATE - datetime.timedelta(hours=3),
                    DEFAULT_DATE - datetime.timedelta(hours=2),
                ),
                DagRunInfo.interval(
                    DEFAULT_DATE - datetime.timedelta(hours=2),
                    DEFAULT_DATE - datetime.timedelta(hours=1),
                ),
                DagRunInfo.interval(
                    DEFAULT_DATE - datetime.timedelta(hours=1),
                    DEFAULT_DATE,
                ),
                DagRunInfo.interval(
                    DEFAULT_DATE,
                    DEFAULT_DATE + datetime.timedelta(hours=1),
                ),
            ],
        ),
    ],
    ids=["in-dag-restriction", "out-of-dag-restriction"],
)
def test_iter_dagrun_infos_between(start_date, expected_infos):
    dag = DAG(dag_id="test_get_dates", start_date=DEFAULT_DATE, schedule="@hourly")
    EmptyOperator(task_id="dummy", dag=dag)

    iterator = dag.iter_dagrun_infos_between(
        earliest=pendulum.instance(start_date),
        latest=pendulum.instance(DEFAULT_DATE),
        align=True,
    )
    assert expected_infos == list(iterator)


def test_iter_dagrun_infos_between_error(caplog):
    start = pendulum.instance(DEFAULT_DATE - datetime.timedelta(hours=1))
    end = pendulum.instance(DEFAULT_DATE)

    class FailingAfterOneTimetable(Timetable):
        def next_dagrun_info(self, last_automated_data_interval, restriction):
            if last_automated_data_interval is None:
                return DagRunInfo.interval(start, end)
            raise RuntimeError("this fails")

    dag = DAG(
        dag_id="test_iter_dagrun_infos_between_error",
        start_date=DEFAULT_DATE,
        schedule=FailingAfterOneTimetable(),
    )

    iterator = dag.iter_dagrun_infos_between(earliest=start, latest=end, align=True)
    with caplog.at_level(logging.ERROR):
        infos = list(iterator)

    # The second timetable.next_dagrun_info() call raises an exception, so only the first result is returned.
    assert infos == [DagRunInfo.interval(start, end)]

    assert caplog.record_tuples == [
        (
            "airflow.models.dag.DAG",
            logging.ERROR,
            f"Failed to fetch run info after data interval {DataInterval(start, end)} for DAG {dag.dag_id!r}",
        ),
    ]
    assert caplog.records[0].exc_info is not None, "should contain exception context"


@pytest.mark.parametrize(
    "logical_date, data_interval_start, data_interval_end, expected_data_interval",
    [
        pytest.param(None, None, None, None, id="no-next-run"),
        pytest.param(
            DEFAULT_DATE,
            DEFAULT_DATE,
            DEFAULT_DATE + timedelta(days=2),
            DataInterval(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=2)),
            id="modern",
        ),
        pytest.param(
            DEFAULT_DATE,
            None,
            None,
            DataInterval(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=1)),
            id="legacy",
        ),
    ],
)
def test_get_next_data_interval(
    logical_date,
    data_interval_start,
    data_interval_end,
    expected_data_interval,
):
    dag = DAG(dag_id="test_get_next_data_interval", schedule="@daily", start_date=DEFAULT_DATE)
    dag_model = DagModel(
        dag_id="test_get_next_data_interval",
        next_dagrun=logical_date,
        next_dagrun_data_interval_start=data_interval_start,
        next_dagrun_data_interval_end=data_interval_end,
    )

    assert dag.get_next_data_interval(dag_model) == expected_data_interval


@pytest.mark.parametrize(
    ("dag_date", "tasks_date", "restrict"),
    [
        [
            (DEFAULT_DATE, None),
            [
                (DEFAULT_DATE + timedelta(days=1), DEFAULT_DATE + timedelta(days=2)),
                (DEFAULT_DATE + timedelta(days=3), DEFAULT_DATE + timedelta(days=4)),
            ],
            TimeRestriction(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=4), True),
        ],
        [
            (DEFAULT_DATE, None),
            [(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=1)), (DEFAULT_DATE, None)],
            TimeRestriction(DEFAULT_DATE, None, True),
        ],
    ],
)
def test__time_restriction(dag_maker, dag_date, tasks_date, restrict):
    with dag_maker(
        "test__time_restriction",
        schedule=None,
        start_date=dag_date[0],
        end_date=dag_date[1],
    ) as dag:
        EmptyOperator(task_id="do1", start_date=tasks_date[0][0], end_date=tasks_date[0][1])
        EmptyOperator(task_id="do2", start_date=tasks_date[1][0], end_date=tasks_date[1][1])

    assert dag._time_restriction == restrict


@pytest.mark.parametrize(
    "tags, should_pass",
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
        with pytest.raises(AirflowException):
            DAG("test-dag", schedule=None, tags=tags)


@pytest.mark.parametrize(
    "input_tags, expected_result",
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


@pytest.mark.need_serialized_dag
def test_get_asset_triggered_next_run_info(dag_maker, clear_assets):
    asset1 = Asset(uri="ds1")
    asset2 = Asset(uri="ds2")
    asset3 = Asset(uri="ds3")
    with dag_maker(dag_id="assets-1", schedule=[asset2]):
        pass
    dag1 = dag_maker.dag

    with dag_maker(dag_id="assets-2", schedule=[asset1, asset2]):
        pass
    dag2 = dag_maker.dag

    with dag_maker(dag_id="assets-3", schedule=[asset1, asset2, asset3]):
        pass
    dag3 = dag_maker.dag

    session = dag_maker.session
    asset1_id = session.query(AssetModel.id).filter_by(uri=asset1.uri).scalar()
    session.bulk_save_objects(
        [
            AssetDagRunQueue(dataset_id=asset1_id, target_dag_id=dag2.dag_id),
            AssetDagRunQueue(dataset_id=asset1_id, target_dag_id=dag3.dag_id),
        ]
    )
    session.flush()

    assets = session.query(AssetModel.uri).order_by(AssetModel.id).all()

    info = get_asset_triggered_next_run_info([dag1.dag_id], session=session)
    assert info[dag1.dag_id] == {
        "ready": 0,
        "total": 1,
        "uri": assets[0].uri,
    }

    # This time, check both dag2 and dag3 at the same time (tests filtering)
    info = get_asset_triggered_next_run_info([dag2.dag_id, dag3.dag_id], session=session)
    assert info[dag2.dag_id] == {
        "ready": 1,
        "total": 2,
        "uri": "",
    }
    assert info[dag3.dag_id] == {
        "ready": 1,
        "total": 3,
        "uri": "",
    }


@pytest.mark.need_serialized_dag
def test_get_dataset_triggered_next_run_info_with_unresolved_dataset_alias(dag_maker, clear_assets):
    dataset_alias1 = AssetAlias(name="alias")
    with dag_maker(dag_id="dag-1", schedule=[dataset_alias1]):
        pass
    dag1 = dag_maker.dag
    session = dag_maker.session
    session.flush()

    info = get_asset_triggered_next_run_info([dag1.dag_id], session=session)
    assert info == {}

    dag1_model = DagModel.get_dagmodel(dag1.dag_id)
    assert dag1_model.get_asset_triggered_next_run_info(session=session) is None


def test_dag_uses_timetable_for_run_id(session):
    class CustomRunIdTimetable(Timetable):
        def generate_run_id(self, *, run_type, logical_date, data_interval, **extra) -> str:
            return "abc"

    dag = DAG(dag_id="test", start_date=DEFAULT_DATE, schedule=CustomRunIdTimetable())
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

    dag_run = dag.create_dagrun(
        run_type=DagRunType.MANUAL,
        state=DagRunState.QUEUED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        **triggered_by_kwargs,
    )

    assert dag_run.run_id == "abc"


@pytest.mark.parametrize(
    "run_id_type",
    [DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED, DagRunType.DATASET_TRIGGERED],
)
def test_create_dagrun_disallow_manual_to_use_automated_run_id(run_id_type: DagRunType) -> None:
    dag = DAG(dag_id="test", start_date=DEFAULT_DATE, schedule="@daily")
    run_id = run_id_type.generate_run_id(DEFAULT_DATE)
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

    with pytest.raises(ValueError) as ctx:
        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            run_id=run_id,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=DagRunState.QUEUED,
            **triggered_by_kwargs,  # type: ignore
        )
    assert str(ctx.value) == (
        f"A manual DAG run cannot use ID {run_id!r} since it is reserved for {run_id_type.value} runs"
    )


def test_invalid_type_for_args():
    with pytest.raises(TypeError):
        DAG("invalid-default-args", schedule=None, max_consecutive_failed_dag_runs="not_an_int")


@mock.patch("airflow.models.dag.validate_instance_args")
def test_dag_init_validates_arg_types(mock_validate_instance_args):
    dag = DAG("dag_with_expected_args", schedule=None)

    mock_validate_instance_args.assert_called_once_with(dag, DAG_ARGS_EXPECTED_TYPES)


class TestTaskClearingSetupTeardownBehavior:
    """
    Task clearing behavior is mainly controlled by dag.partial_subset.
    Here we verify, primarily with regard to setups and teardowns, the
    behavior of dag.partial_subset but also the supporting methods defined
    on AbstractOperator.
    """

    @staticmethod
    def make_tasks(dag, input_str):
        """
        Helper for building setup and teardown tasks for testing.

        Given an input such as 's1, w1, t1, tf1', returns setup task "s1", normal task "w1"
        (the w means *work*), teardown task "t1", and teardown task "tf1" where the f means
        on_failure_fail_dagrun has been set to true.
        """

        def teardown_task(task_id):
            return BaseOperator(task_id=task_id).as_teardown()

        def teardown_task_f(task_id):
            return BaseOperator(task_id=task_id).as_teardown(on_failure_fail_dagrun=True)

        def work_task(task_id):
            return BaseOperator(task_id=task_id)

        def setup_task(task_id):
            return BaseOperator(task_id=task_id).as_setup()

        def make_task(task_id):
            """
            Task factory helper.

            Will give a setup, teardown, work, or teardown-with-dagrun-failure task depending on input.
            """
            if task_id.startswith("s"):
                factory = setup_task
            elif task_id.startswith("w"):
                factory = work_task
            elif task_id.startswith("tf"):
                factory = teardown_task_f
            elif task_id.startswith("t"):
                factory = teardown_task
            else:
                raise ValueError("unexpected")
            return dag.task_dict.get(task_id) or factory(task_id=task_id)

        return (make_task(x) for x in input_str.split(", "))

    @staticmethod
    def cleared_downstream(task):
        """Helper to return tasks that would be cleared if **downstream** selected."""
        upstream = False
        return set(
            task.dag.partial_subset(
                task_ids_or_regex=[task.task_id],
                include_downstream=not upstream,
                include_upstream=upstream,
            ).tasks
        )

    @staticmethod
    def cleared_upstream(task):
        """Helper to return tasks that would be cleared if **upstream** selected."""
        upstream = True
        return set(
            task.dag.partial_subset(
                task_ids_or_regex=task.task_id,
                include_downstream=not upstream,
                include_upstream=upstream,
            ).tasks
        )

    @staticmethod
    def cleared_neither(task):
        """Helper to return tasks that would be cleared if **upstream** selected."""
        return set(
            task.dag.partial_subset(
                task_ids_or_regex=[task.task_id],
                include_downstream=False,
                include_upstream=False,
            ).tasks
        )

    def test_get_flat_relative_ids_with_setup(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, w1, w2, w3, w4, t1 = self.make_tasks(dag, "s1, w1, w2, w3, w4, t1")

        s1 >> w1 >> w2 >> w3

        # w1 is downstream of s1, and s1 has no teardown, so clearing w1 clears s1
        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1}
        # same with w2 and w3
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s1}
        assert set(w3.get_upstreams_only_setups_and_teardowns()) == {s1}
        # so if we clear w2, we should also get s1, and w3, but not w1
        assert self.cleared_downstream(w2) == {s1, w2, w3}

        w3 >> t1

        # now, w2 has a downstream teardown, but it's not connected directly to s1
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s1}
        # so if we clear downstream then s1 will be cleared, and t1 will be cleared but only by virtue of
        # being downstream of w2 -- not as a result of being the teardown for s1, which it ain't
        assert self.cleared_downstream(w2) == {s1, w2, w3, t1}
        # and, another consequence of not linking s1 and t1 is that when we clear upstream, note that
        # t1 doesn't get cleared -- cus it's not upstream and it's not linked to s1
        assert self.cleared_upstream(w2) == {s1, w1, w2}
        # note also that if we add a 4th work task after t1, it will still be "in scope" for s1
        t1 >> w4
        assert self.cleared_downstream(w4) == {s1, w4}

        s1 >> t1

        # now, we know that t1 is the teardown for s1, so now we know that s1 will be "torn down"
        # by the time w4 runs, so we now know that w4 no longer requires s1, so when we clear w4,
        # s1 will not also be cleared
        self.cleared_downstream(w4) == {w4}
        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1, t1}
        assert self.cleared_downstream(w1) == {s1, w1, w2, w3, t1, w4}
        assert self.cleared_upstream(w1) == {s1, w1, t1}
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s1, t1}
        assert set(w2.get_upstreams_follow_setups()) == {s1, w1, t1}
        assert self.cleared_downstream(w2) == {s1, w2, w3, t1, w4}
        assert self.cleared_upstream(w2) == {s1, w1, w2, t1}
        assert self.cleared_downstream(w3) == {s1, w3, t1, w4}
        assert self.cleared_upstream(w3) == {s1, w1, w2, w3, t1}

    def test_get_flat_relative_ids_with_setup_nested_ctx_mgr(self):
        """Let's test some gnarlier cases here"""
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2 = self.make_tasks(dag, "s1, t1, s2, t2")
            with s1 >> t1:
                BaseOperator(task_id="w1")
                with s2 >> t2:
                    BaseOperator(task_id="w2")
                    BaseOperator(task_id="w3")
        # to_do: implement tests

    def test_get_flat_relative_ids_with_setup_nested_no_ctx_mgr(self):
        """Let's test some gnarlier cases here"""
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2, w3 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2, w3")
        s1 >> t1
        s1 >> w1 >> t1
        s1 >> s2
        s2 >> t2
        s2 >> w2 >> w3 >> t2

        assert w1.get_flat_relative_ids(upstream=True) == {"s1"}
        assert w1.get_flat_relative_ids(upstream=False) == {"t1"}
        assert self.cleared_downstream(w1) == {s1, w1, t1}
        assert self.cleared_upstream(w1) == {s1, w1, t1}
        assert w3.get_flat_relative_ids(upstream=True) == {"s1", "s2", "w2"}
        assert w3.get_flat_relative_ids(upstream=False) == {"t2"}
        assert t1 not in w2.get_flat_relatives(upstream=False)  # t1 not required by w2
        # t1 only included because s1 is upstream
        assert self.cleared_upstream(w2) == {s1, t1, s2, w2, t2}
        # t1 not included because t1 is not downstream
        assert self.cleared_downstream(w2) == {s2, w2, w3, t2}
        # t1 only included because s1 is upstream
        assert self.cleared_upstream(w3) == {s1, t1, s2, w2, w3, t2}
        # t1 not included because t1 is not downstream
        assert self.cleared_downstream(w3) == {s2, w3, t2}

    def test_get_flat_relative_ids_follows_teardowns(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, w1, w2, t1 = self.make_tasks(dag, "s1, w1, w2, t1")
        s1 >> w1 >> [w2, t1]
        s1 >> t1
        # w2, we infer, does not require s1, since t1 does not come after it
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == set()
        # w1, however, *does* require s1, since t1 is downstream of it
        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1, t1}
        # downstream is just downstream and includes teardowns
        assert self.cleared_downstream(w1) == {s1, w1, w2, t1}
        assert self.cleared_downstream(w2) == {w2}
        # and if there's a downstream setup, it will be included as well
        s2 = BaseOperator(task_id="s2", dag=dag).as_setup()
        t1 >> s2
        assert w1.get_flat_relative_ids(upstream=False) == {"t1", "w2", "s2"}
        assert self.cleared_downstream(w1) == {s1, w1, w2, t1, s2}

    def test_get_flat_relative_ids_two_tasks_diff_setup_teardowns(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2")
        s1 >> w1 >> [w2, t1]
        s1 >> t1
        s2 >> t2
        s2 >> w2 >> t2

        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1, t1}
        # s2 is included because w2 is included
        assert self.cleared_downstream(w1) == {s1, w1, t1, s2, w2, t2}
        assert self.cleared_neither(w1) == {s1, w1, t1}
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s2, t2}
        assert self.cleared_downstream(w2) == {s2, w2, t2}

    def test_get_flat_relative_ids_one_task_multiple_setup_teardowns(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1a, s1b, t1, s2, t2, s3, t3a, t3b, w1, w2 = self.make_tasks(
                dag, "s1a, s1b, t1, s2, t2, s3, t3a, t3b, w1, w2"
            )
        # teardown t1 has two setups, s1a and s1b
        [s1a, s1b] >> t1
        # work 1 requires s1a and s1b, both of which are torn down by t1
        [s1a, s1b] >> w1 >> [w2, t1]

        # work 2 requires s2, and s3. s2 is torn down by t2. s3 is torn down by two teardowns, t3a and t3b.
        s2 >> t2
        s2 >> w2 >> t2
        s3 >> w2 >> [t3a, t3b]
        s3 >> [t3a, t3b]
        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1a, s1b, t1}
        # since w2 is downstream of w1, w2 gets cleared.
        # and since w2 gets cleared, we should also see s2 and s3 in here
        assert self.cleared_downstream(w1) == {s1a, s1b, w1, t1, s3, t3a, t3b, w2, s2, t2}
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s2, t2, s3, t3a, t3b}
        assert self.cleared_downstream(w2) == {s2, s3, w2, t2, t3a, t3b}

    def test_get_flat_relative_ids_with_setup_and_groups(self):
        """This is a dag with a setup / teardown at dag level and two task groups that have
        their own setups / teardowns.

        When we do tg >> dag_teardown, teardowns should be excluded from tg leaves.
        """
        dag = DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now())
        with dag:
            dag_setup = BaseOperator(task_id="dag_setup").as_setup()
            dag_teardown = BaseOperator(task_id="dag_teardown").as_teardown()
            dag_setup >> dag_teardown
            for group_name in ("g1", "g2"):
                with TaskGroup(group_name) as tg:
                    group_setup = BaseOperator(task_id="group_setup").as_setup()
                    w1 = BaseOperator(task_id="w1")
                    w2 = BaseOperator(task_id="w2")
                    w3 = BaseOperator(task_id="w3")
                    group_teardown = BaseOperator(task_id="group_teardown").as_teardown()
                    group_setup >> w1 >> w2 >> w3 >> group_teardown
                    group_setup >> group_teardown
                dag_setup >> tg >> dag_teardown
        g2_w2 = dag.task_dict["g2.w2"]
        g2_w3 = dag.task_dict["g2.w3"]
        g2_group_teardown = dag.task_dict["g2.group_teardown"]

        # the line `dag_setup >> tg >> dag_teardown` should be equivalent to
        # dag_setup >> group_setup; w3 >> dag_teardown
        # i.e. not group_teardown >> dag_teardown
        # this way the two teardowns can run in parallel
        # so first, check that dag_teardown not downstream of group 2 teardown
        # this means they can run in parallel
        assert "dag_teardown" not in g2_group_teardown.downstream_task_ids
        # and just document that g2 teardown is in effect a dag leaf
        assert g2_group_teardown.downstream_task_ids == set()
        # group 2 task w3 is in the scope of 2 teardowns -- the dag teardown and the group teardown
        # it is arrowed to both of them
        assert g2_w3.downstream_task_ids == {"g2.group_teardown", "dag_teardown"}
        # dag teardown should have 3 upstreams: the last work task in groups 1 and 2, and its setup
        assert dag_teardown.upstream_task_ids == {"g1.w3", "g2.w3", "dag_setup"}

        assert {x.task_id for x in g2_w2.get_upstreams_only_setups_and_teardowns()} == {
            "dag_setup",
            "dag_teardown",
            "g2.group_setup",
            "g2.group_teardown",
        }

        # clearing g2.w2 clears all setups and teardowns and g2.w2 and g2.w2
        # but not anything from g1
        assert {x.task_id for x in self.cleared_downstream(g2_w2)} == {
            "dag_setup",
            "dag_teardown",
            "g2.group_setup",
            "g2.group_teardown",
            "g2.w3",
            "g2.w2",
        }
        assert {x.task_id for x in self.cleared_upstream(g2_w2)} == {
            "dag_setup",
            "dag_teardown",
            "g2.group_setup",
            "g2.group_teardown",
            "g2.w1",
            "g2.w2",
        }

    def test_clear_upstream_not_your_setup(self):
        """
        When you have a work task that comes after a setup, then if you clear upstream
        the setup (and its teardown) will be cleared even though strictly speaking you don't
        "require" it since, depending on speed of execution, it might be torn down by t1
        before / while w2 runs.  It just gets cleared by virtue of it being upstream, and
        that's what you requested.  And its teardown gets cleared too.  But w1 doesn't.
        """
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, w1, w2, t1 = self.make_tasks(dag, "s1, w1, w2, t1")
            s1 >> w1 >> t1.as_teardown(setups=s1)
            s1 >> w2
            # w2 is downstream of s1, so when clearing upstream, it should clear s1 (since it
            # is upstream of w2) and t1 since it's the teardown for s1 even though not downstream of w1
            assert self.cleared_upstream(w2) == {s1, w2, t1}

    def test_clearing_teardown_no_clear_setup(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, w1, t1 = self.make_tasks(dag, "s1, w1, t1")
            s1 >> t1
            # clearing t1 does not clear s1
            assert self.cleared_downstream(t1) == {t1}
            s1 >> w1 >> t1
            # that isn't changed with the introduction of w1
            assert self.cleared_downstream(t1) == {t1}
            # though, of course, clearing w1 clears them all
            assert self.cleared_downstream(w1) == {s1, w1, t1}

    def test_clearing_setup_clears_teardown(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, w1, t1 = self.make_tasks(dag, "s1, w1, t1")
            s1 >> t1
            s1 >> w1 >> t1
            # clearing w1 clears all always
            assert self.cleared_upstream(w1) == {s1, w1, t1}
            assert self.cleared_downstream(w1) == {s1, w1, t1}
            assert self.cleared_neither(w1) == {s1, w1, t1}
            # clearing s1 clears t1 always
            assert self.cleared_upstream(s1) == {s1, t1}
            assert self.cleared_downstream(s1) == {s1, w1, t1}
            assert self.cleared_neither(s1) == {s1, t1}

    @pytest.mark.parametrize(
        "upstream, downstream, expected",
        [
            (False, False, {"my_teardown", "my_setup"}),
            (False, True, {"my_setup", "my_work", "my_teardown"}),
            (True, False, {"my_teardown", "my_setup"}),
            (True, True, {"my_setup", "my_work", "my_teardown"}),
        ],
    )
    def test_clearing_setup_clears_teardown_taskflow(self, upstream, downstream, expected):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:

            @setup
            def my_setup(): ...

            @task_decorator
            def my_work(): ...

            @teardown
            def my_teardown(): ...

            s1 = my_setup()
            w1 = my_work()
            t1 = my_teardown()
            s1 >> w1 >> t1
            s1 >> t1
        assert {
            x.task_id
            for x in dag.partial_subset(
                "my_setup", include_upstream=upstream, include_downstream=downstream
            ).tasks
        } == expected

    def test_get_flat_relative_ids_two_tasks_diff_setup_teardowns_deeper(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2, s3, w3, t3 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2, s3, w3, t3")
        s1 >> w1 >> t1
        s1 >> t1
        w1 >> w2

        # with the below, s2 is not downstream of w1, but it's the setup for w2
        # so it should be cleared when w1 is cleared
        s2 >> w2 >> t2
        s2 >> t2

        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1, t1}
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s2, t2}
        assert self.cleared_downstream(w1) == {s1, w1, t1, s2, w2, t2}
        assert self.cleared_downstream(w2) == {s2, w2, t2}

        # now, what if s2 itself has a setup and teardown?
        s3 >> s2 >> t3
        s3 >> t3
        # note that s3 is excluded because it's assumed that a setup won't have a setup
        # so, we don't continue to recurse for setups after reaching the setups for
        # the downstream work tasks
        # but, t3 is included since it's a teardown for s2
        assert self.cleared_downstream(w1) == {s1, w1, t1, s2, w2, t2, t3}

    def test_clearing_behavior_multiple_setups_for_work_task(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2, s3, w3, t3 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2, s3, w3, t3")
        s1 >> t1
        s2 >> t2
        s3 >> t3
        s1 >> s2 >> s3 >> w1 >> w2 >> [t1, t2, t3]

        assert self.cleared_downstream(w1) == {s1, s2, s3, w1, w2, t1, t2, t3}
        assert self.cleared_downstream(w2) == {s1, s2, s3, w2, t1, t2, t3}
        assert self.cleared_downstream(s3) == {s1, s2, s3, w1, w2, t1, t2, t3}
        # even if we don't include upstream / downstream, setups and teardowns are cleared
        assert self.cleared_neither(w2) == {s3, t3, s2, t2, s1, t1, w2}
        assert self.cleared_neither(w1) == {s3, t3, s2, t2, s1, t1, w1}
        # but, a setup doesn't formally have a setup, so if we only clear s3, say then its upstream setups
        # are not also cleared
        assert self.cleared_neither(s3) == {s3, t3}
        assert self.cleared_neither(s2) == {s2, t2}

    def test_clearing_behavior_multiple_setups_for_work_task2(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2, s3, w3, t3 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2, s3, w3, t3")
        s1 >> t1
        s2 >> t2
        s3 >> t3
        [s1, s2, s3] >> w1 >> w2 >> [t1, t2, t3]

        assert self.cleared_downstream(w1) == {s1, s2, s3, w1, w2, t1, t2, t3}
        assert self.cleared_downstream(w2) == {s1, s2, s3, w2, t1, t2, t3}

    def test_clearing_behavior_more_tertiary_weirdness(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2, s3, t3 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2, s3, t3")
        s1 >> t1
        s2 >> t2
        s1 >> w1 >> s2 >> w2 >> [t1, t2]
        s2 >> w2 >> t2
        s3 >> s2 >> t3
        s3 >> t3

        def sort(task_list):
            return sorted(x.task_id for x in task_list)

        assert set(w1.get_upstreams_only_setups_and_teardowns()) == {s1, t1}
        # s2 is included because w2 is included
        assert self.cleared_downstream(w1) == {s1, w1, t1, s2, w2, t2, t3}
        assert self.cleared_downstream(w2) == {s1, t1, s2, w2, t2, t3}
        # t3 is included since s2 is included and s2 >> t3
        # but s3 not included because it's assumed that a setup doesn't have a setup
        assert self.cleared_neither(w2) == {s1, w2, t1, s2, t2, t3}

        # since we're clearing upstream, s3 is upstream of w2, so s3 and t3 are included
        # even though w2 doesn't require them
        # s2 and t2 are included for obvious reasons, namely that w2 requires s2
        # and s1 and t1 are included for the same reason
        # w1 included since it is upstream of w2
        assert sort(self.cleared_upstream(w2)) == sort({s1, t1, s2, t2, s3, t3, w1, w2})

        # t3 is included here since it's a teardown for s2
        assert set(w2.get_upstreams_only_setups_and_teardowns()) == {s2, t2, s1, t1, t3}

    def test_clearing_behavior_more_tertiary_weirdness2(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1, s2, t2, w1, w2, s3, t3 = self.make_tasks(dag, "s1, t1, s2, t2, w1, w2, s3, t3")
        s1 >> t1
        s2 >> t2
        s1 >> w1 >> t1
        s2 >> t1 >> t2

        def sort(task_list):
            return sorted(x.task_id for x in task_list)

        # t2 included since downstream, but s2 not included since it's not required by t2
        # and clearing teardown does not clear the setup
        assert self.cleared_downstream(w1) == {s1, w1, t1, t2}

        # even though t1 is cleared here, s2 and t2 are not "setup and teardown" for t1
        # so they are not included
        assert self.cleared_neither(w1) == {s1, w1, t1}
        assert self.cleared_upstream(w1) == {s1, w1, t1}

        # t1 does not have a setup or teardown
        # but t2 is downstream so it's included
        # and s2 is not included since clearing teardown does not clear the setup
        assert self.cleared_downstream(t1) == {t1, t2}
        # t1 does not have a setup or teardown
        assert self.cleared_neither(t1) == {t1}
        # s2 included since upstream, and t2 included since s2 included
        assert self.cleared_upstream(t1) == {s1, t1, s2, t2, w1}

    def test_clearing_behavior_just_teardown(self):
        with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.now()) as dag:
            s1, t1 = self.make_tasks(dag, "s1, t1")
        s1 >> t1
        assert set(t1.get_upstreams_only_setups_and_teardowns()) == set()
        assert self.cleared_upstream(t1) == {s1, t1}
        assert self.cleared_downstream(t1) == {t1}
        assert self.cleared_neither(t1) == {t1}
        assert set(s1.get_upstreams_only_setups_and_teardowns()) == set()
        assert self.cleared_upstream(s1) == {s1, t1}
        assert self.cleared_downstream(s1) == {s1, t1}
        assert self.cleared_neither(s1) == {s1, t1}

    def test_validate_setup_teardown_trigger_rule(self):
        with DAG(
            dag_id="direct_setup_trigger_rule", start_date=pendulum.now(), schedule=None, catchup=False
        ) as dag:
            s1, w1 = self.make_tasks(dag, "s1, w1")
            s1 >> w1
            dag.validate_setup_teardown()
            w1.trigger_rule = TriggerRule.ONE_FAILED
            with pytest.raises(
                Exception, match="Setup tasks must be followed with trigger rule ALL_SUCCESS."
            ):
                dag.validate_setup_teardown()
