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
import logging
import os
import pickle
import re
from contextlib import nullcontext
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import ANY, patch

import jinja2
import pendulum
import pytest
import time_machine
from sqlalchemy import delete, inspect, select, update

from airflow import settings
from airflow._shared.module_loading import qualname
from airflow._shared.timezones import timezone
from airflow._shared.timezones.timezone import datetime as datetime_tz
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import AirflowException
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    TaskOutletAssetReference,
)
from airflow.models.dag import (
    DagModel,
    DagOwnerAttributes,
    DagTag,
    get_asset_triggered_next_run_info,
    get_next_data_interval,
    get_run_data_interval,
)
from airflow.models.dagbag import DBDagBag
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseOperator, TaskGroup, setup, task as task_decorator, teardown
from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext
from airflow.sdk.definitions._internal.templater import NativeEnvironment, SandboxedEnvironment
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.sdk.definitions.param import Param
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.serialization.encoders import coerce_to_core_timetable
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.simple import (
    AssetTriggeredTimetable,
    NullTimetable,
    OnceTimetable,
)
from airflow.utils.file import list_py_file_paths
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_teams,
)
from tests_common.test_utils.mapping import expand_mapped_task
from tests_common.test_utils.mock_plugins import mock_plugin_manager
from tests_common.test_utils.taskinstance import run_task_instance
from tests_common.test_utils.timetables import cron_timetable, delta_timetable
from unit.models import DEFAULT_DATE
from unit.plugins.priority_weight_strategy import (
    FactorPriorityWeightStrategy,
    StaticTestPriorityWeightStrategy,
    TestPriorityWeightStrategyPlugin,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

TEST_DATE = datetime_tz(2015, 1, 2, 0, 0)

repo_root = Path(__file__).parents[2]


async def empty_callback_for_deadline():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


@pytest.fixture
def clear_dags():
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_dag_bundles()
    yield
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_dag_bundles()


@pytest.fixture
def clear_assets():
    clear_db_assets()
    yield
    clear_db_assets()


TEST_DAGS_FOLDER = Path(__file__).parents[1] / "dags"


@pytest.fixture
def test_dags_bundle(configure_testing_dag_bundle):
    with configure_testing_dag_bundle(TEST_DAGS_FOLDER):
        yield


def _create_dagrun(
    dag: DAG,
    *,
    logical_date: datetime.datetime,
    data_interval: tuple[datetime.datetime, datetime.datetime],
    run_type: DagRunType,
    state: DagRunState = DagRunState.RUNNING,
    start_date: datetime.datetime | None = None,
    **kwargs,
) -> DagRun:
    logical_date = timezone.coerce_datetime(logical_date)
    if not isinstance(data_interval, DataInterval):
        data_interval = DataInterval(*map(timezone.coerce_datetime, data_interval))
    scheduler_dag = sync_dag_to_db(dag)
    run_id = scheduler_dag.timetable.generate_run_id(
        run_type=run_type,
        run_after=logical_date or data_interval.end,
        data_interval=data_interval,
    )
    return scheduler_dag.create_dagrun(
        run_id=run_id,
        logical_date=logical_date,
        data_interval=data_interval,
        run_after=data_interval.end,
        run_type=run_type,
        state=state,
        start_date=start_date,
        triggered_by=DagRunTriggeredByType.TEST,
        **kwargs,
    )


class TestDag:
    def setup_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_assets()

    @conf_vars({("core", "load_examples"): "false"})
    def test_dag_test_auto_parses_when_not_serialized(self, test_dags_bundle, session):
        """
        DAG.test() should auto-parse and sync the DAG if it's not serialized yet.
        """

        dag_id = "test_example_bash_operator"

        dagbag = DagBag(dag_folder=os.fspath(TEST_DAGS_FOLDER), include_examples=False)
        dag = dagbag.dags.get(dag_id)

        # Ensure not serialized yet
        assert DBDagBag().get_latest_version_of_dag(dag_id, session=session) is None
        assert session.scalar(select(DagRun).where(DagRun.dag_id == dag_id)) is None

        dr = dag.test()
        assert dr is not None

        # Serialized DAG should now exist and DagRun would be created
        ser = DBDagBag().get_latest_version_of_dag(dag_id, session=session)
        assert ser is not None
        assert session.scalar(select(DagRun).where(DagRun.dag_id == dag_id)) is not None

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_assets()

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

    @pytest.mark.parametrize(
        ("cls", "expected"),
        [
            (StaticTestPriorityWeightStrategy, 99),
            (FactorPriorityWeightStrategy, 3),
        ],
    )
    def test_dag_task_custom_weight_strategy(self, cls, expected):
        with (
            mock_plugin_manager(plugins=[TestPriorityWeightStrategyPlugin]),
            DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"}) as dag,
        ):
            task = EmptyOperator(
                task_id="empty_task",
                weight_rule=cls(),
            )
        dr = _create_dagrun(
            dag,
            logical_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            run_type=DagRunType.MANUAL,
        )
        ti = dr.get_task_instance(task.task_id)
        assert ti.priority_weight == expected

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
        ("use_native_obj", "force_sandboxed", "expected_env"),
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

    def test_create_dagrun_when_schedule_is_none_and_empty_start_date(self, testing_dag_bundle):
        # Check that we don't get an AttributeError 'start_date' for self.start_date when schedule is none
        dag = DAG("dag_with_none_schedule_and_empty_start_date", schedule=None)
        dag.add_task(BaseOperator(task_id="task_without_start_date"))
        scheduler_dag = sync_dag_to_db(dag)
        dagrun = scheduler_dag.create_dagrun(
            run_id="test",
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
            logical_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            run_after=DEFAULT_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        assert dagrun is not None

    def test_dagtag_repr(self, testing_dag_bundle):
        clear_db_dags()
        dag = DAG("dag-test-dagtag", schedule=None, start_date=DEFAULT_DATE, tags=["tag-1", "tag-2"])
        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()

        sync_dag_to_db(dag)
        with create_session() as session:
            assert {"tag-1", "tag-2"} == {
                repr(t)
                for t in session.scalars(select(DagTag).where(DagTag.dag_id == "dag-test-dagtag")).all()
            }

    def test_bulk_write_to_db(self, testing_dag_bundle):
        clear_db_dags()
        dags = [
            create_scheduler_dag(
                DAG(f"dag-bulk-sync-{i}", schedule=None, start_date=DEFAULT_DATE, tags=["test-dag"])
            )
            for i in range(4)
        ]

        with assert_queries_count(6):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.execute(select(DagModel.dag_id)).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag"),
                ("dag-bulk-sync-1", "test-dag"),
                ("dag-bulk-sync-2", "test-dag"),
                ("dag-bulk-sync-3", "test-dag"),
            } == set(session.execute(select(DagTag.dag_id, DagTag.name)).all())

            for row in session.execute(select(DagModel.last_parsed_time)).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(9):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with assert_queries_count(9):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        # Adding tags
        for dag in dags:
            dag.tags.add("test-dag2")
        with assert_queries_count(10):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.execute(select(DagModel.dag_id)).all()
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
            } == set(session.execute(select(DagTag.dag_id, DagTag.name)).all())
        # Removing tags
        for dag in dags:
            dag.tags.remove("test-dag")
        with assert_queries_count(10):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.execute(select(DagModel.dag_id)).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag2"),
                ("dag-bulk-sync-1", "test-dag2"),
                ("dag-bulk-sync-2", "test-dag2"),
                ("dag-bulk-sync-3", "test-dag2"),
            } == set(session.execute(select(DagTag.dag_id, DagTag.name)).all())

            for row in session.execute(select(DagModel.last_parsed_time)).all():
                assert row[0] is not None

        # Removing all tags
        for dag in dags:
            dag.tags = set()
        with assert_queries_count(10):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.execute(select(DagModel.dag_id)).all()
            }
            assert not set(session.execute(select(DagTag.dag_id, DagTag.name)).all())

            for row in session.execute(select(DagModel.last_parsed_time)).all():
                assert row[0] is not None

    def test_bulk_write_to_db_single_dag(self, testing_dag_bundle):
        """
        Test bulk_write_to_db for a single dag using the index optimized query
        """
        clear_db_dags()
        dags = [
            create_scheduler_dag(
                DAG(f"dag-bulk-sync-{i}", schedule=None, start_date=DEFAULT_DATE, tags=["test-dag"])
            )
            for i in range(1)
        ]

        with assert_queries_count(6):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0"} == {row[0] for row in session.execute(select(DagModel.dag_id)).all()}
            assert {
                ("dag-bulk-sync-0", "test-dag"),
            } == set(session.execute(select(DagTag.dag_id, DagTag.name)).all())

            for row in session.execute(select(DagModel.last_parsed_time)).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(8):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with assert_queries_count(8):
            SerializedDAG.bulk_write_to_db("testing", None, dags)

    def test_bulk_write_to_db_multiple_dags(self, testing_dag_bundle):
        """
        Test bulk_write_to_db for multiple dags which does not use the index optimized query
        """
        clear_db_dags()
        dags = [
            create_scheduler_dag(
                DAG(f"dag-bulk-sync-{i}", schedule=None, start_date=DEFAULT_DATE, tags=["test-dag"])
            )
            for i in range(4)
        ]

        with assert_queries_count(6):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with create_session() as session:
            assert {"dag-bulk-sync-0", "dag-bulk-sync-1", "dag-bulk-sync-2", "dag-bulk-sync-3"} == {
                row[0] for row in session.execute(select(DagModel.dag_id)).all()
            }
            assert {
                ("dag-bulk-sync-0", "test-dag"),
                ("dag-bulk-sync-1", "test-dag"),
                ("dag-bulk-sync-2", "test-dag"),
                ("dag-bulk-sync-3", "test-dag"),
            } == set(session.execute(select(DagTag.dag_id, DagTag.name)).all())

            for row in session.execute(select(DagModel.last_parsed_time)).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(9):
            SerializedDAG.bulk_write_to_db("testing", None, dags)
        with assert_queries_count(9):
            SerializedDAG.bulk_write_to_db("testing", None, dags)

    @pytest.mark.parametrize("interval", [None, "@daily"])
    def test_bulk_write_to_db_interval_save_runtime(self, testing_dag_bundle, interval):
        mock_active_runs_of_dags = mock.MagicMock(side_effect=DagRun.active_runs_of_dags)
        with mock.patch.object(DagRun, "active_runs_of_dags", mock_active_runs_of_dags):
            dags_null_timetable = [
                create_scheduler_dag(DAG("dag-interval-None", schedule=None, start_date=TEST_DATE)),
                create_scheduler_dag(DAG("dag-interval-test", schedule=interval, start_date=TEST_DATE)),
            ]
            SerializedDAG.bulk_write_to_db("testing", None, dags_null_timetable)
            if interval:
                mock_active_runs_of_dags.assert_called_once()
            else:
                mock_active_runs_of_dags.assert_not_called()

    @pytest.mark.parametrize(
        ("state", "catchup", "expected_next_dagrun"),
        [
            # With catchup=True, next_dagrun is the start date
            (DagRunState.RUNNING, True, DEFAULT_DATE),
            (DagRunState.QUEUED, True, DEFAULT_DATE),
            # With catchup=False, next_dagrun is based on the current date
            (DagRunState.RUNNING, False, None),  # None means we'll use current date
            (DagRunState.QUEUED, False, None),
        ],
    )
    def test_bulk_write_to_db_max_active_runs(self, testing_dag_bundle, state, catchup, expected_next_dagrun):
        """
        Test that DagModel.next_dagrun_create_after is set to NULL when the dag cannot be created due to max
        active runs being hit. Tests both catchup=True and catchup=False scenarios.
        """
        dag = DAG(
            dag_id="test_scheduler_verify_max_active_runs",
            schedule=timedelta(days=1),
            start_date=DEFAULT_DATE,
            catchup=catchup,
        )
        dag.max_active_runs = 1
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        scheduler_dag = sync_dag_to_db(dag)
        scheduler_dag.clear()

        session = settings.Session()
        model = session.get(DagModel, dag.dag_id)

        if expected_next_dagrun is None:
            # For catchup=False, next_dagrun will be around the current date (not DEFAULT_DATE)
            # Instead of comparing exact dates, verify it's relatively recent and not the old start date
            current_time = timezone.utcnow()

            # Verify it's not using the old DEFAULT_DATE from 2016 and is after
            # that since we are picking up present date.
            assert model.next_dagrun.year >= DEFAULT_DATE.year
            assert model.next_dagrun.month >= DEFAULT_DATE.month

            # Verify the date is within a reasonable range of the current date
            # (allowing for timezone differences and scheduling details)
            assert abs((model.next_dagrun - current_time).days) <= 31  # Within the current month

            # Most importantly, verify it's not the default date
            assert model.next_dagrun != DEFAULT_DATE

            # Verify next_dagrun_create_after is scheduled after next_dagrun
            assert model.next_dagrun_create_after > model.next_dagrun
        else:
            # For catchup=True, next_dagrun is the start date
            assert model.next_dagrun == expected_next_dagrun
            assert model.next_dagrun_create_after == expected_next_dagrun + timedelta(days=1)

        dr = scheduler_dag.create_dagrun(
            run_id="test",
            state=state,
            logical_date=model.next_dagrun,
            run_type=DagRunType.SCHEDULED,
            session=session,
            data_interval=(model.next_dagrun, model.next_dagrun),
            run_after=model.next_dagrun_create_after,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        assert dr is not None
        SerializedDAG.bulk_write_to_db("testing", None, [dag])

        model = session.get(DagModel, dag.dag_id)
        # We signal "at max active runs" by saying this run is never eligible to be created
        assert model.next_dagrun_create_after is None
        # test that bulk_write_to_db again doesn't update next_dagrun_create_after
        SerializedDAG.bulk_write_to_db("testing", None, [dag])
        model = session.get(DagModel, dag.dag_id)
        assert model.next_dagrun_create_after is None

    def test_bulk_write_to_db_has_import_error(self, testing_dag_bundle):
        """
        Test that DagModel.has_import_error is set to false if no import errors.
        """
        dag = DAG(dag_id="test_has_import_error", schedule=None, start_date=DEFAULT_DATE)
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        scheduler_dag = sync_dag_to_db(dag, session=session)
        scheduler_dag.clear()

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
        SerializedDAG.bulk_write_to_db("testing", None, [dag])

        model = session.get(DagModel, dag.dag_id)
        # assert that has_import_error is now false
        assert not model.has_import_errors
        session.close()

    def test_bulk_write_to_db_assets(self, testing_dag_bundle):
        """
        Ensure that assets referenced in a dag are correctly loaded into the database.
        """
        dag_id1 = "test_asset_dag1"
        dag_id2 = "test_asset_dag2"

        task_id = "test_asset_task"

        uri1 = "s3://asset/1"
        a1 = Asset(uri=uri1, name="test_asset_1", extra={"not": "used"}, group="test-group")
        a2 = Asset(uri="s3://asset/2", name="test_asset_2", group="test-group")
        a3 = Asset(uri="s3://asset/3", name="test_asset-3", group="test-group")

        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=[a1])
        EmptyOperator(task_id=task_id, dag=dag1, outlets=[a2, a3])

        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(
            task_id=task_id,
            dag=dag2,
            outlets=[Asset(uri=uri1, name="test_asset_1", extra={"should": "be used"}, group="test-group")],
        )

        session = settings.Session()
        create_scheduler_dag(dag1).clear()
        SerializedDAG.bulk_write_to_db("testing", None, [dag1, dag2], session=session)
        session.commit()
        stored_assets = {x.uri: x for x in session.scalars(select(AssetModel)).all()}
        asset1_orm = stored_assets[a1.uri]
        asset2_orm = stored_assets[a2.uri]
        asset3_orm = stored_assets[a3.uri]
        assert stored_assets[uri1].extra == {"should": "be used"}
        assert [x.dag_id for x in asset1_orm.scheduled_dags] == [dag_id1]
        assert [(x.task_id, x.dag_id) for x in asset1_orm.producing_tasks] == [(task_id, dag_id2)]
        assert set(
            session.execute(
                select(
                    TaskOutletAssetReference.task_id,
                    TaskOutletAssetReference.dag_id,
                    TaskOutletAssetReference.asset_id,
                ).where(TaskOutletAssetReference.dag_id.in_((dag_id1, dag_id2)))
            ).all()
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
        EmptyOperator(task_id=task_id, dag=dag1, outlets=[a2])
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, schedule=None)
        EmptyOperator(task_id=task_id, dag=dag2)

        SerializedDAG.bulk_write_to_db("testing", None, [dag1, dag2], session=session)
        session.commit()
        session.expunge_all()
        stored_assets = {x.uri: x for x in session.scalars(select(AssetModel)).all()}
        asset1_orm = stored_assets[a1.uri]
        asset2_orm = stored_assets[a2.uri]
        assert [x.dag_id for x in asset1_orm.scheduled_dags] == []
        assert set(
            session.execute(
                select(
                    TaskOutletAssetReference.task_id,
                    TaskOutletAssetReference.dag_id,
                    TaskOutletAssetReference.asset_id,
                ).where(TaskOutletAssetReference.dag_id.in_((dag_id1, dag_id2)))
            ).all()
        ) == {(task_id, dag_id1, asset2_orm.id)}

    def test_bulk_write_to_db_asset_aliases(self, testing_dag_bundle):
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
        SerializedDAG.bulk_write_to_db("testing", None, [dag1, dag2], session=session)
        session.commit()

        stored_asset_alias_models = {x.name: x for x in session.scalars(select(AssetAliasModel)).all()}
        asset_alias_1_orm = stored_asset_alias_models[asset_alias_1.name]
        asset_alias_2_orm = stored_asset_alias_models[asset_alias_2.name]
        asset_alias_3_orm = stored_asset_alias_models[asset_alias_3.name]
        assert asset_alias_1_orm.name == "asset_alias_1"
        assert asset_alias_2_orm.name == "asset_alias_2"
        assert asset_alias_3_orm.name == "asset_alias_3"
        assert len(stored_asset_alias_models) == 3

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

    def test_existing_dag_is_paused_after_limit(self, testing_dag_bundle):
        def add_failed_dag_run(dag, id, logical_date):
            dr = dag.create_dagrun(
                run_type=DagRunType.MANUAL,
                run_id="run_id_" + id,
                logical_date=logical_date,
                state=State.FAILED,
                data_interval=(logical_date, logical_date),
                run_after=logical_date,
                triggered_by=DagRunTriggeredByType.TEST,
                session=session,
            )
            ti_op1 = dr.get_task_instance(task_id=op1.task_id, session=session)
            ti_op1.set_state(state=TaskInstanceState.FAILED, session=session)
            dr.update_state(session=session)

        dag_id = "dag_paused_after_limit"
        dag = DAG(dag_id, schedule=None, is_paused_upon_creation=False, max_consecutive_failed_dag_runs=2)
        op1 = BashOperator(task_id="task", bash_command="exit 1;")
        dag.add_task(op1)
        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()

        scheduler_dag = sync_dag_to_db(dag, session=session)
        assert not session.get(DagModel, dag.dag_id).is_paused

        # dag should be paused after 2 failed dag_runs
        add_failed_dag_run(scheduler_dag, "1", TEST_DATE)
        add_failed_dag_run(scheduler_dag, "2", TEST_DATE + timedelta(days=1))
        assert session.get(DagModel, dag.dag_id).is_paused

    def test_dag_is_deactivated_upon_dagfile_deletion(self, dag_maker):
        dag_id = "old_existing_dag"
        with dag_maker(dag_id, schedule=None, is_paused_upon_creation=True) as dag:
            ...
        session = settings.Session()
        sync_dag_to_db(dag, session=session)

        orm_dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))

        assert not orm_dag.is_stale

        DagModel.deactivate_deleted_dags(
            bundle_name=orm_dag.bundle_name,
            rel_filelocs=list_py_file_paths(settings.DAGS_FOLDER),
        )

        orm_dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))
        assert orm_dag.is_stale

        session.execute(delete(DagModel).where(DagModel.dag_id == dag_id))
        session.close()

    def test_dag_naive_default_args_start_date_with_timezone(self):
        local_tz = pendulum.timezone("Europe/Zurich")
        default_args = {"start_date": datetime.datetime(2018, 1, 1, tzinfo=local_tz)}

        dag = DAG("DAG", schedule=None, default_args=default_args)
        assert dag.timezone.name == local_tz.name

        dag = DAG("DAG", schedule=None, default_args=default_args)
        assert dag.timezone.name == local_tz.name

    def test_schedule_dag_no_previous_runs(self, testing_dag_bundle):
        """
        Tests scheduling a dag with no previous runs
        """
        dag_id = "test_schedule_dag_no_previous_runs"
        dag = DAG(dag_id=dag_id, schedule=None)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=TEST_DATE))

        scheduler_dag = sync_dag_to_db(dag)
        dag_run = scheduler_dag.create_dagrun(
            run_id="test",
            run_type=DagRunType.SCHEDULED,
            logical_date=TEST_DATE,
            state=State.RUNNING,
            data_interval=(TEST_DATE, TEST_DATE),
            run_after=TEST_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        assert dag_run is not None
        assert dag.dag_id == dag_run.dag_id
        assert dag_run.run_id is not None
        assert dag_run.run_id != ""
        assert dag_run.logical_date == TEST_DATE, (
            f"dag_run.logical_date did not match expectation: {dag_run.logical_date}"
        )
        assert dag_run.state == State.RUNNING
        assert dag_run.run_type != DagRunType.MANUAL

    @patch("airflow.models.dagrun.Stats")
    def test_dag_handle_callback_crash(self, mock_stats, testing_dag_bundle):
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

        scheduler_dag = sync_dag_to_db(dag)

        with create_session() as session:
            dag_run = scheduler_dag.create_dagrun(
                run_id="test",
                state=State.RUNNING,
                logical_date=when,
                run_type=DagRunType.MANUAL,
                session=session,
                data_interval=(when, when),
                run_after=when,
                triggered_by=DagRunTriggeredByType.TEST,
            )

            # should not raise any exception
        dag_run.handle_dag_callback(dag=dag, success=False)
        dag_run.handle_dag_callback(dag=dag, success=True)

        mock_stats.incr.assert_called_with(
            "dag.callback_exceptions",
            tags={"dag_id": "test_dag_callback_crash"},
        )

    def test_dag_handle_callback_with_removed_task(self, dag_maker, session, testing_dag_bundle):
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

        scheduler_dag = sync_dag_to_db(dag)
        with create_session() as session:
            dag_run = scheduler_dag.create_dagrun(
                run_id="test",
                state=State.RUNNING,
                logical_date=TEST_DATE,
                run_type=DagRunType.MANUAL,
                session=session,
                data_interval=(TEST_DATE, TEST_DATE),
                run_after=TEST_DATE,
                triggered_by=DagRunTriggeredByType.TEST,
            )
            dag._remove_task(task_removed.task_id)
            tis = dag_run.get_task_instances(session=session)
            tis[-1].state = TaskInstanceState.REMOVED
            assert dag_run.get_task_instance(task_removed.task_id).state == TaskInstanceState.REMOVED

            # should not raise any exception
            dag_run.handle_dag_callback(dag=dag, success=False)
            dag_run.handle_dag_callback(dag=dag, success=True)

    @time_machine.travel(timezone.datetime(2025, 11, 11))
    @pytest.mark.parametrize(("catchup", "expected_next_dagrun"), [(True, DEFAULT_DATE), (False, None)])
    def test_next_dagrun_after_fake_scheduled_previous(
        self, catchup, expected_next_dagrun, testing_dag_bundle
    ):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have.
        Tests with both catchup=True and catchup=False to verify different behaviors.
        """
        delta = datetime.timedelta(hours=1)
        dag_id = f"test_schedule_dag_fake_scheduled_previous_{catchup}"
        dag = DAG(dag_id=dag_id, schedule=delta, start_date=DEFAULT_DATE, catchup=catchup)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=DEFAULT_DATE))

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()

        _create_dagrun(
            dag,
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
            state=State.SUCCESS,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        )
        sync_dag_to_db(dag)
        with create_session() as session:
            model = session.get(DagModel, dag.dag_id)

        if expected_next_dagrun is None:
            # Verify it's not using the old default date
            assert model.next_dagrun.year == 2025
            assert model.next_dagrun.month == 11
            # Verify next_dagrun_create_after is scheduled after next_dagrun
            assert model.next_dagrun_create_after > model.next_dagrun
        else:
            # For catchup=True, even though there is a run for this date already,
            # it is marked as manual/external, so we should create a scheduled one anyway!
            assert model.next_dagrun == expected_next_dagrun
            assert model.next_dagrun_create_after == expected_next_dagrun + delta

    def test_schedule_dag_once(self, testing_dag_bundle):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag_id = "test_schedule_dag_once"
        with DAG(dag_id=dag_id, schedule="@once", start_date=TEST_DATE) as dag:
            BaseOperator(task_id="faketastic", owner="Also fake", start_date=TEST_DATE)
        assert qualname(dag.timetable) == "airflow.sdk.definitions.timetables.simple.OnceTimetable"

        _create_dagrun(
            dag,
            run_type=DagRunType.SCHEDULED,
            logical_date=TEST_DATE,
            state=State.SUCCESS,
            data_interval=(TEST_DATE, TEST_DATE),
        )

        # Then sync again after creating the dag run -- this should update next_dagrun
        SerializedDAG.bulk_write_to_db("testing", None, [dag])
        with create_session() as session:
            model = session.get(DagModel, dag.dag_id)

        assert model.next_dagrun is None
        assert model.next_dagrun_create_after is None

    def test_fractional_seconds(self):
        """
        Tests if fractional seconds are stored in the database
        """
        dag_id = "test_fractional_seconds"
        dag = DAG(dag_id=dag_id, schedule="@once", start_date=TEST_DATE)
        dag.add_task(BaseOperator(task_id="faketastic", owner="Also fake", start_date=TEST_DATE))

        start_date = timezone.utcnow()

        run = _create_dagrun(
            dag,
            run_type=DagRunType.MANUAL,
            logical_date=start_date,
            start_date=start_date,
            state=State.RUNNING,
            data_interval=(start_date, start_date),
        )

        run.refresh_from_db()

        assert start_date == run.logical_date, "dag run logical_date loses precision"
        assert start_date == run.start_date, "dag run start_date loses precision "

    def test_rich_comparison_ops(self):
        test_dag_id = "test_rich_comparison_ops"

        class DAGsubclass(DAG):
            pass

        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(test_dag_id, schedule=None, default_args=args)

        dag_eq = DAG(test_dag_id, schedule=None, default_args=args)

        dag_diff_name = DAG(test_dag_id + "_neq", schedule=None, default_args=args)

        dag_subclass = DAGsubclass(test_dag_id, schedule=None, default_args=args)
        dag_subclass_diff_name = DAGsubclass(test_dag_id + "2", schedule=None, default_args=args)

        # test identity equality
        assert dag == dag

        # test dag (in)equality based on _comps
        assert dag_eq == dag
        assert dag_diff_name != dag

        # test dag inequality based on type even if _comps happen to match
        assert dag_subclass != dag

        # a dag should equal an unpickled version of itself
        dump = pickle.dumps(dag)
        assert pickle.loads(dump) == dag

        # dags are ordered based on dag_id no matter what the type is
        assert dag < dag_diff_name
        assert dag < dag_subclass_diff_name

        # greater than should have been created automatically by functools
        assert dag_diff_name > dag

        # hashes are non-random and match equality
        assert hash(dag) == hash(dag)
        assert hash(dag_eq) == hash(dag)
        assert hash(dag_diff_name) != hash(dag)
        assert hash(dag_subclass) != hash(dag)

    def test_get_paused_dag_ids(self, testing_dag_bundle):
        dag_id = "test_get_paused_dag_ids"
        dag = DAG(dag_id, schedule=None, is_paused_upon_creation=True)
        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            is_stale=False,
            is_paused=True,  # Set is_paused to match DAG's is_paused_upon_creation
        )
        session.add(orm_dag)
        session.flush()
        sync_dag_to_db(dag)
        assert DagModel.get_dagmodel(dag_id) is not None

        paused_dag_ids = DagModel.get_paused_dag_ids([dag_id])
        assert paused_dag_ids == {dag_id}

        with create_session() as session:
            session.execute(delete(DagModel).where(DagModel.dag_id == dag_id))

    @pytest.mark.parametrize(
        ("schedule_arg", "expected_timetable", "interval_description"),
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
        assert coerce_to_core_timetable(dag.timetable).description == interval_description

    def test_timetable_and_description_from_asset(self):
        uri = "test://asset"
        dag = DAG(
            "test_schedule_interval_arg", schedule=[Asset(uri=uri, group="test-group")], start_date=TEST_DATE
        )
        assert dag.timetable == AssetTriggeredTimetable(Asset(uri=uri, group="test-group"))
        assert coerce_to_core_timetable(dag.timetable).description == "Triggered by assets"

    @pytest.mark.parametrize(
        ("timetable", "expected_description"),
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
        assert coerce_to_core_timetable(dag.timetable).description == expected_description

    def test_create_dagrun_job_id_is_set(self, testing_dag_bundle):
        job_id = 42
        dag = DAG(dag_id="test_create_dagrun_job_id_is_set", schedule=None)
        scheduler_dag = sync_dag_to_db(dag)
        dr = scheduler_dag.create_dagrun(
            run_id="test_create_dagrun_job_id_is_set",
            logical_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            run_after=DEFAULT_DATE,
            run_type=DagRunType.MANUAL,
            state=State.NONE,
            creating_job_id=job_id,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        assert dr.creating_job_id == job_id

    @pytest.mark.parametrize("partition_key", [None, "my-key", 123])
    def test_create_dagrun_partition_key(self, partition_key, dag_maker):
        with dag_maker("test_create_dagrun_partition_key"):
            ...
        cm = nullcontext()
        if isinstance(partition_key, int):
            cm = pytest.raises(ValueError, match="Expected partition_key to be `str` | `None` but got `int`")
        with cm:
            dr = dag_maker.create_dagrun(
                run_id="test_create_dagrun_partition_key",
                run_after=DEFAULT_DATE,
                run_type=DagRunType.MANUAL,
                state=State.NONE,
                triggered_by=DagRunTriggeredByType.TEST,
                partition_key=partition_key,
            )
            assert dr.partition_key == partition_key

    def test_dag_add_task_sets_default_task_group(self):
        dag = DAG(dag_id="test_dag_add_task_sets_default_task_group", schedule=None, start_date=DEFAULT_DATE)
        task_without_task_group = EmptyOperator(task_id="task_without_group_id")
        default_task_group = TaskGroupContext.get_current(dag)
        dag.add_task(task_without_task_group)
        assert default_task_group.get_child_by_label("task_without_group_id") == task_without_task_group

        task_group = TaskGroup(group_id="task_group", dag=dag)
        task_with_task_group = EmptyOperator(task_id="task_with_task_group", task_group=task_group)
        dag.add_task(task_with_task_group)
        assert task_group.get_child_by_label("task_with_task_group") == task_with_task_group
        assert dag.get_task("task_group.task_with_task_group") == task_with_task_group

    @pytest.mark.parametrize("dag_run_state", [DagRunState.QUEUED, DagRunState.RUNNING])
    @pytest.mark.need_serialized_dag
    def test_clear_set_dagrun_state(self, dag_run_state, dag_maker, session):
        dag_id = "test_clear_set_dagrun_state"

        with dag_maker(dag_id, start_date=DEFAULT_DATE, max_active_runs=1) as dag:
            task_id = "t1"
            EmptyOperator(task_id=task_id)

        dr = dag_maker.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.FAILED,
            start_date=DEFAULT_DATE,
            logical_date=DEFAULT_DATE,
            session=session,
        )
        session.commit()
        session.refresh(dr)
        assert dr.state == "failed"

        dag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            dag_run_state=dag_run_state,
            session=session,
        )
        session.refresh(dr)
        assert dr.state == dag_run_state

    @pytest.mark.parametrize("dag_run_state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_set_dagrun_state_for_mapped_task(self, session, dag_run_state, dag_maker):
        dag_id = "test_clear_set_dagrun_state"

        task_id = "t1"

        with dag_maker(
            dag_id, schedule=None, start_date=DEFAULT_DATE, max_active_runs=1, serialized=True
        ) as dag:

            @task_decorator
            def make_arg_lists():
                return [[1], [2], [{"a": "b"}]]

            def consumer(value):
                print(value)

            PythonOperator.partial(task_id=task_id, python_callable=consumer).expand(op_args=make_arg_lists())

        dagrun_1 = dag_maker.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=DagRunState.FAILED,
            start_date=DEFAULT_DATE,
            logical_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            session=session,
        )

        # Get the (de)serialized MappedOperator
        mapped = dag.get_task(task_id)
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
        dagruns = session.scalars(select(DagRun).where(DagRun.dag_id == dag_id)).all()

        assert len(dagruns) == 1
        dagrun: DagRun = dagruns[0]
        assert dagrun.state == dag_run_state

    def test_dag_test_basic(self, testing_dag_bundle):
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)
        sync_dag_to_db(dag)

        mock_object = mock.MagicMock()

        @task_decorator
        def check_task():
            # we call a mock object to ensure that this task actually ran.
            mock_object()

        with dag:
            check_task()
        sync_dag_to_db(dag)

        dag.test()
        mock_object.assert_called_once()

    def test_dag_test_with_dependencies(self, testing_dag_bundle):
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)
        sync_dag_to_db(dag)
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
        sync_dag_to_db(dag)

        dag.test()
        mock_object.assert_called_with("output of first task")

    def test_dag_test_with_fail_handler(self, testing_dag_bundle):
        mock_handle_object_1 = mock.MagicMock()
        mock_handle_object_2 = mock.MagicMock()

        def handle_task_failure(context):
            ti = context["task_instance"]
            mock_handle_object_1(f"task {ti.task_id} failed...")

        def handle_dag_failure(context):
            dag_id = context["dag"].dag_id
            mock_handle_object_2(f"dag {dag_id} run failed...")

        dag = DAG(
            dag_id="test_local_testing_conn_file",
            default_args={"on_failure_callback": handle_task_failure},
            on_failure_callback=handle_dag_failure,
            start_date=DEFAULT_DATE,
            schedule=None,
        )

        mock_task_object_1 = mock.MagicMock()
        mock_task_object_2 = mock.MagicMock()
        sync_dag_to_db(dag)

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
        sync_dag_to_db(dag)

        dr = dag.test()
        ti1 = dr.get_task_instance("check_task")
        ti2 = dr.get_task_instance("check_task_2")

        assert ti1
        assert ti2
        assert ti1.state == TaskInstanceState.FAILED
        assert ti2.state == TaskInstanceState.UPSTREAM_FAILED

        mock_handle_object_1.assert_called_with("task check_task failed...")
        mock_handle_object_2.assert_called_with("dag test_local_testing_conn_file run failed...")
        mock_task_object_1.assert_called()
        mock_task_object_2.assert_not_called()

    def test_dag_connection_file(self, tmp_path, testing_dag_bundle):
        test_connections_string = """
---
my_postgres_conn:
  - conn_id: my_postgres_conn
    conn_type: postgres
        """
        dag = DAG(dag_id="test_local_testing_conn_file", schedule=None, start_date=DEFAULT_DATE)
        sync_dag_to_db(dag)

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
        ("ti_state_begin", "ti_state_end"),
        [
            *((state, None) for state in State.task_states if state != TaskInstanceState.RUNNING),
            (TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING),
        ],
    )
    def test_clear_dag(
        self,
        ti_state_begin: TaskInstanceState | None,
        ti_state_end: TaskInstanceState | None,
        dag_maker,
        session,
    ):
        dag_id = "test_clear_dag"

        task_id = "t1"
        with dag_maker(
            dag_id,
            schedule=None,
            start_date=DEFAULT_DATE,
            max_active_runs=1,
            serialized=True,
        ) as dag:
            EmptyOperator(task_id=task_id)

        session = settings.get_session()()
        dagrun_1 = dag_maker.create_dagrun(
            run_id="backfill",
            run_type=DagRunType.BACKFILL_JOB,
            state=DagRunState.RUNNING,
            start_date=DEFAULT_DATE,
            logical_date=DEFAULT_DATE,
            # triggered_by=DagRunTriggeredByType.TEST,
            session=session,
        )

        task_instance_1 = dagrun_1.get_task_instance(task_id, session=session)
        if TYPE_CHECKING:
            assert task_instance_1
        task_instance_1.state = ti_state_begin
        task_instance_1.job_id = 123
        session.commit()

        create_scheduler_dag(dag).clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            session=session,
        )

        task_instances = session.scalars(select(TI).where(TI.dag_id == dag_id)).all()

        assert len(task_instances) == 1
        task_instance: TI = task_instances[0]
        assert task_instance.state == ti_state_end

    def test_next_dagrun_info_once(self):
        dag = DAG("test_scheduler_dagrun_once", start_date=timezone.datetime(2015, 1, 1), schedule="@once")
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2015, 1, 1)

        next_info = scheduler_dag.next_dagrun_info(next_info.data_interval)
        assert next_info is None

    def test_next_dagrun_info_catchup(self):
        """
        Test to check that a DAG with catchup = False only schedules beginning now, not back to the start date
        """

        def make_dag(dag_id, schedule, start_date, catchup) -> SerializedDAG:
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

            return create_scheduler_dag(dag)

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
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 1, 4)

        # The date to create is in the future, this is handled by "DagModel.dags_needing_dagruns"
        next_info = scheduler_dag.next_dagrun_info(next_info.data_interval)
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
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 1)

        next_info = scheduler_dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 2)

        next_info = scheduler_dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 3)

        # The date to create is in the future, this is handled by "DagModel.dags_needing_dagruns"
        next_info = scheduler_dag.next_dagrun_info(next_info.data_interval)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2020, 5, 4)

    def test_next_dagrun_info_timetable_exception(self, caplog):
        """Test the DAG does not crash the scheduler if the timetable raises an exception."""

        class FailingTimetable(Timetable):
            def next_dagrun_info(self, last_automated_data_interval, restriction):
                raise RuntimeError("this fails")

        def _find_registered_custom_timetable(s):
            if s == qualname(FailingTimetable):
                return FailingTimetable
            raise ValueError(f"unexpected class {s!r}")

        dag = DAG(
            "test_next_dagrun_info_timetable_exception",
            start_date=timezone.datetime(2020, 5, 1),
            schedule=FailingTimetable(),
            catchup=True,
        )
        with (
            mock.patch(
                "airflow.serialization.encoders.find_registered_custom_timetable",
                _find_registered_custom_timetable,
            ),
            mock.patch(
                "airflow.serialization.decoders.find_registered_custom_timetable",
                _find_registered_custom_timetable,
            ),
        ):
            scheduler_dag = create_scheduler_dag(dag)

        def _check_logs(records: list[logging.LogRecord], data_interval: DataInterval) -> None:
            assert len(records) == 1
            record = records[0]
            assert record.exc_info is not None, "Should contain exception"
            assert record.message == (
                f"Failed to fetch run info after data interval {data_interval} "
                f"for DAG 'test_next_dagrun_info_timetable_exception'"
            )

        with caplog.at_level(level=logging.ERROR):
            next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info is None, "failed next_dagrun_info should return None"
        _check_logs(caplog.records, data_interval=None)
        caplog.clear()
        data_interval = DataInterval(timezone.datetime(2020, 5, 1), timezone.datetime(2020, 5, 2))
        with caplog.at_level(level=logging.ERROR):
            next_info = scheduler_dag.next_dagrun_info(data_interval)
        assert next_info is None, "failed next_dagrun_info should return None"
        _check_logs(caplog.records, data_interval)

    def test_next_dagrun_after_auto_align(self):
        """
        Test if the schedule will be auto aligned with the start_date
        such that if the start_date coincides with the schedule the first
        logical_date will be start_date, otherwise it will be start_date +
        interval.

        This test verifies both catchup=True and catchup=False scenarios:
        - With catchup=True: The scheduler aligns with the historical start date
        - With catchup=False: The scheduler aligns with the current date, ignoring historical dates
        """
        # Test catchup=True scenario (using historical dates)
        dag = DAG(
            dag_id="test_scheduler_auto_align_1",
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule="4 5 * * *",
            catchup=True,
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2016, 1, 2, 5, 4)

        dag = DAG(
            dag_id="test_scheduler_auto_align_2",
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule="10 10 * * *",
            catchup=True,
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2016, 1, 1, 10, 10)

        # Test catchup=False scenario (using current dates)
        start_date = timezone.datetime(2016, 1, 1, 10, 10, 0)
        dag = DAG(
            dag_id="test_scheduler_auto_align_3",
            start_date=start_date,
            schedule="4 5 * * *",
            catchup=False,
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        # With catchup=False, next_dagrun should be based on the current date
        # Verify it's not using the old start_date
        assert next_info.logical_date.year >= start_date.year
        assert next_info.logical_date.month >= start_date.month

        # Verify it's following the cron schedule pattern (4 5 * * *)
        assert next_info.logical_date.hour == 5
        assert next_info.logical_date.minute == 4

        start_date = timezone.datetime(2016, 1, 1, 10, 10, 0)
        dag = DAG(
            dag_id="test_scheduler_auto_align_4",
            start_date=start_date,
            schedule="10 10 * * *",
            catchup=False,
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        # With catchup=False, next_dagrun should be based on the current date
        # Verify it's not using the old start_date
        assert next_info.logical_date.year >= start_date.year
        assert next_info.logical_date.month >= start_date.month
        # Verify it's following the cron schedule pattern (10 10 * * *)
        assert next_info.logical_date.hour == 10
        assert next_info.logical_date.minute == 10

    def test_next_dagrun_info_on_29_feb(self):
        dag = DAG(
            "test_scheduler_dagrun_29_feb", start_date=timezone.datetime(2024, 1, 1), schedule="0 0 29 2 *"
        )
        scheduler_dag = create_scheduler_dag(dag)

        next_info = scheduler_dag.next_dagrun_info(None)
        assert next_info
        assert next_info.logical_date == timezone.datetime(2024, 2, 29)

        next_info = scheduler_dag.next_dagrun_info(next_info.data_interval)
        assert next_info.logical_date == timezone.datetime(2028, 2, 29)
        assert next_info.data_interval.start == timezone.datetime(2028, 2, 29)
        assert next_info.data_interval.end == timezone.datetime(2032, 2, 29)

    def test_validate_params_on_trigger_dag(self, testing_dag_bundle):
        dag = DAG("dummy-dag", schedule=None, params={"param1": Param(type="string")})
        with pytest.raises(ValueError, match="No value passed"):
            sync_dag_to_db(dag).create_dagrun(
                run_id="test_dagrun_missing_param",
                run_type=DagRunType.MANUAL,
                state=State.RUNNING,
                logical_date=TEST_DATE,
                data_interval=(TEST_DATE, TEST_DATE),
                run_after=TEST_DATE,
                triggered_by=DagRunTriggeredByType.TEST,
            )

        dag = DAG("dummy-dag", schedule=None, params={"param1": Param(type="string")})
        with pytest.raises(ValueError, match="None is not of type 'string'"):
            sync_dag_to_db(dag).create_dagrun(
                run_id="test_dagrun_missing_param",
                run_type=DagRunType.MANUAL,
                state=State.RUNNING,
                logical_date=TEST_DATE,
                conf={"param1": None},
                data_interval=(TEST_DATE, TEST_DATE),
                run_after=TEST_DATE,
                triggered_by=DagRunTriggeredByType.TEST,
            )

        dag = DAG("dummy-dag", schedule=None, params={"param1": Param(type="string")})
        sync_dag_to_db(dag).create_dagrun(
            run_id="test_dagrun_missing_param",
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            logical_date=TEST_DATE,
            conf={"param1": "hello"},
            data_interval=(TEST_DATE, TEST_DATE),
            run_after=TEST_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )

    def test_dag_owner_links(self, testing_dag_bundle):
        dag = DAG(
            "dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            owner_links={"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"},
        )
        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()
        assert dag.owner_links == {"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"}
        sync_dag_to_db(dag, session=session)

        expected_owners = {"dag": {"owner1": "https://mylink.com", "owner2": "mailto:someone@yoursite.com"}}
        orm_dag_owners = DagOwnerAttributes.get_all(session)
        assert orm_dag_owners == expected_owners

        # Test dag owner links are removed completely
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE)
        sync_dag_to_db(dag, session=session)

        orm_dag_owners = session.scalars(select(DagOwnerAttributes)).all()
        assert not orm_dag_owners

    @pytest.mark.need_serialized_dag
    @pytest.mark.parametrize(
        ("reference_type", "reference_column"),
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, "logical_date", id="logical_date"),
            pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, "queued_at", id="queued_at"),
            pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), "NONE", id="fixed_deadline"),
        ],
    )
    def test_dagrun_deadline(self, reference_type, reference_column, dag_maker, session):
        interval = datetime.timedelta(hours=1)
        with dag_maker(
            dag_id="test_queued_deadline",
            schedule=datetime.timedelta(days=1),
            deadline=DeadlineAlert(
                reference=reference_type,
                interval=interval,
                callback=AsyncCallback(empty_callback_for_deadline),
            ),
        ) as dag:
            ...

        dr = dag.create_dagrun(
            run_id="test_dagrun_deadline",
            run_type=DagRunType.SCHEDULED,
            state=State.QUEUED,
            logical_date=TEST_DATE,
            run_after=TEST_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        session.flush()
        dr = session.merge(dr)

        assert len(dr.deadlines) == 1
        assert dr.deadlines[0].deadline_time == getattr(dr, reference_column, DEFAULT_DATE) + interval

    def test_dag_with_multiple_deadlines(self, dag_maker, session):
        """Test that a DAG with multiple deadlines stores all deadlines in the database."""
        deadlines = [
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=datetime.timedelta(minutes=5),
                callback=AsyncCallback(empty_callback_for_deadline),
            ),
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=datetime.timedelta(minutes=10),
                callback=AsyncCallback(empty_callback_for_deadline),
            ),
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
                interval=datetime.timedelta(hours=1),
                callback=AsyncCallback(empty_callback_for_deadline),
            ),
        ]

        with dag_maker(
            dag_id="test_multiple_deadlines",
            schedule=datetime.timedelta(days=1),
            deadline=deadlines,
        ) as dag:
            ...

        scheduler_dag = sync_dag_to_db(dag)
        dr = scheduler_dag.create_dagrun(
            run_id="test_multiple_deadlines",
            run_type=DagRunType.SCHEDULED,
            state=State.QUEUED,
            logical_date=TEST_DATE,
            run_after=TEST_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        session.flush()
        dr = session.merge(dr)

        # Check that all 3 deadlines were created
        assert len(dr.deadlines) == 3

        # Verify each deadline has correct properties
        deadline_times = [d.deadline_time for d in dr.deadlines]
        expected_times = [
            dr.queued_at + datetime.timedelta(minutes=5),
            dr.queued_at + datetime.timedelta(minutes=10),
            dr.logical_date + datetime.timedelta(hours=1),
        ]

        # Sort both lists to compare regardless of order
        deadline_times.sort()
        expected_times.sort()
        assert deadline_times == expected_times


class TestDagModel:
    def _clean(self):
        clear_db_dags()
        clear_db_assets()
        clear_db_runs()
        clear_db_dag_bundles()
        clear_db_teams()

    def setup_method(self):
        self._clean()

    def teardown_method(self):
        self._clean()

    def test_dags_needing_dagruns_not_too_early(self, testing_dag_bundle):
        dag = DAG(dag_id="far_future_dag", schedule=None, start_date=timezone.datetime(2038, 1, 1))
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            max_active_tasks=1,
            has_task_concurrency_limits=False,
            next_dagrun=dag.start_date,
            next_dagrun_create_after=timezone.datetime(2038, 1, 2),
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_assets(self, dag_maker, session):
        asset = Asset(uri="test://asset", group="test-group")
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
        dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == dag.dag_id))
        asset_model: AssetModel = dag_model.schedule_assets[0]
        session.add(AssetDagRunQueue(asset_id=asset_model.id, target_dag_id=dag_model.dag_id))
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == [dag_model]

        # create run so we don't need a run anymore (due to max active runs)
        dag_maker.create_dagrun(
            run_type=DagRunType.ASSET_TRIGGERED,
            state=DagRunState.QUEUED,
            logical_date=pendulum.now("UTC"),
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
        asset_alias_model.assets.append(asset_model)
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
        session.add(AssetDagRunQueue(asset_id=asset_model.id, target_dag_id=dag_model.dag_id))
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == [dag_model]

        # create run so we don't need a run anymore (due to max active runs)
        dag_maker.create_dagrun(
            run_type=DagRunType.ASSET_TRIGGERED,
            state=DagRunState.QUEUED,
            logical_date=pendulum.now("UTC"),
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

    @pytest.mark.parametrize("ref", [Asset.ref(name="1"), Asset.ref(uri="s3://bucket/assets/1")])
    @pytest.mark.want_activate_assets
    @pytest.mark.need_serialized_dag
    def test_dags_needing_dagruns_asset_refs(self, dag_maker, session, ref):
        asset = Asset(name="1", uri="s3://bucket/assets/1")

        with dag_maker(dag_id="producer", schedule=None, session=session) as dag:
            op = EmptyOperator(task_id="op", outlets=asset)
        ser_op = dag.get_task(op.task_id)

        dr: DagRun = dag_maker.create_dagrun()

        with dag_maker(dag_id="consumer", schedule=ref, max_active_runs=1):
            pass

        # Nothing from the upstream yet, no runs needed.
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == []
        query, _ = DagModel.dags_needing_dagruns(session)
        assert query.all() == []

        # Upstream triggered, now we need a run.
        ti = dr.get_task_instance("op")
        ti.refresh_from_task(ser_op)
        run_task_instance(ti, op)

        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["consumer"]
        query, _ = DagModel.dags_needing_dagruns(session)
        assert [dm.dag_id for dm in query] == ["consumer"]

    @pytest.mark.want_activate_assets
    @pytest.mark.need_serialized_dag
    def test_dags_needing_dagruns_checking_stale_adrq(self, dag_maker, session):
        asset = Asset(name="1", uri="s3://bucket/assets/1")
        dag_id_to_test = "test"

        # Dag 'test' depends on an outlet in 'producer'.
        with dag_maker(dag_id="producer", schedule=None, session=session) as dag:
            op = EmptyOperator(task_id="op", outlets=asset)
        dr = dag_maker.create_dagrun()
        outlet_ti = dr.get_task_instance("op")
        outlet_ti.refresh_from_task(dag.get_task(op.task_id))
        with dag_maker(dag_id=dag_id_to_test, schedule=asset, session=session):
            pass

        # An adrq should be created when the outlet task is run.
        run_task_instance(outlet_ti, op)
        query, _ = DagModel.dags_needing_dagruns(session)
        assert [dm.dag_id for dm in query] == [dag_id_to_test]
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == [dag_id_to_test]

        # Now the dag is changed to NOT depend on 'producer'.
        # Rerunning dags_needing_dagruns should clear up that adrq.
        with dag_maker(dag_id=dag_id_to_test, schedule=None, session=session):
            pass
        query, _ = DagModel.dags_needing_dagruns(session)
        assert query.all() == []
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == []

    def test_max_active_runs_not_none(self, testing_dag_bundle):
        dag = DAG(
            dag_id="test_max_active_runs_not_none",
            schedule=None,
            start_date=timezone.datetime(2038, 1, 1),
        )
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            has_task_concurrency_limits=False,
            next_dagrun=None,
            next_dagrun_create_after=None,
            is_stale=False,
        )
        # assert max_active_runs updated
        assert orm_dag.max_active_runs == 16
        session.add(orm_dag)
        session.flush()
        assert orm_dag.max_active_runs is not None

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_only_unpaused(self, testing_dag_bundle):
        """
        We should never create dagruns for unpaused DAGs
        """
        dag = DAG(dag_id="test_dags", schedule=None, start_date=DEFAULT_DATE)
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            has_task_concurrency_limits=False,
            next_dagrun=DEFAULT_DATE,
            next_dagrun_create_after=DEFAULT_DATE + timedelta(days=1),
            is_stale=False,
        )
        session.merge(orm_dag)
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        needed = query.all()
        assert [d.dag_id for d in needed] == [orm_dag.dag_id]

        orm_dag.is_paused = True
        session.merge(orm_dag)
        session.flush()

        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        assert dag_models == []

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_doesnot_send_dagmodel_with_import_errors(self, session, testing_dag_bundle):
        """
        We check that has_import_error is false for dags
        being set to scheduler to create dagruns
        """
        dag = DAG(dag_id="test_dags", schedule=None, start_date=DEFAULT_DATE)
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            has_task_concurrency_limits=False,
            next_dagrun=DEFAULT_DATE,
            next_dagrun_create_after=DEFAULT_DATE + timedelta(days=1),
            is_stale=False,
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
        dag_models = query.all()
        assert dag_models == []

        session.rollback()
        session.close()

    def test_relative_fileloc(self, session, testing_dag_bundle):
        rel_path = "test_assets.py"
        bundle_path = TEST_DAGS_FOLDER
        file_path = bundle_path / rel_path
        bag = DagBag(dag_folder=file_path, bundle_path=bundle_path)

        dag = bag.get_dag("dag_with_skip_task")

        bundle_name = "testing"

        dag_model = DagModel(
            dag_id=dag.dag_id,
            bundle_name=bundle_name,
        )
        session.merge(dag_model)
        session.flush()

        sync_dag_to_db(dag, session=session)

        assert dag.fileloc == str(file_path)
        assert dag.relative_fileloc == str(rel_path)

        SerializedDagModel.write_dag(
            LazyDeserializedDAG.from_dag(dag),
            bundle_name=bundle_name,
            session=session,
        )
        dm = session.get(DagModel, dag.dag_id)
        assert dm.fileloc == str(file_path)
        assert dm.relative_fileloc == str(rel_path)
        sdm = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag.dag_id))
        assert sdm.dag.fileloc == str(file_path)
        assert sdm.dag.relative_fileloc == str(rel_path)

    def test__processor_dags_folder(self, session, testing_dag_bundle):
        """Only populated after deserializtion"""
        bundle_name = "testing"

        dag = DAG(dag_id="test", schedule=None)
        dag.fileloc = "/abc/test.py"

        dag_model = DagModel(
            dag_id=dag.dag_id,
            bundle_name=bundle_name,
        )
        session.merge(dag_model)
        session.flush()

        scheduler_dag = sync_dag_to_db(dag)
        assert scheduler_dag._processor_dags_folder == settings.DAGS_FOLDER
        sdm = SerializedDagModel.get(dag.dag_id, session)
        assert sdm.dag._processor_dags_folder == settings.DAGS_FOLDER

    @pytest.mark.need_serialized_dag
    def test_dags_needing_dagruns_triggered_date_by_dag_queued_times(self, session, dag_maker):
        asset1 = Asset(uri="test://asset1", group="test-group")
        asset2 = Asset(uri="test://asset2", name="test_asset_2", group="test-group")

        for dag_id, asset in [("assets-1", asset1), ("assets-2", asset2)]:
            with dag_maker(dag_id=dag_id, start_date=timezone.utcnow(), session=session):
                EmptyOperator(task_id="task", outlets=[asset])
            dr = dag_maker.create_dagrun()

            asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset.uri))

            session.add(
                AssetEvent(
                    asset_id=asset_id,
                    source_task_id="task",
                    source_dag_id=dr.dag_id,
                    source_run_id=dr.run_id,
                    source_map_index=-1,
                )
            )

        asset1_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset1.uri))
        asset2_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset2.uri))

        with dag_maker(dag_id="assets-consumer-multiple", schedule=[asset1, asset2]) as dag:
            pass

        session.flush()
        session.add_all(
            [
                AssetDagRunQueue(asset_id=asset1_id, target_dag_id=dag.dag_id, created_at=DEFAULT_DATE),
                AssetDagRunQueue(
                    asset_id=asset2_id,
                    target_dag_id=dag.dag_id,
                    created_at=DEFAULT_DATE + timedelta(hours=1),
                ),
            ]
        )
        session.flush()

        query, triggered_date_by_dag = DagModel.dags_needing_dagruns(session)
        assert len(triggered_date_by_dag) == 1
        assert dag.dag_id in triggered_date_by_dag
        last_queued_time = triggered_date_by_dag[dag.dag_id]
        assert last_queued_time == DEFAULT_DATE + timedelta(hours=1)

    def test_asset_expression(self, session: Session, testing_dag_bundle) -> None:
        dag = DAG(
            dag_id="test_dag_asset_expression",
            schedule=AssetAny(
                Asset(uri="s3://dag1/output_1.txt", extra={"hi": "bye"}, group="test-group"),
                AssetAll(
                    Asset(
                        uri="s3://dag2/output_1.txt",
                        name="test_asset_2",
                        extra={"hi": "bye"},
                        group="test-group",
                    ),
                    Asset("s3://dag3/output_3.txt", extra={"hi": "bye"}, group="test-group"),
                    AssetAll(
                        AssetAll(
                            Asset("s3://dag3/output_4.txt", extra={"hi": "bye"}, group="test-group"),
                            Asset("s3://dag3/output_5.txt", extra={"hi": "bye"}, group="test-group"),
                        ),
                        Asset("s3://dag3/output_6.txt", extra={"hi": "bye"}, group="test-group"),
                    ),
                ),
                AssetAlias(name="test_name", group="test-group"),
            ),
            start_date=datetime.datetime.min,
        )
        SerializedDAG.bulk_write_to_db("testing", None, [dag], session=session)

        expression = session.scalars(
            select(DagModel.asset_expression).where(DagModel.dag_id == dag.dag_id)
        ).one()
        assert expression == {
            "any": [
                {
                    "asset": {
                        "uri": "s3://dag1/output_1.txt",
                        "name": "s3://dag1/output_1.txt",
                        "group": "test-group",
                        "id": ANY,
                    }
                },
                {
                    "all": [
                        {
                            "asset": {
                                "uri": "s3://dag2/output_1.txt",
                                "name": "test_asset_2",
                                "group": "test-group",
                                "id": ANY,
                            }
                        },
                        {
                            "asset": {
                                "uri": "s3://dag3/output_3.txt",
                                "name": "s3://dag3/output_3.txt",
                                "group": "test-group",
                                "id": ANY,
                            }
                        },
                        {
                            "all": [
                                {
                                    "all": [
                                        {
                                            "asset": {
                                                "uri": "s3://dag3/output_4.txt",
                                                "name": "s3://dag3/output_4.txt",
                                                "group": "test-group",
                                                "id": ANY,
                                            }
                                        },
                                        {
                                            "asset": {
                                                "uri": "s3://dag3/output_5.txt",
                                                "name": "s3://dag3/output_5.txt",
                                                "group": "test-group",
                                                "id": ANY,
                                            }
                                        },
                                    ],
                                },
                                {
                                    "asset": {
                                        "uri": "s3://dag3/output_6.txt",
                                        "name": "s3://dag3/output_6.txt",
                                        "group": "test-group",
                                        "id": ANY,
                                    },
                                },
                            ]
                        },
                    ]
                },
                {"alias": {"name": "test_name", "group": "test-group"}},
            ]
        }

    def test_get_team_name(self, testing_team):
        session = settings.Session()
        dag_bundle = DagBundleModel(name="testing-team")
        dag_bundle.teams.append(testing_team)
        session.add(dag_bundle)
        session.flush()

        dag_id = "test_get_team_name"
        dag = DAG(dag_id, schedule=None)
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing-team",
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()
        assert DagModel.get_dagmodel(dag_id) is not None
        assert DagModel.get_team_name(dag_id, session=session) == "testing"

    def test_get_team_name_no_team(self, testing_team):
        session = settings.Session()
        dag_bundle = DagBundleModel(name="testing")
        session.add(dag_bundle)
        session.flush()

        dag_id = "test_get_team_name_no_team"
        dag = DAG(dag_id, schedule=None)
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            bundle_name="testing",
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()
        assert DagModel.get_dagmodel(dag_id) is not None
        assert DagModel.get_team_name(dag_id, session=session) is None

    def test_get_dag_id_to_team_name_mapping(self, testing_team):
        session = settings.Session()
        bundle1 = DagBundleModel(name="bundle1")
        bundle1.teams.append(testing_team)
        bundle2 = DagBundleModel(name="bundle2")
        session.add(bundle1)
        session.add(bundle2)
        session.flush()

        dag_id1 = "test_dag1"
        dag1 = DAG(dag_id1, schedule=None)
        orm_dag1 = DagModel(
            dag_id=dag1.dag_id,
            bundle_name="bundle1",
            is_stale=False,
        )
        dag_id2 = "test_dag2"
        dag2 = DAG(dag_id2, schedule=None)
        orm_dag2 = DagModel(
            dag_id=dag2.dag_id,
            bundle_name="bundle2",
            is_stale=False,
        )
        session.add(orm_dag1)
        session.add(orm_dag2)
        session.flush()
        assert DagModel.get_dag_id_to_team_name_mapping([dag_id1, dag_id2], session=session) == {
            dag_id1: "testing"
        }


class TestQueries:
    def setup_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()

    @pytest.mark.parametrize("tasks_count", [3, 12])
    def test_count_number_queries(self, tasks_count, testing_dag_bundle):
        dag = DAG("test_dagrun_query_count", schedule=None, start_date=DEFAULT_DATE)
        for i in range(tasks_count):
            EmptyOperator(task_id=f"dummy_task_{i}", owner="test", dag=dag)
        scheduler_dag = sync_dag_to_db(dag)
        with assert_queries_count(5):
            scheduler_dag.create_dagrun(
                run_id="test_dagrun_query_count",
                run_type=DagRunType.MANUAL,
                state=State.RUNNING,
                logical_date=TEST_DATE,
                data_interval=(TEST_DATE, TEST_DATE),
                run_after=TEST_DATE,
                triggered_by=DagRunTriggeredByType.TEST,
            )


@pytest.mark.need_serialized_dag
@pytest.mark.parametrize(
    "run_id",
    ["test-run-id"],
)
def test_set_task_instance_state(run_id, session, dag_maker):
    """Test that set_task_instance_state updates the TaskInstance state and clear downstream failed"""
    start_date = datetime_tz(2020, 1, 1)
    with dag_maker(
        "test_set_task_instance_state",
        start_date=start_date,
        session=session,
        serialized=True,
    ) as dag:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = EmptyOperator(task_id="task_2")
        task_3 = EmptyOperator(task_id="task_3")
        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")
        task_1 >> [task_2, task_3, task_4, task_5]

    dagrun = dag_maker.create_dagrun(
        run_id=run_id,
        state=State.FAILED,
        run_type=DagRunType.SCHEDULED,
    )

    def get_ti_from_db(task):
        return session.scalar(
            select(TI).where(
                TI.dag_id == dag.dag_id,
                TI.task_id == task.task_id,
                TI.run_id == dagrun.run_id,
            )
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


@pytest.mark.need_serialized_dag
def test_set_task_instance_state_mapped(dag_maker, session):
    """Test that when setting an individual mapped TI that the other TIs are not affected"""
    task_id = "t1"

    # The catchup behavior isn't central to what's being tested. Setting catchup explicitly to True.
    with dag_maker(session=session, catchup=True) as dag:

        @task_decorator
        def make_arg_lists():
            return [[1], [2], [{"a": "b"}]]

        def consumer(value):
            print(value)

        mapped = PythonOperator.partial(task_id=task_id, python_callable=consumer).expand(
            op_args=make_arg_lists()
        )

        mapped >> BaseOperator(task_id="downstream")

    dr1 = dag_maker.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.FAILED,
    )

    mapped = dag.get_task(task_id)
    expand_mapped_task(mapped, dr1.run_id, "make_arg_lists", length=2, session=session)

    # set_state(future=True) only applies to scheduled runs
    dr2 = dag_maker.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.FAILED,
        logical_date=DEFAULT_DATE + datetime.timedelta(days=1),
    )
    expand_mapped_task(mapped, dr2.run_id, "make_arg_lists", length=2, session=session)

    session.execute(update(TI).where(TI.dag_id == dag.dag_id).values(state=TaskInstanceState.FAILED))

    ti_query = (
        select(TI.task_id, TI.map_index, TI.run_id, TI.state)
        .where(TI.dag_id == dag.dag_id, TI.task_id.in_([task_id, "downstream"]))
        .order_by(TI.run_id, TI.task_id, TI.map_index)
    )

    # Check pre-conditions
    assert session.execute(ti_query).all() == [
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

    assert session.execute(ti_query).all() == [
        ("downstream", -1, dr1.run_id, None),
        (task_id, 0, dr1.run_id, TaskInstanceState.FAILED),
        (task_id, 1, dr1.run_id, TaskInstanceState.SUCCESS),
        ("downstream", -1, dr2.run_id, None),
        (task_id, 0, dr2.run_id, TaskInstanceState.FAILED),
        (task_id, 1, dr2.run_id, TaskInstanceState.SUCCESS),
    ]


def test_dag_teardowns_property_lists_all_teardown_tasks():
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

    with DAG("dag") as dag:
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
    ("start_date", "expected_infos"),
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

    iterator = create_scheduler_dag(dag).iter_dagrun_infos_between(
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

    def _get_registered_timetable(s):
        if s == "unit.models.test_dag.FailingAfterOneTimetable":
            return FailingAfterOneTimetable
        raise ValueError(f"unexpected class {s!r}")

    dag = DAG(
        dag_id="test_iter_dagrun_infos_between_error",
        start_date=DEFAULT_DATE,
        schedule=FailingAfterOneTimetable(),
    )
    with (
        mock.patch(
            "airflow.serialization.decoders.find_registered_custom_timetable",
            _get_registered_timetable,
        ),
        mock.patch(
            "airflow.serialization.encoders.find_registered_custom_timetable",
            _get_registered_timetable,
        ),
    ):
        scheduler_dag = create_scheduler_dag(dag)

    iterator = scheduler_dag.iter_dagrun_infos_between(earliest=start, latest=end, align=True)
    with caplog.at_level(logging.ERROR):
        infos = list(iterator)

    # The second timetable.next_dagrun_info() call raises an exception, so only the first result is returned.
    assert infos == [DagRunInfo.interval(start, end)]

    assert caplog.record_tuples == [
        (
            "airflow.serialization.definitions.dag",
            logging.ERROR,
            f"Failed to fetch run info after data interval {DataInterval(start, end)} for DAG {dag.dag_id!r}",
        ),
    ]
    assert caplog.entries[0].get("exception"), "should contain exception context"


@pytest.mark.parametrize(
    ("logical_date", "data_interval_start", "data_interval_end", "expected_data_interval"),
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
        bundle_name="dags-folder",
        next_dagrun=logical_date,
        next_dagrun_data_interval_start=data_interval_start,
        next_dagrun_data_interval_end=data_interval_end,
    )

    core_timetable = coerce_to_core_timetable(dag.timetable)
    assert get_next_data_interval(core_timetable, dag_model) == expected_data_interval


@pytest.mark.need_serialized_dag
@pytest.mark.parametrize(
    ("dag_date", "tasks_date", "catchup", "restrict"),
    [
        # catchup=True cases - respects task start dates
        [
            (DEFAULT_DATE, None),
            [
                (DEFAULT_DATE + timedelta(days=1), DEFAULT_DATE + timedelta(days=2)),
                (DEFAULT_DATE + timedelta(days=3), DEFAULT_DATE + timedelta(days=4)),
            ],
            True,
            TimeRestriction(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=4), True),
        ],
        [
            (DEFAULT_DATE, None),
            [(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=1)), (DEFAULT_DATE, None)],
            True,
            TimeRestriction(DEFAULT_DATE, None, True),
        ],
        # catchup=False cases - same time boundaries but different catchup flag
        [
            (DEFAULT_DATE, None),
            [
                (DEFAULT_DATE + timedelta(days=1), DEFAULT_DATE + timedelta(days=2)),
                (DEFAULT_DATE + timedelta(days=3), DEFAULT_DATE + timedelta(days=4)),
            ],
            False,
            TimeRestriction(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=4), False),
        ],
        [
            (DEFAULT_DATE, None),
            [(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=1)), (DEFAULT_DATE, None)],
            False,
            TimeRestriction(DEFAULT_DATE, None, False),
        ],
    ],
)
def test__time_restriction(dag_maker, dag_date, tasks_date, catchup, restrict):
    """
    Test that _time_restriction correctly reflects the DAG's time constraints with different catchup settings.

    With catchup=True, future task start dates are respected.
    With catchup=False, the scheduler may schedule tasks regardless of their future start dates.
    """
    with dag_maker(
        "test__time_restriction",
        schedule=None,
        catchup=catchup,  # Use the parametrized catchup value
        start_date=dag_date[0],
        end_date=dag_date[1],
    ) as dag:
        EmptyOperator(task_id="do1", start_date=tasks_date[0][0], end_date=tasks_date[0][1])
        EmptyOperator(task_id="do2", start_date=tasks_date[1][0], end_date=tasks_date[1][1])

    assert dag._time_restriction == restrict


def test_get_asset_triggered_next_run_info(dag_maker, clear_assets):
    asset1 = Asset(uri="test://asset1", name="test_asset1", group="test-group")
    asset2 = Asset(uri="test://asset2", group="test-group")
    asset3 = Asset(uri="test://asset3", group="test-group")
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
    asset1_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset1.uri))
    session.bulk_save_objects(
        [
            AssetDagRunQueue(asset_id=asset1_id, target_dag_id=dag2.dag_id),
            AssetDagRunQueue(asset_id=asset1_id, target_dag_id=dag3.dag_id),
        ]
    )
    session.flush()

    assets = session.execute(select(AssetModel.uri).order_by(AssetModel.id)).all()

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
def test_get_asset_triggered_next_run_info_with_unresolved_asset_alias(dag_maker, clear_assets):
    asset_alias1 = AssetAlias(name="alias")
    with dag_maker(dag_id="dag-1", schedule=[asset_alias1]):
        pass
    dag1 = dag_maker.dag
    session = dag_maker.session
    session.flush()

    info = get_asset_triggered_next_run_info([dag1.dag_id], session=session)
    assert info == {}

    dag1_model = DagModel.get_dagmodel(dag1.dag_id)
    assert dag1_model.get_asset_triggered_next_run_info(session=session) is None


@pytest.mark.parametrize(
    "run_id_type",
    [DagRunType.BACKFILL_JOB, DagRunType.SCHEDULED, DagRunType.ASSET_TRIGGERED],
)
def test_create_dagrun_disallow_manual_to_use_automated_run_id(run_id_type: DagRunType) -> None:
    dag = DAG(dag_id="test", start_date=DEFAULT_DATE, schedule="@daily")
    run_id = DagRun.generate_run_id(run_type=run_id_type, run_after=DEFAULT_DATE, logical_date=DEFAULT_DATE)

    with pytest.raises(
        ValueError,
        match=re.escape(
            f"A manual DAG run cannot use ID {run_id!r} since it is reserved for {run_id_type.value} runs"
        ),
    ):
        create_scheduler_dag(dag).create_dagrun(
            run_type=DagRunType.MANUAL,
            run_id=run_id,
            logical_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            run_after=DEFAULT_DATE,
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
        )


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
                task_ids=[task.task_id],
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
                task_ids=task.task_id,
                include_downstream=not upstream,
                include_upstream=upstream,
            ).tasks
        )

    @staticmethod
    def cleared_neither(task):
        """Helper to return tasks that would be cleared if **upstream** selected."""
        return set(
            task.dag.partial_subset(
                task_ids=[task.task_id],
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
        """
        This is a dag with a setup / teardown at dag level and two task groups that have
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
        ("upstream", "downstream", "expected"),
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


@pytest.mark.parametrize(
    ("disable", "bundle_version", "expected"),
    [
        (True, "some-version", None),
        (False, "some-version", "some-version"),
    ],
)
def test_disable_bundle_versioning(disable, bundle_version, expected, dag_maker, session, clear_dags):
    """When bundle versioning is disabled for a dag, the dag run should not have a bundle version."""

    def hello():
        print("hello")

    with dag_maker(disable_bundle_versioning=disable, session=session, serialized=True) as dag:
        PythonOperator(task_id="hi", python_callable=hello)

    assert dag.disable_bundle_versioning is disable

    # the dag *always* has bundle version
    dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == dag.dag_id))
    dag_model.bundle_version = bundle_version
    session.commit()

    dr = dag.create_dagrun(
        run_id="abcoercuhcrh",
        run_after=pendulum.now(),
        run_type="manual",
        triggered_by=DagRunTriggeredByType.TEST,
        state=None,
    )

    # but it only gets stamped on the dag run when bundle versioning not disabled
    assert dr.bundle_version == expected


def test_get_run_data_interval():
    with DAG("dag", schedule=None, start_date=DEFAULT_DATE) as dag:
        EmptyOperator(task_id="empty_task")

    dr = _create_dagrun(
        dag,
        logical_date=timezone.utcnow(),
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        run_type=DagRunType.MANUAL,
    )
    timetable = coerce_to_core_timetable(dag.timetable)
    assert get_run_data_interval(timetable, dr) == DataInterval(start=DEFAULT_DATE, end=DEFAULT_DATE)


@pytest.mark.need_serialized_dag
def test_get_run_data_interval_pre_aip_39():
    with DAG(
        "dag",
        schedule="0 0 * * *",
        start_date=DEFAULT_DATE,
    ) as dag:
        EmptyOperator(task_id="empty_task")

    current_ts = timezone.utcnow()
    dr = _create_dagrun(
        dag,
        logical_date=current_ts,
        data_interval=(None, None),
        run_type=DagRunType.MANUAL,
    )
    ds_start = current_ts.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    ds_end = current_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    timetable = coerce_to_core_timetable(dag.timetable)
    assert get_run_data_interval(timetable, dr) == DataInterval(start=ds_start, end=ds_end)
