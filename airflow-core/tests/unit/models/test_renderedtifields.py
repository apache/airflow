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
"""Unit tests for RenderedTaskInstanceFields."""

from __future__ import annotations

import os
from collections import Counter
from datetime import date, timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest
from sqlalchemy import select

from airflow import settings
from airflow._shared.timezones.timezone import datetime
from airflow.configuration import conf
from airflow.models import DagRun
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.taskmap import TaskMap
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task as task_decorator
from airflow.utils.sqlalchemy import get_dialect_name
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_rendered_ti_fields

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2018, 1, 1)
EXECUTION_DATE = datetime(2019, 1, 1)


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


class LargeStrObject:
    def __init__(self):
        self.a = "a" * 5000

    def __str__(self):
        return self.a


max_length = conf.getint("core", "max_templated_field_length")


def _get_mysql_margin(session) -> int:
    """Return extra query margin for MySQL (which fetches run_ids separately due to LIMIT subquery limitation)."""
    return 1 if get_dialect_name(session) == "mysql" else 0


class TestRenderedTaskInstanceFields:
    """Unit tests for RenderedTaskInstanceFields."""

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_dags()
        clear_rendered_ti_fields()

    def setup_method(self):
        self.clean_db()

    def teardown_method(self):
        self.clean_db()

    @pytest.mark.parametrize(
        ("templated_field", "expected_rendered_field"),
        [
            pytest.param(None, None, id="None"),
            pytest.param([], [], id="list"),
            pytest.param({}, {}, id="empty_dict"),
            pytest.param((), [], id="empty_tuple"),
            pytest.param(set(), "set()", id="empty_set"),
            pytest.param("test-string", "test-string", id="string"),
            pytest.param({"foo": "bar"}, {"foo": "bar"}, id="dict"),
            pytest.param(("foo", "bar"), ["foo", "bar"], id="tuple"),
            pytest.param({"foo"}, "{'foo'}", id="set"),
            (date(2018, 12, 6), "2018-12-06"),
            pytest.param(datetime(2018, 12, 6, 10, 55), "2018-12-06 10:55:00+00:00", id="datetime"),
            pytest.param(
                "a" * 5000,
                f"Truncated. You can change this behaviour in [core]max_templated_field_length. {('a' * 5000)[: max_length - 79]!r}... ",
                id="large_string",
            ),
            pytest.param(
                LargeStrObject(),
                f"Truncated. You can change this behaviour in "
                f"[core]max_templated_field_length. {str(LargeStrObject())[: max_length - 79]!r}... ",
                id="large_object",
            ),
        ],
    )
    def test_get_templated_fields(self, templated_field, expected_rendered_field, dag_maker):
        """
        Test that template_fields are rendered correctly, stored in the Database,
        and are correctly fetched using RTIF.get_templated_fields
        """
        with dag_maker("test_serialized_rendered_fields"):
            task = BashOperator(task_id="test", bash_command=templated_field)
            task_2 = BashOperator(task_id="test2", bash_command=templated_field)
        dr = dag_maker.create_dagrun()

        session = dag_maker.session

        ti, ti2 = dr.task_instances
        ti.task = task
        ti2.task = task_2
        rtif = RTIF(ti=ti, render_templates=False)

        assert ti.dag_id == rtif.dag_id
        assert ti.task_id == rtif.task_id
        assert ti.run_id == rtif.run_id
        assert expected_rendered_field == rtif.rendered_fields.get("bash_command")

        session.add(rtif)
        session.flush()

        assert RTIF.get_templated_fields(ti=ti, session=session) == {
            "bash_command": expected_rendered_field,
            "env": None,
            "cwd": None,
        }
        # Test the else part of get_templated_fields
        # i.e. for the TIs that are not stored in RTIF table
        # Fetching them will return None
        assert RTIF.get_templated_fields(ti=ti2) is None

    @mock.patch("airflow.models.BaseOperator.render_template")
    def test_pandas_dataframes_works_with_the_string_compare(self, render_mock, dag_maker):
        """Test that rendered dataframe gets passed through the serialized template fields."""
        import pandas

        render_mock.return_value = pandas.DataFrame({"a": [1, 2, 3]})
        with dag_maker("test_serialized_rendered_fields"):

            @task_decorator
            def generate_pd():
                return pandas.DataFrame({"a": [1, 2, 3]})

            @task_decorator
            def consume_pd(data):
                return data

            consume_pd(generate_pd())

        dr = dag_maker.create_dagrun()
        ti, ti2 = dr.task_instances
        rtif = RTIF(ti=ti2, render_templates=False)
        rtif.write()

    @pytest.mark.parametrize(
        ("rtif_num", "num_to_keep", "remaining_rtifs", "expected_query_count"),
        [
            (0, 1, 0, 1),
            (1, 1, 1, 1),
            (1, 0, 1, 0),
            (3, 1, 1, 1),
            (4, 2, 2, 1),
            (5, 2, 2, 1),
        ],
    )
    def test_delete_old_records(
        self, rtif_num, num_to_keep, remaining_rtifs, expected_query_count, dag_maker, session
    ):
        """
        Test that RTIF records from older dag runs are deleted, keeping only
        records from the most recent N dag runs for a given task_id and dag_id.
        """
        with dag_maker("test_delete_old_records") as dag:
            task = BashOperator(task_id="test", bash_command="echo {{ ds }}")
        rtif_list = []
        for num in range(rtif_num):
            dr = dag_maker.create_dagrun(run_id=str(num), logical_date=dag.start_date + timedelta(days=num))
            ti = dr.task_instances[0]
            ti.task = task
            rtif_list.append(RTIF(ti, render_templates=False))

        session.add_all(rtif_list)
        session.flush()

        result = session.scalars(
            select(RTIF).where(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id)
        ).all()

        for rtif in rtif_list:
            assert rtif in result

        assert rtif_num == len(result)

        with assert_queries_count(expected_query_count, margin=_get_mysql_margin(session)):
            RTIF.delete_old_records(task_id=task.task_id, dag_id=task.dag_id, num_to_keep=num_to_keep)
        result = session.scalars(
            select(RTIF).where(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id)
        ).all()
        assert remaining_rtifs == len(result)

    @pytest.mark.parametrize(
        ("num_runs", "num_to_keep", "remaining_rtifs", "expected_query_count"),
        [
            (3, 1, 1, 1),
            (4, 2, 2, 1),
            (5, 2, 2, 1),
        ],
    )
    def test_delete_old_records_mapped(
        self, num_runs, num_to_keep, remaining_rtifs, expected_query_count, dag_maker, session
    ):
        """
        Test that RTIF records from older dag runs are deleted for mapped tasks,
        keeping all map_index records together for the most recent N dag runs.
        """
        with dag_maker("test_delete_old_records", session=session, serialized=True) as dag:
            mapped = BashOperator.partial(task_id="mapped").expand(bash_command=["a", "b"])
        for num in range(num_runs):
            dr = dag_maker.create_dagrun(
                run_id=f"run_{num}", logical_date=dag.start_date + timedelta(days=num)
            )

            TaskMap.expand_mapped_task(dag.task_dict[mapped.task_id], dr.run_id, session=dag_maker.session)
            session.refresh(dr)
            for ti in dr.task_instances:
                ti.task = dag_maker.serialized_dag.get_task(ti.task_id)
                session.add(RTIF(ti, render_templates=False))
        session.flush()

        result = session.scalars(select(RTIF).where(RTIF.dag_id == dag.dag_id)).all()
        assert len(result) == num_runs * 2

        with assert_queries_count(expected_query_count, margin=_get_mysql_margin(session)):
            RTIF.delete_old_records(
                task_id=mapped.task_id, dag_id=dr.dag_id, num_to_keep=num_to_keep, session=session
            )
        result = session.scalars(
            select(RTIF).where(RTIF.dag_id == dag.dag_id, RTIF.task_id == mapped.task_id)
        ).all()
        rtif_num_runs = Counter(rtif.run_id for rtif in result)
        assert len(rtif_num_runs) == remaining_rtifs
        # Check that we have _all_ the data for each row
        assert len(result) == remaining_rtifs * 2

    def test_delete_old_records_sparse_task(self, dag_maker, session):
        """
        Test deletion behavior for sparse tasks (tasks that don't run every dag run).

        Retention is based on the N most recent dag runs, not the N most recent
        task executions. For sparse tasks, this means RTIF records may be deleted
        even if fewer than N executions exist.
        """
        with dag_maker("test_sparse", session=session) as dag:
            task = BashOperator(task_id="sparse_task", bash_command="echo {{ ds }}")

        # Create 10 dag runs but only add RTIF for runs 0, 3, 6, 9 (every 3rd run)
        sparse_run_indices = [0, 3, 6, 9]
        for num in range(10):
            dr = dag_maker.create_dagrun(
                run_id=f"run_{num}", logical_date=dag.start_date + timedelta(days=num)
            )
            if num in sparse_run_indices:
                ti = dr.task_instances[0]
                ti.task = task
                session.add(RTIF(ti, render_templates=False))
        session.flush()

        # Verify we have 4 RTIF records
        result = session.scalars(
            select(RTIF).where(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id)
        ).all()
        assert len(result) == 4

        # With num_to_keep=5, we keep runs 5-9 (the 5 most recent dag runs).
        # Only runs 6 and 9 have RTIF records, so 2 should remain.
        RTIF.delete_old_records(task_id=task.task_id, dag_id=dag.dag_id, num_to_keep=5, session=session)
        result = session.scalars(
            select(RTIF).where(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id)
        ).all()
        assert len(result) == 2
        assert {r.run_id for r in result} == {"run_6", "run_9"}

    def test_write(self, dag_maker):
        """
        Test records can be written and overwritten
        """
        session = settings.Session()
        result = session.scalars(select(RTIF)).all()
        assert result == []

        with dag_maker("test_write"):
            task = BashOperator(task_id="test", bash_command="echo test_val")

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task

        rtif = RTIF(ti, render_templates=False)
        rtif.write()
        result = session.execute(
            select(RTIF.dag_id, RTIF.task_id, RTIF.rendered_fields).where(
                RTIF.dag_id == rtif.dag_id,
                RTIF.task_id == rtif.task_id,
                RTIF.run_id == rtif.run_id,
            )
        ).first()
        assert result == ("test_write", "test", {"bash_command": "echo test_val", "env": None, "cwd": None})

        # Test that overwrite saves new values to the DB
        self.clean_db()
        with dag_maker("test_write"):
            updated_task = BashOperator(task_id="test", bash_command="echo test_val_updated")
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = updated_task
        rtif_updated = RTIF(ti, render_templates=False)
        rtif_updated.write()

        result_updated = session.execute(
            select(RTIF.dag_id, RTIF.task_id, RTIF.rendered_fields).where(
                RTIF.dag_id == rtif_updated.dag_id,
                RTIF.task_id == rtif_updated.task_id,
                RTIF.run_id == rtif_updated.run_id,
            )
        ).first()
        assert result_updated == (
            "test_write",
            "test",
            {"bash_command": "echo test_val_updated", "env": None, "cwd": None},
        )

    @mock.patch.dict(os.environ, {"AIRFLOW_VAR_API_KEY": "secret"})
    def test_redact(self, dag_maker):
        with mock.patch("airflow._shared.secrets_masker.redact", autospec=True) as redact:
            with dag_maker("test_ritf_redact", serialized=True):
                task = BashOperator(
                    task_id="test",
                    bash_command="echo secret",
                    env={"foo": "secret", "other_api_key": "masked based on key name"},
                )
            dr = dag_maker.create_dagrun()
            redact.side_effect = [
                # Order depends on order in Operator template_fields
                "val 1",  # bash_command
                "val 2",  # env
                "val 3",  # cwd
            ]

            ti = dr.task_instances[0]
            ti.task = task
            rtif = RTIF(ti=ti, render_templates=False)
            assert rtif.rendered_fields == {
                "bash_command": "val 1",
                "env": "val 2",
                "cwd": "val 3",
            }

    def test_rtif_deletion_stale_data_error(self, dag_maker, session):
        """
        Here we verify bad behavior.  When we rerun a task whose RTIF
        will get removed, we get a stale data error.
        """
        with dag_maker(dag_id="test_retry_handling", session=session) as dag:
            task = PythonOperator(
                task_id="test_retry_handling_op",
                python_callable=lambda a: print(f"{a}\n"),
                op_args=[f"dag {dag.dag_id};"],
            )

        def popuate_rtif(date):
            run_id = f"abc_{date.to_date_string()}"
            dag_run = dag_maker.create_dagrun(logical_date=date, run_id=run_id)
            ti = dag_run.task_instances[0]
            ti.state = TaskInstanceState.SUCCESS

            rtif = RTIF(ti=ti, render_templates=False, rendered_fields={"a": "1"})
            session.merge(rtif)
            return dag_run

        base_date = pendulum.datetime(2021, 1, 1)
        exec_dates = [base_date.add(days=x) for x in range(40)]
        for when in exec_dates:
            popuate_rtif(date=when)

        session.commit()
        session.expunge_all()

        # find oldest dag run
        dr = session.scalar(select(DagRun).join(RTIF.dag_run).order_by(DagRun.run_after).limit(1))
        assert dr
        ti: TaskInstance = dr.task_instances[0]
        ti.state = None
        # rerun the old run. this shouldn't fail
        dag_maker.run_ti(task.task_id, dr)
