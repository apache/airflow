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
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pendulum
import pytest
from pytest import param

from airflow.models import DagModel, DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.metastore_cleanup import _build_query, config_dict, run_cleanup
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_dags, clear_db_runs


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    clear_db_dags()
    yield  # Test runs here
    clear_db_dags()
    clear_db_runs()


class TestMetastoreCleanup:
    @pytest.mark.parametrize(
        'kwargs, called',
        [
            param(dict(confirm=True), True, id='true'),
            param(dict(), True, id='not supplied'),
            param(dict(confirm=False), False, id='false'),
        ],
    )
    @patch('airflow.utils.metastore_cleanup._cleanup_table', new=MagicMock())
    @patch('airflow.utils.metastore_cleanup._confirm_delete')
    def test_run_cleanup_confirm(self, confirm_delete_mock, kwargs, called):
        """test that delete confirmation input is called when appropriate"""
        run_cleanup(
            clean_before_timestamp=None,
            table_names=None,
            dry_run=None,
            verbose=None,
            **kwargs,
        )
        if called:
            confirm_delete_mock.assert_called()
        else:
            confirm_delete_mock.assert_not_called()

    @pytest.mark.parametrize(
        'table_names',
        [
            ['xcom', 'log'],
            None,
        ],
    )
    @patch('airflow.utils.metastore_cleanup._cleanup_table')
    @patch('airflow.utils.metastore_cleanup._confirm_delete', new=MagicMock())
    def test_run_cleanup_tables(self, clean_table_mock, table_names):
        """
        ``_cleanup_table`` should be called for each table in subset if one
        is provided else should be called for all tables.
        """
        base_kwargs = dict(
            clean_before_timestamp=None,
            dry_run=None,
            verbose=None,
        )
        run_cleanup(**base_kwargs, table_names=table_names)
        assert clean_table_mock.call_count == len(table_names) if table_names else len(config_dict)

    @pytest.mark.parametrize(
        'dry_run',
        [None, True, False],
    )
    @patch('airflow.utils.metastore_cleanup._build_query', MagicMock())
    @patch('airflow.utils.metastore_cleanup._print_entities', MagicMock())
    @patch('airflow.utils.metastore_cleanup._do_delete')
    @patch('airflow.utils.metastore_cleanup._confirm_delete', MagicMock())
    def test_run_cleanup_dry_run(self, do_delete, dry_run):
        """Delete should only be called when not dry_run"""
        base_kwargs = dict(
            clean_before_timestamp=None,
            dry_run=dry_run,
            verbose=None,
        )
        run_cleanup(
            **base_kwargs,
        )
        if dry_run:
            do_delete.assert_not_called()
        else:
            do_delete.assert_called()

    @pytest.mark.parametrize(
        'pendulum_add_kwargs, expected_to_delete',
        [
            param(dict(days=0), 0, id='beginning'),
            param(dict(days=4), 4, id='middle'),
            param(dict(days=9), 9, id='end_exactly'),
            param(dict(days=9, microseconds=1), 10, id='beyond_end'),
        ],
    )
    def test__build_query_non_keep_last(self, pendulum_add_kwargs, expected_to_delete):
        """
        Verify that ``_build_query`` produces a query that would delete the right
        task instance records depending on the value of ``clean_before_timestamp``.
        """
        base_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.timezone('America/Los_Angeles'))
        create_tis(
            base_date=base_date,
            num_tis=10,
        )
        with create_session() as session:
            tis = session.query(TaskInstance).all()
            assert len(tis) == 10

        with create_session() as session:
            clean_before_date = base_date.add(**pendulum_add_kwargs)
            query = _build_query(
                **config_dict['task_instance'].__dict__,
                clean_before_timestamp=clean_before_date,
                session=session,
            )
            assert len(query.all()) == expected_to_delete

    @pytest.mark.parametrize(
        'pendulum_add_kwargs, expected_to_delete, external_trigger',
        [
            param(dict(days=0), 0, False, id='beginning'),
            param(dict(days=4), 4, False, id='middle'),
            param(dict(days=9), 9, False, id='end_exactly'),
            param(dict(days=9, microseconds=1), 9, False, id='beyond_end'),
            param(dict(days=9, microseconds=1), 10, True, id='beyond_end_external'),
        ],
    )
    def test__build_query_keep_last(self, pendulum_add_kwargs, expected_to_delete, external_trigger):
        """
        Verify that ``_build_query`` produces a query that would delete the right
        dag run records depending on the value of ``clean_before_timestamp``.

        DagRun is a special case where we always keep the last dag run even if
        the ``clean_before_timestamp`` is in the future and we verify this behavior as well.
        """
        base_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.timezone('America/Los_Angeles'))
        create_tis(
            base_date=base_date,
            num_tis=10,
            external_trigger=external_trigger,
        )
        with create_session() as session:
            tis = session.query(TaskInstance).all()
            assert len(tis) == 10

        with create_session() as session:
            clean_before_date = base_date.add(**pendulum_add_kwargs)
            query = _build_query(
                **config_dict['dag_run'].__dict__, clean_before_timestamp=clean_before_date, session=session
            )
            assert len(query.all()) == expected_to_delete


def create_tis(base_date, num_tis, external_trigger=False):
    with create_session() as session:
        dag = DagModel(dag_id=f'test-dag_{uuid4()}')
        session.add(dag)
        for num in range(num_tis):
            start_date = base_date.add(days=num)
            dag_run = DagRun(
                dag.dag_id,
                run_id=f'abc_{num}',
                run_type='none',
                start_date=start_date,
                external_trigger=external_trigger,
            )
            ti = TaskInstance(
                PythonOperator(task_id='dummy-task', python_callable=print), run_id=dag_run.run_id
            )
            ti.dag_id = dag.dag_id
            ti.start_date = start_date
            session.add(dag_run)
            session.add(ti)
        session.commit()
