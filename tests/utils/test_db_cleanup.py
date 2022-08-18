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
from contextlib import suppress
from importlib import import_module
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pendulum
import pytest
from pytest import param
from sqlalchemy.ext.declarative import DeclarativeMeta

from airflow.models import DagModel, DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.db_cleanup import _build_query, _cleanup_table, config_dict, run_cleanup
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_dags, clear_db_runs, drop_tables_with_prefix


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    clear_db_dags()
    yield  # Test runs here
    clear_db_dags()
    clear_db_runs()


class TestDBCleanup:
    @pytest.fixture(autouse=True)
    def clear_airflow_tables(self):
        drop_tables_with_prefix('_airflow_')

    @pytest.mark.parametrize(
        'kwargs, called',
        [
            param(dict(confirm=True), True, id='true'),
            param(dict(), True, id='not supplied'),
            param(dict(confirm=False), False, id='false'),
        ],
    )
    @patch('airflow.utils.db_cleanup._cleanup_table', new=MagicMock())
    @patch('airflow.utils.db_cleanup._confirm_delete')
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
        'kwargs, should_skip',
        [
            param(dict(skip_archive=True), True, id='true'),
            param(dict(), False, id='not supplied'),
            param(dict(skip_archive=False), False, id='false'),
        ],
    )
    @patch('airflow.utils.db_cleanup._cleanup_table')
    def test_run_cleanup_skip_archive(self, cleanup_table_mock, kwargs, should_skip):
        """test that delete confirmation input is called when appropriate"""
        run_cleanup(
            clean_before_timestamp=None,
            table_names=['log'],
            dry_run=None,
            verbose=None,
            confirm=False,
            **kwargs,
        )
        assert cleanup_table_mock.call_args[1]['skip_archive'] is should_skip

    @pytest.mark.parametrize(
        'table_names',
        [
            ['xcom', 'log'],
            None,
        ],
    )
    @patch('airflow.utils.db_cleanup._cleanup_table')
    @patch('airflow.utils.db_cleanup._confirm_delete', new=MagicMock())
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
    @patch('airflow.utils.db_cleanup._build_query', MagicMock())
    @patch('airflow.utils.db_cleanup._confirm_delete', MagicMock())
    @patch('airflow.utils.db_cleanup._check_for_rows')
    @patch('airflow.utils.db_cleanup._do_delete')
    def test_run_cleanup_dry_run(self, do_delete, check_rows_mock, dry_run):
        """Delete should only be called when not dry_run"""
        check_rows_mock.return_value = 10
        base_kwargs = dict(
            table_names=['log'],
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
        'table_name, date_add_kwargs, expected_to_delete, external_trigger',
        [
            param('task_instance', dict(days=0), 0, False, id='beginning'),
            param('task_instance', dict(days=4), 4, False, id='middle'),
            param('task_instance', dict(days=9), 9, False, id='end_exactly'),
            param('task_instance', dict(days=9, microseconds=1), 10, False, id='beyond_end'),
            param('dag_run', dict(days=9, microseconds=1), 9, False, id='beyond_end_dr'),
            param('dag_run', dict(days=9, microseconds=1), 10, True, id='beyond_end_dr_external'),
        ],
    )
    def test__build_query(self, table_name, date_add_kwargs, expected_to_delete, external_trigger):
        """
        Verify that ``_build_query`` produces a query that would delete the right
        task instance records depending on the value of ``clean_before_timestamp``.

        DagRun is a special case where we always keep the last dag run even if
        the ``clean_before_timestamp`` is in the future, except for
        externally-triggered dag runs. That is, only the last non-externally-triggered
        dag run is kept.

        """
        base_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.timezone('UTC'))
        create_tis(
            base_date=base_date,
            num_tis=10,
            external_trigger=external_trigger,
        )
        with create_session() as session:
            clean_before_date = base_date.add(**date_add_kwargs)
            query = _build_query(
                **config_dict[table_name].__dict__,
                clean_before_timestamp=clean_before_date,
                session=session,
            )
            assert len(query.all()) == expected_to_delete

    @pytest.mark.parametrize(
        'table_name, date_add_kwargs, expected_to_delete, external_trigger',
        [
            param('task_instance', dict(days=0), 0, False, id='beginning'),
            param('task_instance', dict(days=4), 4, False, id='middle'),
            param('task_instance', dict(days=9), 9, False, id='end_exactly'),
            param('task_instance', dict(days=9, microseconds=1), 10, False, id='beyond_end'),
            param('dag_run', dict(days=9, microseconds=1), 9, False, id='beyond_end_dr'),
            param('dag_run', dict(days=9, microseconds=1), 10, True, id='beyond_end_dr_external'),
        ],
    )
    def test__cleanup_table(self, table_name, date_add_kwargs, expected_to_delete, external_trigger):
        """
        Verify that _cleanup_table actually deletes the rows it should.

        TaskInstance represents the "normal" case.  DagRun is the odd case where we want
        to keep the last non-externally-triggered DagRun record even if if it should be
        deleted according to the provided timestamp.

        We also verify that the "on delete cascade" behavior is as expected.  Some tables
        have foreign keys defined so for example if we delete a dag run, all its associated
        task instances should be purged as well.  But if we delete task instances the
        associated dag runs should remain.

        """
        base_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.timezone('UTC'))
        num_tis = 10
        create_tis(
            base_date=base_date,
            num_tis=num_tis,
            external_trigger=external_trigger,
        )
        with create_session() as session:
            clean_before_date = base_date.add(**date_add_kwargs)
            _cleanup_table(
                **config_dict[table_name].__dict__,
                clean_before_timestamp=clean_before_date,
                dry_run=False,
                session=session,
                table_names=['dag_run', 'task_instance'],
            )
            model = config_dict[table_name].orm_model
            expected_remaining = num_tis - expected_to_delete
            assert len(session.query(model).all()) == expected_remaining
            if model.name == 'task_instance':
                assert len(session.query(DagRun).all()) == num_tis
            elif model.name == 'dag_run':
                assert len(session.query(TaskInstance).all()) == expected_remaining
            else:
                raise Exception("unexpected")

    def test_no_models_missing(self):
        """
        1. Verify that for all tables in `airflow.models`, we either have them enabled in db cleanup,
        or documented in the exclusion list in this test.
        2. Verify that no table is enabled for db cleanup and also in exclusion list.
        """
        import pkgutil

        proj_root = Path(__file__).parents[2].resolve()
        mods = list(
            f"airflow.models.{name}"
            for _, name, _ in pkgutil.iter_modules([str(proj_root / 'airflow/models')])
        )

        all_models = {}
        for mod_name in mods:
            mod = import_module(mod_name)

            for table_name, class_ in mod.__dict__.items():
                if isinstance(class_, DeclarativeMeta):
                    with suppress(AttributeError):
                        all_models.update({class_.__tablename__: class_})
        exclusion_list = {
            'variable',  # leave alone
            'dataset',  # not good way to know if "stale"
            'trigger',  # self-maintaining
            'task_map',  # keys to TI, so no need
            'serialized_dag',  # handled through FK to Dag
            'log_template',  # not a significant source of data; age not indicative of staleness
            'dag_tag',  # not a significant source of data; age not indicative of staleness,
            'dag_owner_attributes',  # not a significant source of data; age not indicative of staleness,
            'dag_pickle',  # unsure of consequences
            'dag_code',  # self-maintaining
            'dag_warning',  # self-maintaining
            'connection',  # leave alone
            'slot_pool',  # leave alone
            'dataset_dag_ref',  # leave alone for now
            'dataset_task_ref',  # leave alone for now
            'dataset_dag_run_queue',  # self-managed
            'dataset_event_dag_run',  # foreign keys
        }

        from airflow.utils.db_cleanup import config_dict

        print(f"all_models={set(all_models)}")
        print(f"excl+conf={exclusion_list.union(config_dict)}")
        assert set(all_models) - exclusion_list.union(config_dict) == set()
        assert exclusion_list.isdisjoint(config_dict)


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
