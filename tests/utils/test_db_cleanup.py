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

from contextlib import suppress
from datetime import datetime
from importlib import import_module
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch
from uuid import uuid4

import pendulum
import pytest
from pytest import param
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import DeclarativeMeta

from airflow import AirflowException
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.db_cleanup import (
    ARCHIVE_TABLE_PREFIX,
    CreateTableAs,
    _build_query,
    _cleanup_table,
    _confirm_drop_archives,
    _dump_table_to_file,
    config_dict,
    export_cleaned_records,
    run_cleanup,
)
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_dags, clear_db_datasets, clear_db_runs, drop_tables_with_prefix


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    clear_db_datasets()
    clear_db_dags()
    yield  # Test runs here
    clear_db_dags()
    clear_db_datasets()
    clear_db_runs()


class TestDBCleanup:
    @pytest.fixture(autouse=True)
    def clear_airflow_tables(self):
        drop_tables_with_prefix("_airflow_")

    @pytest.mark.parametrize(
        "kwargs, called",
        [
            param(dict(confirm=True), True, id="true"),
            param(dict(), True, id="not supplied"),
            param(dict(confirm=False), False, id="false"),
        ],
    )
    @patch("airflow.utils.db_cleanup._cleanup_table", new=MagicMock())
    @patch("airflow.utils.db_cleanup._confirm_delete")
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
        "kwargs, should_skip",
        [
            param(dict(skip_archive=True), True, id="true"),
            param(dict(), False, id="not supplied"),
            param(dict(skip_archive=False), False, id="false"),
        ],
    )
    @patch("airflow.utils.db_cleanup._cleanup_table")
    def test_run_cleanup_skip_archive(self, cleanup_table_mock, kwargs, should_skip):
        """test that delete confirmation input is called when appropriate"""
        run_cleanup(
            clean_before_timestamp=None,
            table_names=["log"],
            dry_run=None,
            verbose=None,
            confirm=False,
            **kwargs,
        )
        assert cleanup_table_mock.call_args[1]["skip_archive"] is should_skip

    @pytest.mark.parametrize(
        "table_names",
        [
            ["xcom", "log"],
            None,
        ],
    )
    @patch("airflow.utils.db_cleanup._cleanup_table")
    @patch("airflow.utils.db_cleanup._confirm_delete", new=MagicMock())
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

    @patch("airflow.utils.db_cleanup._cleanup_table")
    @patch("airflow.utils.db_cleanup._confirm_delete")
    def test_validate_tables_all_invalid(self, confirm_delete_mock, clean_table_mock):
        """If only invalid tables are provided, don't try cleaning anything"""
        base_kwargs = dict(
            clean_before_timestamp=None,
            dry_run=None,
            verbose=None,
        )
        with pytest.raises(SystemExit) as execinfo:
            run_cleanup(**base_kwargs, table_names=["all", "fake"])
        assert "No tables selected for db cleanup" in str(execinfo.value)
        confirm_delete_mock.assert_not_called()

    @pytest.mark.parametrize(
        "dry_run",
        [None, True, False],
    )
    @patch("airflow.utils.db_cleanup._build_query", MagicMock())
    @patch("airflow.utils.db_cleanup._confirm_delete", MagicMock())
    @patch("airflow.utils.db_cleanup._check_for_rows")
    @patch("airflow.utils.db_cleanup._do_delete")
    def test_run_cleanup_dry_run(self, do_delete, check_rows_mock, dry_run):
        """Delete should only be called when not dry_run"""
        check_rows_mock.return_value = 10
        base_kwargs = dict(
            table_names=["log"],
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
        "table_name, date_add_kwargs, expected_to_delete, external_trigger",
        [
            param("task_instance", dict(days=0), 0, False, id="beginning"),
            param("task_instance", dict(days=4), 4, False, id="middle"),
            param("task_instance", dict(days=9), 9, False, id="end_exactly"),
            param("task_instance", dict(days=9, microseconds=1), 10, False, id="beyond_end"),
            param("dag_run", dict(days=9, microseconds=1), 9, False, id="beyond_end_dr"),
            param("dag_run", dict(days=9, microseconds=1), 10, True, id="beyond_end_dr_external"),
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
        base_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.timezone("UTC"))
        create_tis(
            base_date=base_date,
            num_tis=10,
            external_trigger=external_trigger,
        )
        target_table_name = "_airflow_temp_table_name"
        with create_session() as session:
            clean_before_date = base_date.add(**date_add_kwargs)
            query = _build_query(
                **config_dict[table_name].__dict__,
                clean_before_timestamp=clean_before_date,
                session=session,
            )
            stmt = CreateTableAs(target_table_name, query.selectable)
            session.execute(stmt)
            res = session.execute(f"SELECT COUNT(1) FROM {target_table_name}")
            for row in res:
                assert row[0] == expected_to_delete

    @pytest.mark.parametrize(
        "table_name, date_add_kwargs, expected_to_delete, external_trigger",
        [
            param("task_instance", dict(days=0), 0, False, id="beginning"),
            param("task_instance", dict(days=4), 4, False, id="middle"),
            param("task_instance", dict(days=9), 9, False, id="end_exactly"),
            param("task_instance", dict(days=9, microseconds=1), 10, False, id="beyond_end"),
            param("dag_run", dict(days=9, microseconds=1), 9, False, id="beyond_end_dr"),
            param("dag_run", dict(days=9, microseconds=1), 10, True, id="beyond_end_dr_external"),
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
        base_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.timezone("UTC"))
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
                table_names=["dag_run", "task_instance"],
            )
            model = config_dict[table_name].orm_model
            expected_remaining = num_tis - expected_to_delete
            assert len(session.query(model).all()) == expected_remaining
            if model.name == "task_instance":
                assert len(session.query(DagRun).all()) == num_tis
            elif model.name == "dag_run":
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
            for _, name, _ in pkgutil.iter_modules([str(proj_root / "airflow/models")])
        )

        all_models = {}
        for mod_name in mods:
            mod = import_module(mod_name)

            for table_name, class_ in mod.__dict__.items():
                if isinstance(class_, DeclarativeMeta):
                    with suppress(AttributeError):
                        all_models.update({class_.__tablename__: class_})
        exclusion_list = {
            "ab_user",
            "variable",  # leave alone
            "dataset",  # not good way to know if "stale"
            "trigger",  # self-maintaining
            "task_map",  # keys to TI, so no need
            "serialized_dag",  # handled through FK to Dag
            "log_template",  # not a significant source of data; age not indicative of staleness
            "dag_tag",  # not a significant source of data; age not indicative of staleness,
            "dag_owner_attributes",  # not a significant source of data; age not indicative of staleness,
            "dag_pickle",  # unsure of consequences
            "dag_code",  # self-maintaining
            "dag_warning",  # self-maintaining
            "connection",  # leave alone
            "slot_pool",  # leave alone
            "dag_schedule_dataset_reference",  # leave alone for now
            "task_outlet_dataset_reference",  # leave alone for now
            "dataset_dag_run_queue",  # self-managed
            "dataset_event_dag_run",  # foreign keys
            "task_instance_note",  # foreign keys
            "dag_run_note",  # foreign keys
            "rendered_task_instance_fields",  # foreign key with TI
        }

        from airflow.utils.db_cleanup import config_dict

        print(f"all_models={set(all_models)}")
        print(f"excl+conf={exclusion_list.union(config_dict)}")
        assert set(all_models) - exclusion_list.union(config_dict) == set()
        assert exclusion_list.isdisjoint(config_dict)

    def test_no_failure_warnings(self, caplog):
        """
        Ensure every table we have configured (and that is present in the db) can be cleaned successfully.
        For example, this checks that the recency column is actually a column.
        """
        run_cleanup(clean_before_timestamp=datetime.utcnow(), dry_run=True)
        assert "Encountered error when attempting to clean table" not in caplog.text

        # Lets check we have the right error message just in case
        caplog.clear()
        with patch("airflow.utils.db_cleanup._cleanup_table", side_effect=OperationalError("oops", {}, None)):
            run_cleanup(clean_before_timestamp=datetime.utcnow(), table_names=["task_instance"], dry_run=True)
        assert "Encountered error when attempting to clean table" in caplog.text

    @pytest.mark.parametrize(
        "drop_archive",
        [True, False],
    )
    @patch("airflow.utils.db_cleanup._confirm_drop_archives")
    def test_confirm_drop_called_when_drop_archives_is_true(self, confirm_drop_mock, drop_archive):
        """test that drop confirmation input is called when appropriate"""
        export_cleaned_records(export_format="csv", output_path="path", drop_archives=drop_archive)
        if drop_archive:
            confirm_drop_mock.assert_called()
        else:
            confirm_drop_mock.assert_not_called()

    def test_confirm_drop_archives(self):
        tables = ["table1", "table2"]
        with patch("sys.stdout", new=StringIO()) as fake_out, patch(
            "builtins.input", side_effect=["drop archived tables"]
        ):
            _confirm_drop_archives(tables=tables)
            output = fake_out.getvalue().strip()
            expected = (
                f"You have requested that we drop archived records for tables {tables}.\n"
                "This is irreversible.  Consider backing up the tables first \n"
                "Enter 'drop archived tables' (without quotes) to proceed."
            )
        assert output == expected

    def test_user_did_not_confirm(self):
        tables = ["table1", "table2"]
        with pytest.raises(SystemExit) as cm, patch(
            "builtins.input", side_effect=["not drop archived tables"]
        ):
            _confirm_drop_archives(tables=tables)
        assert str(cm.value) == "User did not confirm; exiting."

    @pytest.mark.parametrize("drop_archive", [True, False])
    @patch("airflow.utils.db_cleanup._dump_table_to_file")
    @patch("airflow.utils.db_cleanup.inspect")
    @patch("builtins.input", side_effect=["drop archived tables"])
    def test_export_cleaned_records_only_archived_tables(
        self, mock_input, inspect_mock, dump_mock, caplog, drop_archive
    ):
        """Test export_cleaned_records and show that only tables with the archive prefix are exported."""
        session_mock = MagicMock()
        inspector = inspect_mock.return_value
        inspector.get_table_names.return_value = [f"{ARCHIVE_TABLE_PREFIX}dag_run__233", "task_instance"]
        export_cleaned_records(
            export_format="csv", output_path="path", drop_archives=drop_archive, session=session_mock
        )
        dump_mock.assert_called_once_with(
            target_table=f"{ARCHIVE_TABLE_PREFIX}dag_run__233",
            file_path=f"path/{ARCHIVE_TABLE_PREFIX}dag_run__233.csv",
            export_format="csv",
            session=session_mock,
        )
        assert f"Exporting table {ARCHIVE_TABLE_PREFIX}dag_run__233" in caplog.text

        if drop_archive:
            assert "Total exported tables: 1, Total dropped tables: 1" in caplog.text
        else:
            assert "Total exported tables: 1, Total dropped tables: 0" in caplog.text

    @patch("airflow.utils.db_cleanup.csv")
    def test_dump_table_to_file_function_for_csv(self, mock_csv):
        mockopen = mock_open()
        with patch("airflow.utils.db_cleanup.open", mockopen, create=True):
            _dump_table_to_file(
                target_table="mytable", file_path="dags/myfile.csv", export_format="csv", session=MagicMock()
            )
            mockopen.assert_called_once_with("dags/myfile.csv", "w")
            writer = mock_csv.writer
            writer.assert_called_once()
            writer.return_value.writerow.assert_called_once()
            writer.return_value.writerows.assert_called_once()

    def test_dump_table_to_file_raises_if_format_not_supported(self):
        with pytest.raises(AirflowException) as exc_info:
            _dump_table_to_file(
                target_table="mytable",
                file_path="dags/myfile.json",
                export_format="json",
                session=MagicMock(),
            )
        assert "Export format json is not supported" in str(exc_info.value)


def create_tis(base_date, num_tis, external_trigger=False):
    with create_session() as session:
        dag = DagModel(dag_id=f"test-dag_{uuid4()}")
        session.add(dag)
        for num in range(num_tis):
            start_date = base_date.add(days=num)
            dag_run = DagRun(
                dag.dag_id,
                run_id=f"abc_{num}",
                run_type="none",
                start_date=start_date,
                external_trigger=external_trigger,
            )
            ti = TaskInstance(
                PythonOperator(task_id="dummy-task", python_callable=print), run_id=dag_run.run_id
            )
            ti.dag_id = dag.dag_id
            ti.start_date = start_date
            session.add(dag_run)
            session.add(ti)
        session.commit()
