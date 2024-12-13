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

import logging
import warnings
from collections.abc import Generator
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError, SAWarning

import airflow.dag_processing.collection
from airflow.dag_processing.collection import (
    AssetModelOperation,
    _get_latest_runs_stmt,
    _sync_dag_perms,
    update_dag_parsing_results_in_db,
)
from airflow.exceptions import SerializationError
from airflow.models import DagModel, Trigger
from airflow.models.asset import (
    AssetActive,
    asset_trigger_association_table,
)
from airflow.models.dag import DAG
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk.definitions.asset import Asset
from airflow.utils import timezone as tz
from airflow.utils.session import create_session

from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_serialized_dags,
    clear_db_triggers,
)

if TYPE_CHECKING:
    from kgb import SpyAgency


def test_statement_latest_runs_one_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.id, dag_run.dag_id, dag_run.logical_date, "
            "dag_run.data_interval_start, dag_run.data_interval_end",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_1 AND dag_run.logical_date = ("
            "SELECT max(dag_run.logical_date) AS max_logical_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_2 AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]))",
        ]
        assert actual == expected, compiled_stmt


def test_statement_latest_runs_many_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag-1", "fake-dag-2"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.id, dag_run.dag_id, dag_run.logical_date, "
            "dag_run.data_interval_start, dag_run.data_interval_end",
            "FROM dag_run, (SELECT dag_run.dag_id AS dag_id, "
            "max(dag_run.logical_date) AS max_logical_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id IN (__[POSTCOMPILE_dag_id_1]) "
            "AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]) GROUP BY dag_run.dag_id) AS anon_1",
            "WHERE dag_run.dag_id = anon_1.dag_id AND dag_run.logical_date = anon_1.max_logical_date",
        ]
        assert actual == expected, compiled_stmt


@pytest.mark.db_test
class TestAssetModelOperation:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_assets()
        clear_db_triggers()

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        self.clean_db()
        yield
        self.clean_db()

    @pytest.mark.parametrize(
        "is_active, is_paused, expected_num_triggers",
        [
            (True, True, 0),
            (True, False, 1),
            (False, True, 0),
            (False, False, 0),
        ],
    )
    def test_add_asset_trigger_references(self, is_active, is_paused, expected_num_triggers, dag_maker):
        trigger = TimeDeltaTrigger(timedelta(seconds=0))
        asset = Asset("test_add_asset_trigger_references_asset", watchers=[trigger])

        with dag_maker(dag_id="test_add_asset_trigger_references_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

            asset_op = AssetModelOperation.collect({"test_add_asset_trigger_references_dag": dag})

        with create_session() as session:
            # Update `is_active` and `is_paused` properties from DAG
            dags = session.query(DagModel).all()
            for dag in dags:
                dag.is_active = is_active
                dag.is_paused = is_paused

            orm_assets = asset_op.add_assets(session=session)
            # Create AssetActive objects from assets. It is usually done in the scheduler
            for asset in orm_assets.values():
                session.add(AssetActive.for_asset(asset))
            session.commit()

            asset_op.add_asset_trigger_references(orm_assets, session=session)

            session.commit()

            assert session.query(Trigger).count() == expected_num_triggers
            assert session.query(asset_trigger_association_table).count() == expected_num_triggers


@pytest.mark.db_test
class TestUpdateDagParsingResults:
    """Tests centred around the ``update_dag_parsing_results_in_db`` function."""

    @pytest.fixture
    def clean_db(self, session):
        yield
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_import_errors()

    @pytest.mark.usefixtures("clean_db")  # sync_perms in fab has bad session commit hygiene
    def test_sync_perms_syncs_dag_specific_perms_on_update(
        self, monkeypatch, spy_agency: SpyAgency, session, time_machine
    ):
        """
        Test that dagbag.sync_to_db will sync DAG specific permissions when a DAG is
        new or updated
        """
        from airflow import settings

        serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
        assert serialized_dags_count == 0

        monkeypatch.setattr(settings, "MIN_SERIALIZED_DAG_UPDATE_INTERVAL", 5)
        time_machine.move_to(tz.datetime(2020, 1, 5, 0, 0, 0), tick=False)

        dag = DAG(dag_id="test")

        sync_perms_spy = spy_agency.spy_on(
            airflow.dag_processing.collection._sync_dag_perms,
            call_original=False,
        )

        def _sync_to_db():
            sync_perms_spy.reset_calls()
            time_machine.shift(20)

            update_dag_parsing_results_in_db([dag], dict(), None, set(), session)

        _sync_to_db()
        spy_agency.assert_spy_called_with(sync_perms_spy, dag, session=session)

        # DAG isn't updated
        _sync_to_db()
        spy_agency.assert_spy_not_called(sync_perms_spy)

        # DAG is updated
        dag.tags = {"new_tag"}
        _sync_to_db()
        spy_agency.assert_spy_called_with(sync_perms_spy, dag, session=session)

        serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()

    @patch.object(SerializedDagModel, "write_dag")
    @patch("airflow.models.dag.DAG.bulk_write_to_db")
    def test_sync_to_db_is_retried(self, mock_bulk_write_to_db, mock_s10n_write_dag, session):
        """Test that important DB operations in db sync are retried on OperationalError"""

        serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
        assert serialized_dags_count == 0
        mock_dag = mock.MagicMock()
        dags = [mock_dag]

        op_error = OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        # Mock error for the first 2 tries and a successful third try
        side_effect = [op_error, op_error, mock.ANY]

        mock_bulk_write_to_db.side_effect = side_effect

        mock_session = mock.MagicMock()
        update_dag_parsing_results_in_db(
            dags=dags, import_errors={}, processor_subdir=None, warnings=set(), session=mock_session
        )

        # Test that 3 attempts were made to run 'DAG.bulk_write_to_db' successfully
        mock_bulk_write_to_db.assert_has_calls(
            [
                mock.call(mock.ANY, processor_subdir=None, session=mock.ANY),
                mock.call(mock.ANY, processor_subdir=None, session=mock.ANY),
                mock.call(mock.ANY, processor_subdir=None, session=mock.ANY),
            ]
        )
        # Assert that rollback is called twice (i.e. whenever OperationalError occurs)
        mock_session.rollback.assert_has_calls([mock.call(), mock.call()])
        # Check that 'SerializedDagModel.write_dag' is also called
        # Only called once since the other two times the 'DAG.bulk_write_to_db' error'd
        # and the session was roll-backed before even reaching 'SerializedDagModel.write_dag'
        mock_s10n_write_dag.assert_has_calls(
            [
                mock.call(
                    mock_dag, min_update_interval=mock.ANY, processor_subdir=None, session=mock_session
                ),
            ]
        )

        serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
        assert serialized_dags_count == 0

    def test_serialized_dags_are_written_to_db_on_sync(self, session):
        """
        Test that when dagbag.sync_to_db is called the DAGs are Serialized and written to DB
        even when dagbag.read_dags_from_db is False
        """
        serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
        assert serialized_dags_count == 0

        dag = DAG(dag_id="test")

        update_dag_parsing_results_in_db([dag], dict(), None, set(), session)

        new_serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
        assert new_serialized_dags_count == 1

    @patch.object(SerializedDagModel, "write_dag")
    def test_serialized_dag_errors_are_import_errors(self, mock_serialize, caplog, session):
        """
        Test that errors serializing a DAG are recorded as import_errors in the DB
        """
        mock_serialize.side_effect = SerializationError

        caplog.set_level(logging.ERROR)

        dag = DAG(dag_id="test")
        dag.fileloc = "abc.py"

        import_errors = {}
        update_dag_parsing_results_in_db([dag], import_errors, None, set(), session)
        assert "SerializationError" in caplog.text

        # Should have been edited in places
        err = import_errors.get(dag.fileloc)
        assert "SerializationError" in err

        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        assert dag_model.has_import_errors is True

        import_errors = session.query(ParseImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == dag.fileloc
        assert "SerializationError" in import_error.stacktrace

    def test_new_import_error_replaces_old(self, session):
        """
        Test that existing import error is updated and new record not created
        for a dag with the same filename
        """
        filename = "abc.py"
        prev_error = ParseImportError(
            filename=filename,
            timestamp=tz.utcnow(),
            stacktrace="Some error",
            processor_subdir=None,
        )
        session.add(prev_error)
        session.flush()
        prev_error_id = prev_error.id

        update_dag_parsing_results_in_db(
            dags=[],
            import_errors={"abc.py": "New error"},
            processor_subdir=None,
            warnings=set(),
            session=session,
        )

        import_error = session.query(ParseImportError).filter(ParseImportError.filename == filename).one()

        # assert that the ID of the import error did not change
        assert import_error.id == prev_error_id
        assert import_error.stacktrace == "New error"

    def test_remove_error_clears_import_error(self, session):
        # Pre-condition: there is an import error for the dag file
        filename = "abc.py"
        prev_error = ParseImportError(
            filename=filename,
            timestamp=tz.utcnow(),
            stacktrace="Some error",
            processor_subdir=None,
        )
        session.add(prev_error)

        # And one for another file we haven't been given results for -- this shouldn't be deleted
        session.add(
            ParseImportError(
                filename="def.py",
                timestamp=tz.utcnow(),
                stacktrace="Some error",
                processor_subdir=None,
            )
        )
        session.flush()

        # Sanity check of pre-condition
        import_errors = set(session.scalars(select(ParseImportError.filename)))
        assert import_errors == {"abc.py", "def.py"}

        dag = DAG(dag_id="test")
        dag.fileloc = filename

        import_errors = {}
        update_dag_parsing_results_in_db([dag], import_errors, None, set(), session)

        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        assert dag_model.has_import_errors is False

        import_errors = set(session.scalars(select(ParseImportError.filename)))

        assert import_errors == {"def.py"}

    def test_sync_perm_for_dag_with_dict_access_control(self, session, spy_agency: SpyAgency):
        """
        Test that dagbag._sync_perm_for_dag will call ApplessAirflowSecurityManager.sync_perm_for_dag
        """
        from airflow.www.security_appless import ApplessAirflowSecurityManager

        spy = spy_agency.spy_on(
            ApplessAirflowSecurityManager.sync_perm_for_dag, owner=ApplessAirflowSecurityManager
        )

        dag = DAG(dag_id="test")

        def _sync_perms():
            spy.reset_calls()
            _sync_dag_perms(dag, session=session)

        # perms dont exist
        _sync_perms()
        spy_agency.assert_spy_called_with(spy, dag.dag_id, access_control=None)

        # perms now exist
        _sync_perms()
        spy_agency.assert_spy_called_with(spy, dag.dag_id, access_control=None)

        # Always sync if we have access_control
        dag.access_control = {"Public": {"DAGs": {"can_read"}, "DAG Runs": {"can_create"}}}
        _sync_perms()
        spy_agency.assert_spy_called_with(
            spy, dag.dag_id, access_control={"Public": {"DAGs": {"can_read"}, "DAG Runs": {"can_create"}}}
        )
