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

import contextlib
import inspect
import logging
import os
import pathlib
import sys
import textwrap
import warnings
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest import mock
from unittest.mock import patch

import pytest
import time_machine

import airflow.example_dags
from airflow import settings
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone as tz
from airflow.utils.session import create_session

from tests_common.test_utils import db
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from unit import cluster_policies
from unit.models import TEST_DAGS_FOLDER

pytestmark = pytest.mark.db_test

example_dags_folder = pathlib.Path(airflow.example_dags.__path__[0])  # type: ignore[attr-defined]

PY311 = sys.version_info >= (3, 11)

# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
INVALID_DAG_WITH_DEPTH_FILE_CONTENTS = "def something():\n    return airflow_DAG\nsomething()"


def db_clean_up():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_serialized_dags()
    db.clear_dag_specific_permissions()


class TestDagBag:
    def setup_class(self):
        db_clean_up()

    def teardown_class(self):
        db_clean_up()

    def test_get_existing_dag(self, tmp_path):
        """
        Test that we're able to parse some example DAGs and retrieve them
        """
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=True)

        some_expected_dag_ids = ["example_bash_operator", "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            assert dag is not None
            assert dag_id == dag.dag_id

        assert dagbag.size() >= 7

    def test_get_non_existing_dag(self, tmp_path):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None

    def test_serialized_dag_not_existing_doesnt_raise(self, tmp_path):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False, read_dags_from_db=True)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None

    def test_dont_load_example(self, tmp_path):
        """
        test that the example are not loaded
        """
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        assert dagbag.size() == 0

    def test_safe_mode_heuristic_match(self, tmp_path):
        """
        With safe mode enabled, a file matching the discovery heuristics
        should be discovered.
        """
        path = tmp_path / "testfile.py"
        path.write_text("# airflow\n# DAG")

        with conf_vars({("core", "dags_folder"): os.fspath(path.parent)}):
            dagbag = DagBag(include_examples=False, safe_mode=True)

        assert len(dagbag.dagbag_stats) == 1
        assert dagbag.dagbag_stats[0].file == f"/{path.name}"

    def test_safe_mode_heuristic_mismatch(self, tmp_path):
        """
        With safe mode enabled, a file not matching the discovery heuristics
        should not be discovered.
        """
        path = tmp_path / "testfile.py"
        path.write_text("")
        with conf_vars({("core", "dags_folder"): os.fspath(path.parent)}):
            dagbag = DagBag(include_examples=False, safe_mode=True)
        assert len(dagbag.dagbag_stats) == 0

    def test_safe_mode_disabled(self, tmp_path):
        """With safe mode disabled, an empty python file should be discovered."""
        path = tmp_path / "testfile.py"
        path.write_text("")
        with conf_vars({("core", "dags_folder"): os.fspath(path.parent)}):
            dagbag = DagBag(include_examples=False, safe_mode=False)
        assert len(dagbag.dagbag_stats) == 1
        assert dagbag.dagbag_stats[0].file == f"/{path.name}"

    def test_process_file_that_contains_multi_bytes_char(self, tmp_path):
        """
        test that we're able to parse file that contains multi-byte char
        """
        path = tmp_path / "testfile"
        path.write_text("\u3042")  # write multi-byte char (hiragana)

        dagbag = DagBag(dag_folder=os.fspath(path.parent), include_examples=False)
        assert dagbag.process_file(os.fspath(path)) == []

    def test_process_file_duplicated_dag_id(self, tmp_path):
        """Loading a DAG with ID that already existed in a DAG bag should result in an import error."""
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        def create_dag():
            from airflow.sdk import dag

            @dag(schedule=None, default_args={"owner": "owner1"})
            def my_flow():
                pass

            my_dag = my_flow()  # noqa: F841

        source_lines = [line[12:] for line in inspect.getsource(create_dag).splitlines(keepends=True)[1:]]
        path1 = tmp_path / "testfile1"
        path2 = tmp_path / "testfile2"
        path1.write_text("".join(source_lines))
        path2.write_text("".join(source_lines))

        found_1 = dagbag.process_file(os.fspath(path1))
        assert len(found_1) == 1
        assert found_1[0].dag_id == "my_flow"
        assert dagbag.import_errors == {}
        dags_in_bag = dagbag.dags

        found_2 = dagbag.process_file(os.fspath(path2))
        assert len(found_2) == 0
        assert dagbag.import_errors[os.fspath(path2)].startswith(
            "AirflowDagDuplicatedIdException: Ignoring DAG"
        )
        assert dagbag.dags == dags_in_bag  # Should not change.

    def test_zip_skip_log(self, caplog, test_zip_path):
        """
        test the loading of a DAG from within a zip file that skips another file because
        it doesn't have "airflow" and "DAG"
        """
        caplog.set_level(logging.INFO)
        dagbag = DagBag(dag_folder=test_zip_path, include_examples=False)

        assert dagbag.has_logged
        assert (
            f"File {test_zip_path}:file_no_airflow_dag.py "
            "assumed to contain no DAGs. Skipping." in caplog.text
        )

    def test_zip(self, tmp_path, test_zip_path):
        """
        test the loading of a DAG within a zip file that includes dependencies
        """
        syspath_before = deepcopy(sys.path)
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)
        dagbag.process_file(test_zip_path)
        assert dagbag.get_dag("test_zip_dag")
        assert sys.path == syspath_before  # sys.path doesn't change
        assert not dagbag.import_errors

    @patch("airflow.models.dagbag.timeout")
    @patch("airflow.models.dagbag.settings.get_dagbag_import_timeout")
    def test_process_dag_file_without_timeout(
        self, mocked_get_dagbag_import_timeout, mocked_timeout, tmp_path
    ):
        """
        Test dag file parsing without timeout
        """
        mocked_get_dagbag_import_timeout.return_value = 0

        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_sensor.py"))
        mocked_timeout.assert_not_called()

        mocked_get_dagbag_import_timeout.return_value = -1
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_sensor.py"))
        mocked_timeout.assert_not_called()

    @patch("airflow.models.dagbag.timeout")
    @patch("airflow.models.dagbag.settings.get_dagbag_import_timeout")
    def test_process_dag_file_with_non_default_timeout(
        self, mocked_get_dagbag_import_timeout, mocked_timeout, tmp_path
    ):
        """
        Test customized dag file parsing timeout
        """
        timeout_value = 100
        mocked_get_dagbag_import_timeout.return_value = timeout_value

        # ensure the test value is not equal to the default value
        assert timeout_value != settings.conf.getfloat("core", "DAGBAG_IMPORT_TIMEOUT")

        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_sensor.py"))

        mocked_timeout.assert_called_once_with(timeout_value, error_message=mock.ANY)

    @patch("airflow.models.dagbag.settings.get_dagbag_import_timeout")
    def test_check_value_type_from_get_dagbag_import_timeout(
        self, mocked_get_dagbag_import_timeout, tmp_path
    ):
        """
        Test correctness of value from get_dagbag_import_timeout
        """
        mocked_get_dagbag_import_timeout.return_value = "1"

        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)
        with pytest.raises(
            TypeError, match=r"Value \(1\) from get_dagbag_import_timeout must be int or float"
        ):
            dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_sensor.py"))

    @pytest.fixture
    def invalid_cron_dag(self) -> str:
        return os.path.join(TEST_DAGS_FOLDER, "test_invalid_cron.py")

    @pytest.fixture
    def invalid_cron_zipped_dag(self, invalid_cron_dag: str, tmp_path: pathlib.Path) -> str:
        zipped = tmp_path / "test_zip_invalid_cron.zip"
        with zipfile.ZipFile(zipped, "w") as zf:
            zf.write(invalid_cron_dag, os.path.basename(invalid_cron_dag))
        return os.fspath(zipped)

    @pytest.mark.parametrize("invalid_dag_name", ["invalid_cron_dag", "invalid_cron_zipped_dag"])
    def test_process_file_cron_validity_check(
        self, request: pytest.FixtureRequest, invalid_dag_name: str, tmp_path
    ):
        """Test if an invalid cron expression as schedule interval can be identified"""
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)
        assert len(dagbag.import_errors) == 0
        dagbag.process_file(request.getfixturevalue(invalid_dag_name))
        assert len(dagbag.import_errors) == 1
        assert len(dagbag.dags) == 0

    def test_process_file_invalid_param_check(self, tmp_path):
        """
        test if an invalid param in the dags can be identified
        """
        invalid_dag_files = [
            "test_invalid_param.py",
            "test_invalid_param2.py",
            "test_invalid_param3.py",
            "test_invalid_param4.py",
        ]
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        assert len(dagbag.import_errors) == 0
        for file in invalid_dag_files:
            dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, file))
        assert len(dagbag.import_errors) == len(invalid_dag_files)
        assert len(dagbag.dags) == 0

    def test_process_file_valid_param_check(self, tmp_path):
        """
        test if valid params in the dags param can be validated (positive test)
        """
        valid_dag_files = [
            "test_valid_param.py",
            "test_valid_param2.py",
        ]
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        assert len(dagbag.import_errors) == 0
        for file in valid_dag_files:
            dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, file))
        assert len(dagbag.import_errors) == 0
        assert len(dagbag.dags) == len(valid_dag_files)

    @patch.object(DagModel, "get_current")
    def test_get_dag_without_refresh(self, mock_dagmodel):
        """
        Test that, once a DAG is loaded, it doesn't get refreshed again if it
        hasn't been expired.
        """
        dag_id = "example_bash_operator"

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = None
        mock_dagmodel.return_value.fileloc = "foo"

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if os.path.basename(filepath) == "example_bash_operator.py":
                    _TestDagBag.process_file_calls += 1
                super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(include_examples=True)
        dagbag.process_file_calls

        # Should not call process_file again, since it's already loaded during init.
        assert dagbag.process_file_calls == 1
        assert dagbag.get_dag(dag_id) is not None
        assert dagbag.process_file_calls == 1

    @pytest.mark.parametrize(
        ("file_to_load", "expected"),
        (
            pytest.param(
                pathlib.Path(example_dags_folder) / "example_bash_operator.py",
                {"example_bash_operator": "airflow/example_dags/example_bash_operator.py"},
                id="example_bash_operator",
            ),
        ),
    )
    def test_get_dag_registration(self, file_to_load, expected):
        dagbag = DagBag(dag_folder=os.devnull, include_examples=False)
        dagbag.process_file(os.fspath(file_to_load))
        for dag_id, path in expected.items():
            dag = dagbag.get_dag(dag_id)
            assert dag, f"{dag_id} was bagged"
            assert dag.fileloc.endswith(path)

    @pytest.mark.parametrize(
        ("expected"),
        (
            pytest.param(
                {
                    "test_zip_dag": "test_zip.zip/test_zip.py",
                    "test_zip_autoregister": "test_zip.zip/test_zip.py",
                },
                id="test_zip.zip",
            ),
        ),
    )
    def test_get_zip_dag_registration(self, test_zip_path, expected):
        dagbag = DagBag(dag_folder=os.devnull, include_examples=False)
        dagbag.process_file(test_zip_path)
        for dag_id, path in expected.items():
            dag = dagbag.get_dag(dag_id)
            assert dag, f"{dag_id} was bagged"
            assert dag.fileloc.endswith(f"{pathlib.Path(test_zip_path).parent}/{path}")

    def test_dag_registration_with_failure(self):
        dagbag = DagBag(dag_folder=os.devnull, include_examples=False)
        found = dagbag.process_file(str(TEST_DAGS_FOLDER / "test_invalid_dup_task.py"))
        assert found == []

    @pytest.fixture
    def zip_with_valid_dag_and_dup_tasks(self, tmp_path: pathlib.Path) -> str:
        failing_dag_file = TEST_DAGS_FOLDER / "test_invalid_dup_task.py"
        working_dag_file = TEST_DAGS_FOLDER / "test_example_bash_operator.py"
        zipped = tmp_path / "test_zip_invalid_dup_task.zip"
        with zipfile.ZipFile(zipped, "w") as zf:
            zf.write(failing_dag_file, failing_dag_file.name)
            zf.write(working_dag_file, working_dag_file.name)
        return os.fspath(zipped)

    def test_dag_registration_with_failure_zipped(self, zip_with_valid_dag_and_dup_tasks):
        dagbag = DagBag(dag_folder=os.devnull, include_examples=False)
        found = dagbag.process_file(zip_with_valid_dag_and_dup_tasks)
        assert len(found) == 1
        assert [dag.dag_id for dag in found] == ["test_example_bash_operator"]

    @patch.object(DagModel, "get_current")
    def test_refresh_py_dag(self, mock_dagmodel, tmp_path):
        """
        Test that we can refresh an ordinary .py DAG
        """
        dag_id = "example_bash_operator"
        fileloc = str(example_dags_folder / "example_bash_operator.py")

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = datetime.max.replace(tzinfo=timezone.utc)
        mock_dagmodel.return_value.fileloc = fileloc

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if filepath == fileloc:
                    _TestDagBag.process_file_calls += 1
                return super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(dag_folder=os.fspath(tmp_path), include_examples=True)

        assert dagbag.process_file_calls == 1
        dag = dagbag.get_dag(dag_id)
        assert dag is not None
        assert dag_id == dag.dag_id
        assert dagbag.process_file_calls == 2

    @patch.object(DagModel, "get_current")
    def test_refresh_packaged_dag(self, mock_dagmodel, test_zip_path):
        """
        Test that we can refresh a packaged DAG
        """
        dag_id = "test_zip_dag"
        fileloc = os.path.realpath(os.path.join(test_zip_path, "test_zip.py"))

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = datetime.max.replace(tzinfo=timezone.utc)
        mock_dagmodel.return_value.fileloc = fileloc

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if filepath in fileloc:
                    _TestDagBag.process_file_calls += 1
                return super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(dag_folder=os.path.realpath(test_zip_path), include_examples=False)

        assert dagbag.process_file_calls == 1
        dag = dagbag.get_dag(dag_id)
        assert dag is not None
        assert dag_id == dag.dag_id
        assert dagbag.process_file_calls == 2

    def test_dag_removed_if_serialized_dag_is_removed(self, dag_maker, tmp_path):
        """
        Test that if a DAG does not exist in serialized_dag table (as the DAG file was removed),
        remove dags from the DagBag
        """
        from airflow.providers.standard.operators.empty import EmptyOperator

        with dag_maker(
            dag_id="test_dag_removed_if_serialized_dag_is_removed",
            schedule=None,
            start_date=tz.datetime(2021, 10, 12),
        ) as dag:
            EmptyOperator(task_id="task_1")
        dag_maker.create_dagrun()
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False, read_dags_from_db=True)
        dagbag.dags = {dag.dag_id: SerializedDAG.from_dict(SerializedDAG.to_dict(dag))}
        dagbag.dags_last_fetched = {dag.dag_id: (tz.utcnow() - timedelta(minutes=2))}
        dagbag.dags_hash = {dag.dag_id: mock.ANY}

        assert SerializedDagModel.has_dag(dag.dag_id) is False

        assert dagbag.get_dag(dag.dag_id) is None
        assert dag.dag_id not in dagbag.dags
        assert dag.dag_id not in dagbag.dags_last_fetched
        assert dag.dag_id not in dagbag.dags_hash

    def process_dag(self, create_dag, tmp_path):
        """
        Helper method to process a file generated from the input create_dag function.
        """
        # write source to file
        source = textwrap.dedent("".join(inspect.getsource(create_dag).splitlines(True)[1:-1]))
        path = tmp_path / "testfile"
        path.write_text(source)

        dagbag = DagBag(dag_folder=os.fspath(path.parent), include_examples=False)
        found_dags = dagbag.process_file(os.fspath(path))
        return dagbag, found_dags, os.fspath(path)

    def validate_dags(self, expected_dag, actual_found_dags, actual_dagbag, should_be_found=True):
        actual_found_dag_ids = [dag.dag_id for dag in actual_found_dags]
        dag_id = expected_dag.dag_id
        actual_dagbag.log.info("validating %s", dag_id)
        assert (dag_id in actual_found_dag_ids) == should_be_found, (
            f'dag "{dag_id}" should {"" if should_be_found else "not "}'
            f'have been found after processing dag "{expected_dag.dag_id}"'
        )
        assert (dag_id in actual_dagbag.dags) == should_be_found, (
            f'dag "{dag_id}" should {"" if should_be_found else "not "}'
            f'be in dagbag.dags after processing dag "{expected_dag.dag_id}"'
        )

    def test_skip_cycle_dags(self, tmp_path):
        """
        Don't crash when loading an invalid (contains a cycle) DAG file.
        Don't load the dag into the DagBag either
        """

        # Define Dag to load
        def basic_cycle():
            import datetime

            from airflow.models.dag import DAG
            from airflow.providers.standard.operators.empty import EmptyOperator

            dag_name = "cycle_dag"
            default_args = {"owner": "owner1", "start_date": datetime.datetime(2016, 1, 1)}
            dag = DAG(dag_name, schedule=timedelta(days=1), default_args=default_args)

            # A -> A
            with dag:
                op_a = EmptyOperator(task_id="A")
                op_a.set_downstream(op_a)

            return dag

        test_dag = basic_cycle()

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(basic_cycle, tmp_path)

        # #Validate correctness
        # None of the dags should be found
        self.validate_dags(test_dag, found_dags, dagbag, should_be_found=False)
        assert file_path in dagbag.import_errors

    def test_process_file_with_none(self, tmp_path):
        """
        test that process_file can handle Nones
        """
        dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        assert dagbag.process_file(None) == []

    def test_deactivate_unknown_dags(self):
        """
        Test that dag_ids not passed into deactivate_unknown_dags
        are deactivated when function is invoked
        """
        dagbag = DagBag(include_examples=True)
        dag_id = "test_deactivate_unknown_dags"
        expected_active_dags = dagbag.dags.keys()

        model_before = DagModel(dag_id=dag_id, is_stale=False)
        with create_session() as session:
            session.merge(model_before)

        DAG.deactivate_unknown_dags(expected_active_dags)

        after_model = DagModel.get_dagmodel(dag_id)
        assert not model_before.is_stale
        assert after_model.is_stale

        # clean up
        with create_session() as session:
            session.query(DagModel).filter(DagModel.dag_id == "test_deactivate_unknown_dags").delete()

    def test_timeout_dag_errors_are_import_errors(self, tmp_path, caplog):
        """
        Test that if the DAG contains Timeout error it will be still loaded to DB as import_errors
        """
        code_to_save = """
# Define Dag to load
import datetime
import time

import airflow
from airflow.providers.standard.operators.python import PythonOperator

time.sleep(1)

with airflow.DAG(
    "import_timeout",
    start_date=datetime.datetime(2022, 1, 1),
    schedule=None) as dag:
    def f():
        print("Sleeping")
        time.sleep(1)


    for ind in range(10):
        PythonOperator(
            dag=dag,
            task_id=f"sleep_2_{ind}",
            python_callable=f,
        )
        """
        with open("tmp_file.py", "w") as f:
            f.write(code_to_save)

        with conf_vars({("core", "DAGBAG_IMPORT_TIMEOUT"): "0.01"}):
            dagbag = DagBag(dag_folder=os.fspath("tmp_file.py"), include_examples=False)
            dag = dagbag._load_modules_from_file("tmp_file.py", safe_mode=False)

        assert dag is not None
        assert "tmp_file.py" in dagbag.import_errors
        assert "DagBag import timeout for" in caplog.text

    @staticmethod
    def _make_test_traceback(unparseable_filename: str, depth=None) -> str:
        marker = "           ^^^^^^^^^^^\n" if PY311 else ""
        frames = (
            f'  File "{unparseable_filename}", line 3, in <module>\n    something()\n',
            f'  File "{unparseable_filename}", line 2, in something\n    return airflow_DAG\n{marker}',
        )
        depth = 0 if depth is None else -depth
        return (
            "Traceback (most recent call last):\n"
            + "".join(frames[depth:])
            + "NameError: name 'airflow_DAG' is not defined\n"
        )

    @pytest.mark.parametrize(("depth",), ((None,), (1,)))
    def test_import_error_tracebacks(self, tmp_path, depth):
        unparseable_filename = tmp_path.joinpath("dag.py").as_posix()
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with contextlib.ExitStack() as cm:
            if depth is not None:
                cm.enter_context(conf_vars({("core", "dagbag_import_error_traceback_depth"): str(depth)}))
            dagbag = DagBag(dag_folder=unparseable_filename, include_examples=False)
        import_errors = dagbag.import_errors

        assert unparseable_filename in import_errors
        assert import_errors[unparseable_filename] == self._make_test_traceback(unparseable_filename, depth)

    @pytest.mark.parametrize(("depth",), ((None,), (1,)))
    def test_import_error_tracebacks_zip(self, tmp_path, depth):
        invalid_zip_filename = (tmp_path / "test_zip_invalid.zip").as_posix()
        invalid_dag_filename = os.path.join(invalid_zip_filename, "dag.py")
        with zipfile.ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
            invalid_zip_file.writestr("dag.py", INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with contextlib.ExitStack() as cm:
            if depth is not None:
                cm.enter_context(conf_vars({("core", "dagbag_import_error_traceback_depth"): str(depth)}))
            dagbag = DagBag(dag_folder=invalid_zip_filename, include_examples=False)
        import_errors = dagbag.import_errors
        assert invalid_dag_filename in import_errors
        assert import_errors[invalid_dag_filename] == self._make_test_traceback(invalid_dag_filename, depth)

    @patch("airflow.models.dagbag.settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL", 5)
    @patch("airflow.models.dagbag.settings.MIN_SERIALIZED_DAG_FETCH_INTERVAL", 5)
    def test_get_dag_with_dag_serialization(self):
        """
        Test that Serialized DAG is updated in DagBag when it is updated in
        Serialized DAG table after 'min_serialized_dag_fetch_interval' seconds are passed.
        """
        with time_machine.travel((tz.datetime(2020, 1, 5, 0, 0, 0)), tick=False):
            example_bash_op_dag = DagBag(include_examples=True).dags.get("example_bash_operator")
            DAG.from_sdk_dag(example_bash_op_dag).sync_to_db()
            SerializedDagModel.write_dag(dag=example_bash_op_dag, bundle_name="testing")

            dag_bag = DagBag(read_dags_from_db=True)
            ser_dag_1 = dag_bag.get_dag("example_bash_operator")
            ser_dag_1_update_time = dag_bag.dags_last_fetched["example_bash_operator"]
            assert example_bash_op_dag.tags == ser_dag_1.tags
            assert ser_dag_1_update_time == tz.datetime(2020, 1, 5, 0, 0, 0)

        # Check that if min_serialized_dag_fetch_interval has not passed we do not fetch the DAG
        # from DB
        with time_machine.travel((tz.datetime(2020, 1, 5, 0, 0, 4)), tick=False):
            with assert_queries_count(0):
                assert dag_bag.get_dag("example_bash_operator").tags == {"example", "example2"}

        # Make a change in the DAG and write Serialized DAG to the DB
        with time_machine.travel((tz.datetime(2020, 1, 5, 0, 0, 6)), tick=False):
            example_bash_op_dag.tags.add("new_tag")
            DAG.from_sdk_dag(example_bash_op_dag).sync_to_db()
            SerializedDagModel.write_dag(dag=example_bash_op_dag, bundle_name="testing")

        # Since min_serialized_dag_fetch_interval is passed verify that calling 'dag_bag.get_dag'
        # fetches the Serialized DAG from DB
        with time_machine.travel((tz.datetime(2020, 1, 5, 0, 0, 8)), tick=False):
            with assert_queries_count(2):
                updated_ser_dag_1 = dag_bag.get_dag("example_bash_operator")
                updated_ser_dag_1_update_time = dag_bag.dags_last_fetched["example_bash_operator"]

        assert set(updated_ser_dag_1.tags) == {"example", "example2", "new_tag"}
        assert updated_ser_dag_1_update_time > ser_dag_1_update_time

    @patch("airflow.models.dagbag.settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL", 5)
    @patch("airflow.models.dagbag.settings.MIN_SERIALIZED_DAG_FETCH_INTERVAL", 5)
    def test_get_dag_refresh_race_condition(self, session, testing_dag_bundle):
        """
        Test that DagBag.get_dag correctly refresh the Serialized DAG even if SerializedDagModel.last_updated
        is before DagBag.dags_last_fetched.
        """
        db_clean_up()
        # serialize the initial version of the DAG
        with time_machine.travel((tz.datetime(2020, 1, 5, 0, 0, 0)), tick=False):
            example_bash_op_dag = DagBag(include_examples=True).dags.get("example_bash_operator")
            DAG.from_sdk_dag(example_bash_op_dag).sync_to_db()
            SerializedDagModel.write_dag(dag=example_bash_op_dag, bundle_name="testing")

        # deserialize the DAG
        with time_machine.travel((tz.datetime(2020, 1, 5, 1, 0, 10)), tick=False):
            dag_bag = DagBag(read_dags_from_db=True)

            with assert_queries_count(2):
                ser_dag = dag_bag.get_dag("example_bash_operator")

            ser_dag_update_time = dag_bag.dags_last_fetched["example_bash_operator"]
            assert ser_dag.tags == {"example", "example2"}
            assert ser_dag_update_time == tz.datetime(2020, 1, 5, 1, 0, 10)

            with create_session() as session:
                assert SerializedDagModel.get_last_updated_datetime(
                    dag_id="example_bash_operator",
                    session=session,
                ) == tz.datetime(2020, 1, 5, 0, 0, 0)

        # Simulate a long-running serialization transaction
        # Make a change in the DAG and write Serialized DAG to the DB
        # Note the date *before* the deserialize step above, simulating a serialization happening
        # long before the transaction is committed
        with time_machine.travel((tz.datetime(2020, 1, 5, 1, 0, 0)), tick=False):
            example_bash_op_dag.tags.add("new_tag")
            DAG.from_sdk_dag(example_bash_op_dag).sync_to_db()
            SerializedDagModel.write_dag(dag=example_bash_op_dag, bundle_name="testing")

        # Since min_serialized_dag_fetch_interval is passed verify that calling 'dag_bag.get_dag'
        # fetches the Serialized DAG from DB
        with time_machine.travel((tz.datetime(2020, 1, 5, 1, 0, 30)), tick=False):
            with assert_queries_count(2):
                updated_ser_dag = dag_bag.get_dag("example_bash_operator")
                updated_ser_dag_update_time = dag_bag.dags_last_fetched["example_bash_operator"]

        assert set(updated_ser_dag.tags) == {"example", "example2", "new_tag"}
        assert updated_ser_dag_update_time > ser_dag_update_time

    def test_collect_dags_from_db(self, testing_dag_bundle):
        """DAGs are collected from Database"""
        db.clear_db_dags()
        dagbag = DagBag(str(example_dags_folder))

        example_dags = dagbag.dags
        for dag in example_dags.values():
            DAG.from_sdk_dag(dag).sync_to_db()
            SerializedDagModel.write_dag(dag, bundle_name="dag_maker")

        new_dagbag = DagBag(read_dags_from_db=True)
        assert len(new_dagbag.dags) == 0
        new_dagbag.collect_dags_from_db()
        new_dags = new_dagbag.dags
        assert len(example_dags) == len(new_dags)
        for dag_id, dag in example_dags.items():
            serialized_dag = new_dags[dag_id]

            assert serialized_dag.dag_id == dag.dag_id
            assert set(serialized_dag.task_dict) == set(dag.task_dict)

    @patch("airflow.settings.task_policy", cluster_policies.example_task_policy)
    def test_task_cluster_policy_violation(self):
        """
        test that file processing results in import error when task does not
        obey cluster policy.
        """
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_missing_owner.py")
        dag_id = "test_missing_owner"
        err_cls_name = "AirflowClusterPolicyViolation"

        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        assert set() == set(dagbag.dag_ids)
        expected_import_errors = {
            dag_file: (
                f"""{err_cls_name}: DAG policy violation (DAG ID: {dag_id}, Path: {dag_file}):\n"""
                """Notices:\n"""
                """ * Task must have non-None non-default owner. Current value: airflow"""
            )
        }
        assert expected_import_errors == dagbag.import_errors

    @patch("airflow.settings.task_policy", cluster_policies.example_task_policy)
    def test_task_cluster_policy_nonstring_owner(self):
        """
        test that file processing results in import error when task does not
        obey cluster policy and has owner whose type is not string.
        """
        TEST_DAGS_CORRUPTED_FOLDER = pathlib.Path(__file__).parent.with_name("dags_corrupted")
        dag_file = os.path.join(TEST_DAGS_CORRUPTED_FOLDER, "test_nonstring_owner.py")
        dag_id = "test_nonstring_owner"
        err_cls_name = "AirflowClusterPolicyViolation"

        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        assert set() == set(dagbag.dag_ids)
        expected_import_errors = {
            dag_file: (
                f"""{err_cls_name}: DAG policy violation (DAG ID: {dag_id}, Path: {dag_file}):\n"""
                """Notices:\n"""
                """ * owner should be a string. Current value: ['a']"""
            )
        }
        assert expected_import_errors == dagbag.import_errors

    @patch("airflow.settings.task_policy", cluster_policies.example_task_policy)
    def test_task_cluster_policy_obeyed(self):
        """
        test that dag successfully imported without import errors when tasks
        obey cluster policy.
        """
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_with_non_default_owner.py")

        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        assert {"test_with_non_default_owner"} == set(dagbag.dag_ids)

        assert dagbag.import_errors == {}

    @patch("airflow.settings.dag_policy", cluster_policies.dag_policy)
    def test_dag_cluster_policy_obeyed(self):
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_dag_with_no_tags.py")

        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        assert len(dagbag.dag_ids) == 0
        assert "has no tags" in dagbag.import_errors[dag_file]

    def test_dagbag_dag_collection(self):
        dagbag = DagBag(dag_folder=TEST_DAGS_FOLDER, include_examples=False, collect_dags=False)
        # since collect_dags is False, dagbag.dags should be empty
        assert not dagbag.dags

        dagbag.collect_dags()
        assert dagbag.dags

        # test that dagbag.dags is not empty if collect_dags is True
        dagbag = DagBag(dag_folder=TEST_DAGS_FOLDER, include_examples=False)
        assert dagbag.dags

    def test_dabgag_captured_warnings(self):
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_dag_warnings.py")
        dagbag = DagBag(dag_folder=dag_file, include_examples=False, collect_dags=False)
        assert dag_file not in dagbag.captured_warnings

        dagbag.collect_dags(dag_folder=dagbag.dag_folder, include_examples=False, only_if_updated=False)
        assert len(dagbag.dag_ids) == 1
        assert dag_file in dagbag.captured_warnings
        captured_warnings = dagbag.captured_warnings[dag_file]
        assert len(captured_warnings) == 2
        assert dagbag.dagbag_stats[0].warning_num == 2

        assert captured_warnings[0] == (f"{dag_file}:47: DeprecationWarning: Deprecated Parameter")
        assert captured_warnings[1] == f"{dag_file}:49: UserWarning: Some Warning"

        with warnings.catch_warnings():
            # Disable capture DeprecationWarning, and it should be reflected in captured warnings
            warnings.simplefilter("ignore", DeprecationWarning)
            dagbag.collect_dags(dag_folder=dagbag.dag_folder, include_examples=False, only_if_updated=False)
            assert dag_file in dagbag.captured_warnings
            assert len(dagbag.captured_warnings[dag_file]) == 1
            assert dagbag.dagbag_stats[0].warning_num == 1

            # Disable all warnings, no captured warnings expected
            warnings.simplefilter("ignore")
            dagbag.collect_dags(dag_folder=dagbag.dag_folder, include_examples=False, only_if_updated=False)
            assert dag_file not in dagbag.captured_warnings
            assert dagbag.dagbag_stats[0].warning_num == 0

    @pytest.fixture
    def warning_zipped_dag_path(self, tmp_path: pathlib.Path) -> str:
        warnings_dag_file = TEST_DAGS_FOLDER / "test_dag_warnings.py"
        zipped = tmp_path / "test_dag_warnings.zip"
        with zipfile.ZipFile(zipped, "w") as zf:
            zf.write(warnings_dag_file, warnings_dag_file.name)
        return os.fspath(zipped)

    def test_dabgag_captured_warnings_zip(self, warning_zipped_dag_path: str):
        in_zip_dag_file = f"{warning_zipped_dag_path}/test_dag_warnings.py"
        dagbag = DagBag(dag_folder=warning_zipped_dag_path, include_examples=False)
        assert len(dagbag.dag_ids) == 1
        assert warning_zipped_dag_path in dagbag.captured_warnings
        captured_warnings = dagbag.captured_warnings[warning_zipped_dag_path]
        assert len(captured_warnings) == 2
        assert captured_warnings[0] == (f"{in_zip_dag_file}:47: DeprecationWarning: Deprecated Parameter")
        assert captured_warnings[1] == f"{in_zip_dag_file}:49: UserWarning: Some Warning"

    @pytest.mark.parametrize(
        ("known_pools", "expected"),
        (
            pytest.param(None, set(), id="disabled"),
            pytest.param(
                {"default_pool"},
                {
                    DagWarning(
                        "test",
                        DagWarningType.NONEXISTENT_POOL,
                        "Dag 'test' references non-existent pools: ['pool1']",
                    ),
                },
                id="only-default",
            ),
            pytest.param(
                {"default_pool", "pool1"},
                set(),
                id="known-pools",
            ),
        ),
    )
    def test_dag_warnings_invalid_pool(self, known_pools, expected):
        from airflow.models.baseoperator import BaseOperator

        with DAG(dag_id="test") as dag:
            BaseOperator(task_id="1")
            BaseOperator(task_id="2", pool="pool1")

        dagbag = DagBag(dag_folder="", include_examples=False, collect_dags=False, known_pools=known_pools)
        dagbag.bag_dag(dag)
        assert dagbag.dag_warnings == expected
