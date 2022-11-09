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
import os
from unittest import mock
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest

from airflow import settings
from airflow.callbacks.callback_requests import TaskCallbackRequest
from airflow.configuration import TEST_DAGS_FOLDER, conf
from airflow.dag_processing.manager import DagFileProcessorAgent
from airflow.dag_processing.processor import DagFileProcessor, DagFileProcessorProcess
from airflow.models import DagBag, DagModel, SlaMiss, TaskInstance, errors
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars, env_vars
from tests.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_sla_miss,
)
from tests.test_utils.mock_executor import MockExecutor

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'
UNPARSEABLE_DAG_FILE_CONTENTS = "airflow DAG"
INVALID_DAG_WITH_DEPTH_FILE_CONTENTS = "def something():\n    return airflow_DAG\nsomething()"

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({("core", "load_examples"): "false"}):
        with env_vars({"AIRFLOW__CORE__LOAD_EXAMPLES": "false"}):
            yield


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor:
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    def setup_class(self):
        self.clean_db()

    def setup_method(self):
        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()
        self.scheduler_job = None

    def teardown_method(self) -> None:
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
            self.scheduler_job = None
        self.clean_db()

    def _process_file(self, file_path, dag_directory, session):
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=str(dag_directory), log=mock.MagicMock()
        )

        dag_file_processor.process_file(file_path, [], False, session)

    def test_dag_file_processor_sla_miss_callback(self, create_dummy_dag):
        """
        Test that the dag file processor calls the sla miss callback
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        test_start_date = timezone.utcnow() - datetime.timedelta(days=1)
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="dummy",
            sla_miss_callback=sla_callback,
            default_args={"start_date": test_start_date, "sla": datetime.timedelta()},
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="success"))

        session.merge(SlaMiss(task_id="dummy", dag_id="test_sla_miss", execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        dag_file_processor.manage_slas(dag=dag, session=session)

        assert sla_callback.called

    def test_dag_file_processor_sla_miss_callback_invalid_sla(self, create_dummy_dag):
        """
        Test that the dag file processor does not call the sla miss callback when
        given an invalid sla
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        # Pass anything besides a timedelta object to the sla argument.
        test_start_date = timezone.utcnow() - datetime.timedelta(days=1)
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="dummy",
            sla_miss_callback=sla_callback,
            default_args={"start_date": test_start_date, "sla": None},
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="success"))

        session.merge(SlaMiss(task_id="dummy", dag_id="test_sla_miss", execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        dag_file_processor.manage_slas(dag=dag, session=session)
        sla_callback.assert_not_called()

    def test_dag_file_processor_sla_miss_callback_sent_notification(self, create_dummy_dag):
        """
        Test that the dag file processor does not call the sla_miss_callback when a
        notification has already been sent
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        sla_callback = MagicMock()

        # Create dag with a start of 2 days ago, but an sla of 1 day
        # ago so we'll already have an sla_miss on the books
        test_start_date = timezone.utcnow() - datetime.timedelta(days=2)
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="dummy",
            sla_miss_callback=sla_callback,
            default_args={"start_date": test_start_date, "sla": datetime.timedelta(days=1)},
        )

        # Create a TaskInstance for two days ago
        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="success"))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(
                task_id="dummy",
                dag_id="test_sla_miss",
                execution_date=test_start_date,
                email_sent=False,
                notification_sent=True,
            )
        )

        # Now call manage_slas and see if the sla_miss callback gets called
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        dag_file_processor.manage_slas(dag=dag, session=session)

        sla_callback.assert_not_called()

    @mock.patch("airflow.dag_processing.processor.Stats.incr")
    def test_dag_file_processor_sla_miss_doesnot_raise_integrity_error(self, mock_stats_incr, dag_maker):
        """
        Test that the dag file processor does not try to insert already existing item into the database
        """
        session = settings.Session()

        # Create dag with a start of 2 days ago, but an sla of 1 day
        # ago so we'll already have an sla_miss on the books
        test_start_date = timezone.utcnow() - datetime.timedelta(days=2)
        with dag_maker(
            dag_id="test_sla_miss",
            default_args={"start_date": test_start_date, "sla": datetime.timedelta(days=1)},
        ) as dag:
            task = EmptyOperator(task_id="dummy")

        dag_maker.create_dagrun(execution_date=test_start_date, state=State.SUCCESS)

        # Create a TaskInstance for two days ago
        ti = TaskInstance(task=task, execution_date=test_start_date, state="success")
        session.merge(ti)
        session.flush()

        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        dag_file_processor.manage_slas(dag=dag, session=session)
        sla_miss_count = (
            session.query(SlaMiss)
            .filter(
                SlaMiss.dag_id == dag.dag_id,
                SlaMiss.task_id == task.task_id,
            )
            .count()
        )
        assert sla_miss_count == 1
        mock_stats_incr.assert_called_with("sla_missed")
        # Now call manage_slas and see that it runs without errors
        # because of existing SlaMiss above.
        # Since this is run often, it's possible that it runs before another
        # ti is successful thereby trying to insert a duplicate record.
        dag_file_processor.manage_slas(dag=dag, session=session)

    @mock.patch("airflow.dag_processing.processor.Stats.incr")
    def test_dag_file_processor_sla_miss_callback_exception(self, mock_stats_incr, create_dummy_dag):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
        calling the sla_miss_callback
        """
        session = settings.Session()

        sla_callback = MagicMock(side_effect=RuntimeError("Could not call function"))

        test_start_date = timezone.utcnow() - datetime.timedelta(days=1)
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="dummy",
            sla_miss_callback=sla_callback,
            default_args={"start_date": test_start_date, "sla": datetime.timedelta(hours=1)},
        )
        mock_stats_incr.reset_mock()

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="Success"))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id="dummy", dag_id="test_sla_miss", execution_date=test_start_date))

        # Now call manage_slas and see if the sla_miss callback gets called
        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)
        assert sla_callback.called
        mock_log.exception.assert_called_once_with(
            "Could not call sla_miss_callback for DAG %s", "test_sla_miss"
        )
        mock_stats_incr.assert_called_once_with("sla_callback_notification_failure")

    @mock.patch("airflow.dag_processing.processor.send_email")
    def test_dag_file_processor_only_collect_emails_from_sla_missed_tasks(
        self, mock_send_email, create_dummy_dag
    ):
        session = settings.Session()

        test_start_date = timezone.utcnow() - datetime.timedelta(days=1)
        email1 = "test1@test.com"
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="sla_missed",
            email=email1,
            default_args={"start_date": test_start_date, "sla": datetime.timedelta(hours=1)},
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="Success"))

        email2 = "test2@test.com"
        EmptyOperator(task_id="sla_not_missed", dag=dag, owner="airflow", email=email2)

        session.merge(SlaMiss(task_id="sla_missed", dag_id="test_sla_miss", execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )

        dag_file_processor.manage_slas(dag=dag, session=session)

        assert len(mock_send_email.call_args_list) == 1

        send_email_to = mock_send_email.call_args_list[0][0][0]
        assert email1 in send_email_to
        assert email2 not in send_email_to

    @mock.patch("airflow.dag_processing.processor.Stats.incr")
    @mock.patch("airflow.utils.email.send_email")
    def test_dag_file_processor_sla_miss_email_exception(
        self, mock_send_email, mock_stats_incr, create_dummy_dag
    ):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
        sending an email
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        mock_send_email.side_effect = RuntimeError("Could not send an email")

        test_start_date = timezone.utcnow() - datetime.timedelta(days=1)
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="dummy",
            email="test@test.com",
            default_args={"start_date": test_start_date, "sla": datetime.timedelta(hours=1)},
        )
        mock_stats_incr.reset_mock()

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="Success"))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id="dummy", dag_id="test_sla_miss", execution_date=test_start_date))

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock_log)

        dag_file_processor.manage_slas(dag=dag, session=session)
        mock_log.exception.assert_called_once_with(
            "Could not send SLA Miss email notification for DAG %s", "test_sla_miss"
        )
        mock_stats_incr.assert_called_once_with("sla_email_notification_failure")

    def test_dag_file_processor_sla_miss_deleted_task(self, create_dummy_dag):
        """
        Test that the dag file processor will not crash when trying to send
        sla miss notification for a deleted task
        """
        session = settings.Session()

        test_start_date = timezone.utcnow() - datetime.timedelta(days=1)
        dag, task = create_dummy_dag(
            dag_id="test_sla_miss",
            task_id="dummy",
            email="test@test.com",
            default_args={"start_date": test_start_date, "sla": datetime.timedelta(hours=1)},
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state="Success"))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(task_id="dummy_deleted", dag_id="test_sla_miss", execution_date=test_start_date)
        )

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)

    @patch.object(TaskInstance, "handle_failure")
    def test_execute_on_failure_callbacks(self, mock_ti_handle_failure):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag("example_branch_operator")
            dagrun = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
            task = dag.get_task(task_id="run_this_first")
            ti = TaskInstance(task, run_id=dagrun.run_id, state=State.RUNNING)
            session.add(ti)

        requests = [
            TaskCallbackRequest(
                full_filepath="A", simple_task_instance=SimpleTaskInstance.from_ti(ti), msg="Message"
            )
        ]
        dag_file_processor.execute_callbacks(dagbag, requests, session)
        mock_ti_handle_failure.assert_called_once_with(
            error="Message", test_mode=conf.getboolean("core", "unit_test_mode"), session=session
        )

    @pytest.mark.parametrize(
        ["has_serialized_dag"],
        [pytest.param(True, id="dag_in_db"), pytest.param(False, id="no_dag_found")],
    )
    @patch.object(TaskInstance, "handle_failure")
    def test_execute_on_failure_callbacks_without_dag(self, mock_ti_handle_failure, has_serialized_dag):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag("example_branch_operator")
            dagrun = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
            task = dag.get_task(task_id="run_this_first")
            ti = TaskInstance(task, run_id=dagrun.run_id, state=State.QUEUED)
            session.add(ti)

            if has_serialized_dag:
                assert SerializedDagModel.write_dag(dag, session=session) is True
                session.flush()

        requests = [
            TaskCallbackRequest(
                full_filepath="A", simple_task_instance=SimpleTaskInstance.from_ti(ti), msg="Message"
            )
        ]
        dag_file_processor.execute_callbacks_without_dag(requests, session)
        mock_ti_handle_failure.assert_called_once_with(
            error="Message", test_mode=conf.getboolean("core", "unit_test_mode"), session=session
        )

    def test_failure_callbacks_should_not_drop_hostname(self):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )
        dag_file_processor.UNIT_TEST_MODE = False

        with create_session() as session:
            dag = dagbag.get_dag("example_branch_operator")
            task = dag.get_task(task_id="run_this_first")
            dagrun = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
            ti = TaskInstance(task, run_id=dagrun.run_id, state=State.RUNNING)
            ti.hostname = "test_hostname"
            session.add(ti)

        requests = [
            TaskCallbackRequest(
                full_filepath="A", simple_task_instance=SimpleTaskInstance.from_ti(ti), msg="Message"
            )
        ]
        dag_file_processor.execute_callbacks(dagbag, requests)

        with create_session() as session:
            tis = session.query(TaskInstance)
            assert tis[0].hostname == "test_hostname"

    def test_process_file_should_failure_callback(self, monkeypatch, tmp_path, get_test_dag):
        callback_file = tmp_path.joinpath("callback.txt")
        callback_file.touch()
        monkeypatch.setenv("AIRFLOW_CALLBACK_FILE", str(callback_file))
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=TEST_DAGS_FOLDER, log=mock.MagicMock()
        )

        dag = get_test_dag("test_on_failure_callback")
        task = dag.get_task(task_id="test_on_failure_callback_task")
        with create_session() as session:
            dagrun = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
            ti = dagrun.get_task_instance(task.task_id)
            ti.refresh_from_task(task)

            requests = [
                TaskCallbackRequest(
                    full_filepath=dag.fileloc,
                    simple_task_instance=SimpleTaskInstance.from_ti(ti),
                    msg="Message",
                )
            ]
            dag_file_processor.process_file(dag.fileloc, requests, session=session)

        ti.refresh_from_db()
        msg = " ".join([str(k) for k in ti.key.primary]) + " fired callback"
        assert msg in callback_file.read_text()

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_add_unparseable_file_before_sched_start_creates_import_error(self, tmpdir):
        unparseable_filename = os.path.join(tmpdir, TEMP_DAG_FILENAME)
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(unparseable_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == unparseable_filename
            assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 1)"
            session.rollback()

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_add_unparseable_zip_file_creates_import_error(self, tmpdir):
        zip_filename = os.path.join(tmpdir, "test_zip.zip")
        invalid_dag_filename = os.path.join(zip_filename, TEMP_DAG_FILENAME)
        with ZipFile(zip_filename, "w") as zip_file:
            zip_file.writestr(TEMP_DAG_FILENAME, UNPARSEABLE_DAG_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(zip_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == invalid_dag_filename
            assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 1)"
            session.rollback()

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_dag_model_has_import_error_is_true_when_import_error_exists(self, tmpdir, session):
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_example_bash_operator.py")
        temp_dagfile = os.path.join(tmpdir, TEMP_DAG_FILENAME)
        with open(dag_file) as main_dag, open(temp_dagfile, "w") as next_dag:
            for line in main_dag:
                next_dag.write(line)
        # first we parse the dag
        self._process_file(temp_dagfile, dag_directory=tmpdir, session=session)
        # assert DagModel.has_import_errors is false
        dm = session.query(DagModel).filter(DagModel.fileloc == temp_dagfile).first()
        assert not dm.has_import_errors
        # corrupt the file
        with open(temp_dagfile, "a") as file:
            file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)

        self._process_file(temp_dagfile, dag_directory=tmpdir, session=session)
        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == temp_dagfile
        assert import_error.stacktrace
        dm = session.query(DagModel).filter(DagModel.fileloc == temp_dagfile).first()
        assert dm.has_import_errors

    def test_no_import_errors_with_parseable_dag(self, tmpdir):
        parseable_filename = os.path.join(tmpdir, TEMP_DAG_FILENAME)

        with open(parseable_filename, "w") as parseable_file:
            parseable_file.writelines(PARSEABLE_DAG_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(parseable_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 0

            session.rollback()

    def test_no_import_errors_with_parseable_dag_in_zip(self, tmpdir):
        zip_filename = os.path.join(tmpdir, "test_zip.zip")
        with ZipFile(zip_filename, "w") as zip_file:
            zip_file.writestr(TEMP_DAG_FILENAME, PARSEABLE_DAG_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(zip_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 0

            session.rollback()

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_new_import_error_replaces_old(self, tmpdir):
        unparseable_filename = os.path.join(tmpdir, TEMP_DAG_FILENAME)

        # Generate original import error
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
        session = settings.Session()
        self._process_file(unparseable_filename, dag_directory=tmpdir, session=session)

        # Generate replacement import error (the error will be on the second line now)
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(
                PARSEABLE_DAG_FILE_CONTENTS + os.linesep + UNPARSEABLE_DAG_FILE_CONTENTS
            )
        self._process_file(unparseable_filename, dag_directory=tmpdir, session=session)

        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 2)"

        session.rollback()

    def test_import_error_record_is_updated_not_deleted_and_recreated(self, tmpdir):
        """
        Test that existing import error is updated and new record not created
        for a dag with the same filename
        """
        filename_to_parse = os.path.join(tmpdir, TEMP_DAG_FILENAME)

        # Generate original import error
        with open(filename_to_parse, "w") as file_to_parse:
            file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
        session = settings.Session()
        self._process_file(filename_to_parse, dag_directory=tmpdir, session=session)

        import_error_1 = (
            session.query(errors.ImportError).filter(errors.ImportError.filename == filename_to_parse).one()
        )

        # process the file multiple times
        for _ in range(10):
            self._process_file(filename_to_parse, dag_directory=tmpdir, session=session)

        import_error_2 = (
            session.query(errors.ImportError).filter(errors.ImportError.filename == filename_to_parse).one()
        )

        # assert that the ID of the import error did not change
        assert import_error_1.id == import_error_2.id

    def test_remove_error_clears_import_error(self, tmpdir):
        filename_to_parse = os.path.join(tmpdir, TEMP_DAG_FILENAME)

        # Generate original import error
        with open(filename_to_parse, "w") as file_to_parse:
            file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
        session = settings.Session()
        self._process_file(filename_to_parse, dag_directory=tmpdir, session=session)

        # Remove the import error from the file
        with open(filename_to_parse, "w") as file_to_parse:
            file_to_parse.writelines(PARSEABLE_DAG_FILE_CONTENTS)
        self._process_file(filename_to_parse, dag_directory=tmpdir, session=session)

        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 0

        session.rollback()

    def test_remove_error_clears_import_error_zip(self, tmpdir):
        session = settings.Session()

        # Generate original import error
        zip_filename = os.path.join(tmpdir, "test_zip.zip")
        with ZipFile(zip_filename, "w") as zip_file:
            zip_file.writestr(TEMP_DAG_FILENAME, UNPARSEABLE_DAG_FILE_CONTENTS)
        self._process_file(zip_filename, dag_directory=tmpdir, session=session)

        import_errors = session.query(errors.ImportError).all()
        assert len(import_errors) == 1

        # Remove the import error from the file
        with ZipFile(zip_filename, "w") as zip_file:
            zip_file.writestr(TEMP_DAG_FILENAME, "import os # airflow DAG")
        self._process_file(zip_filename, dag_directory=tmpdir, session=session)

        import_errors = session.query(errors.ImportError).all()
        assert len(import_errors) == 0

        session.rollback()

    def test_import_error_tracebacks(self, tmpdir):
        unparseable_filename = os.path.join(tmpdir, TEMP_DAG_FILENAME)
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(unparseable_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == unparseable_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 3, in <module>\n'
                "    something()\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(
                unparseable_filename, unparseable_filename
            )
            session.rollback()

    @conf_vars({("core", "dagbag_import_error_traceback_depth"): "1"})
    def test_import_error_traceback_depth(self, tmpdir):
        unparseable_filename = os.path.join(tmpdir, TEMP_DAG_FILENAME)
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(unparseable_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == unparseable_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(unparseable_filename)

            session.rollback()

    def test_import_error_tracebacks_zip(self, tmpdir):
        invalid_zip_filename = os.path.join(tmpdir, "test_zip_invalid.zip")
        invalid_dag_filename = os.path.join(invalid_zip_filename, TEMP_DAG_FILENAME)
        with ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
            invalid_zip_file.writestr(TEMP_DAG_FILENAME, INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(invalid_zip_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == invalid_dag_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 3, in <module>\n'
                "    something()\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(
                invalid_dag_filename, invalid_dag_filename
            )
            session.rollback()

    @conf_vars({("core", "dagbag_import_error_traceback_depth"): "1"})
    def test_import_error_tracebacks_zip_depth(self, tmpdir):
        invalid_zip_filename = os.path.join(tmpdir, "test_zip_invalid.zip")
        invalid_dag_filename = os.path.join(invalid_zip_filename, TEMP_DAG_FILENAME)
        with ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
            invalid_zip_file.writestr(TEMP_DAG_FILENAME, INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(invalid_zip_filename, dag_directory=tmpdir, session=session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == invalid_dag_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(invalid_dag_filename)
            session.rollback()

    @conf_vars({("logging", "dag_processor_log_target"): "stdout"})
    @mock.patch("airflow.dag_processing.processor.settings.dispose_orm", MagicMock)
    @mock.patch("airflow.dag_processing.processor.redirect_stdout")
    def test_dag_parser_output_when_logging_to_stdout(self, mock_redirect_stdout_for_file):
        processor = DagFileProcessorProcess(
            file_path="abc.txt",
            pickle_dags=False,
            dag_ids=[],
            dag_directory=[],
            callback_requests=[],
        )
        processor._run_file_processor(
            result_channel=MagicMock(),
            parent_channel=MagicMock(),
            file_path="fake_file_path",
            pickle_dags=False,
            dag_ids=[],
            thread_name="fake_thread_name",
            callback_requests=[],
            dag_directory=[],
        )
        mock_redirect_stdout_for_file.assert_not_called()

    @conf_vars({("logging", "dag_processor_log_target"): "file"})
    @mock.patch("airflow.dag_processing.processor.settings.dispose_orm", MagicMock)
    @mock.patch("airflow.dag_processing.processor.redirect_stdout")
    def test_dag_parser_output_when_logging_to_file(self, mock_redirect_stdout_for_file):
        processor = DagFileProcessorProcess(
            file_path="abc.txt",
            pickle_dags=False,
            dag_ids=[],
            dag_directory=[],
            callback_requests=[],
        )
        processor._run_file_processor(
            result_channel=MagicMock(),
            parent_channel=MagicMock(),
            file_path="fake_file_path",
            pickle_dags=False,
            dag_ids=[],
            thread_name="fake_thread_name",
            callback_requests=[],
            dag_directory=[],
        )
        mock_redirect_stdout_for_file.assert_called_once()


class TestProcessorAgent:
    @pytest.fixture(autouse=True)
    def per_test(self):
        self.processor_agent = None
        yield
        if self.processor_agent:
            self.processor_agent.end()

    def test_error_when_waiting_in_async_mode(self, tmp_path):
        self.processor_agent = DagFileProcessorAgent(
            dag_directory=tmp_path,
            max_runs=1,
            processor_timeout=datetime.timedelta(1),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )
        self.processor_agent.start()
        with pytest.raises(RuntimeError, match="wait_until_finished should only be called in sync_mode"):
            self.processor_agent.wait_until_finished()

    def test_default_multiprocessing_behaviour(self, tmp_path):
        self.processor_agent = DagFileProcessorAgent(
            dag_directory=tmp_path,
            max_runs=1,
            processor_timeout=datetime.timedelta(1),
            dag_ids=[],
            pickle_dags=False,
            async_mode=False,
        )
        self.processor_agent.start()
        self.processor_agent.run_single_parsing_loop()
        self.processor_agent.wait_until_finished()

    @conf_vars({("core", "mp_start_method"): "spawn"})
    def test_spawn_multiprocessing_behaviour(self, tmp_path):
        self.processor_agent = DagFileProcessorAgent(
            dag_directory=tmp_path,
            max_runs=1,
            processor_timeout=datetime.timedelta(1),
            dag_ids=[],
            pickle_dags=False,
            async_mode=False,
        )
        self.processor_agent.start()
        self.processor_agent.run_single_parsing_loop()
        self.processor_agent.wait_until_finished()
