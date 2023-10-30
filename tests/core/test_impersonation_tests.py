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

import contextlib
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from airflow.configuration import conf
from airflow.jobs.backfill_job_runner import BackfillJobRunner
from airflow.jobs.job import Job, run_job
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.db import add_default_pool_if_not_exists
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils import db

pytestmark = pytest.mark.db_test

DEV_NULL = "/dev/null"
TEST_ROOT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TEST_DAG_FOLDER = os.path.join(TEST_ROOT_FOLDER, "dags")
TEST_DAG_CORRUPTED_FOLDER = os.path.join(TEST_ROOT_FOLDER, "dags_corrupted")
TEST_UTILS_FOLDER = os.path.join(TEST_ROOT_FOLDER, "test_utils")
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_USER = "airflow_test_user"


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def set_permissions(settings: dict[Path | str, int]):
    """Helper for recursively set permissions only for specific path and revert it back."""
    orig_permissions = []
    try:
        print(" Change file/directory permissions ".center(72, "+"))
        for path, mode in settings.items():
            if isinstance(path, str):
                path = Path(path)
            if len(path.parts) <= 1:
                raise SystemError(f"Unable to change permission for the root directory: {path}.")

            st_mode = os.stat(path).st_mode
            new_st_mode = st_mode | mode
            if new_st_mode > st_mode:
                print(f"Path={path}, mode={oct(st_mode)}, new_mode={oct(new_st_mode)}")
                orig_permissions.append((path, st_mode))
                os.chmod(path, new_st_mode)

            parent_path = path.parent
            while len(parent_path.parts) > 1:
                st_mode = os.stat(parent_path).st_mode
                new_st_mode = st_mode | 0o755  # grant r/o access to the parent directories
                if new_st_mode > st_mode:
                    print(f"Path={parent_path}, mode={oct(st_mode)}, new_mode={oct(new_st_mode)}")
                    orig_permissions.append((parent_path, st_mode))
                    os.chmod(parent_path, new_st_mode)

                parent_path = parent_path.parent
        print("".center(72, "+"))
        yield
    finally:
        for path, mode in orig_permissions:
            os.chmod(path, mode)


@pytest.fixture
def check_original_docker_image():
    if not os.path.isfile("/.dockerenv") or os.environ.get("PYTHON_BASE_IMAGE") is None:
        raise pytest.skip(
            "Adding/removing a user as part of a test is very bad for host os "
            "(especially if the user already existed to begin with on the OS), "
            "therefore we check if we run inside a the official docker container "
            "and only allow to run the test there. This is done by checking /.dockerenv file "
            "(always present inside container) and checking for PYTHON_BASE_IMAGE variable."
        )
    yield


@pytest.fixture
def create_user(check_original_docker_image):
    try:
        subprocess.check_output(
            ["sudo", "useradd", "-m", TEST_USER, "-g", str(os.getegid())], stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        command = e.cmd[1]
        if e.returncode != 9:  # pass: username already exists
            raise pytest.skip(
                f"{e} Skipping tests.\n"
                f"Does command {command!r} exists and the current user have permission to run "
                f"{command!r} without a password prompt (check sudoers file)?\n"
                f"{e.stdout.decode() if e.stdout else ''}"
            )
    yield TEST_USER
    subprocess.check_call(["sudo", "userdel", "-r", TEST_USER])


@pytest.fixture
def create_airflow_home(create_user, tmp_path, monkeypatch):
    sql_alchemy_conn = conf.get_mandatory_value("database", "sql_alchemy_conn")
    username = create_user
    airflow_home = tmp_path / "airflow-home"

    if not airflow_home.exists():
        airflow_home.mkdir(parents=True, exist_ok=True)

    permissions = {
        # By default, ``tmp_path`` save temporary files/directories in user temporary directory
        # something like: `/tmp/pytest-of-root` and other users might be limited to access to it,
        # so we need to grant at least r/o access to everyone.
        airflow_home: 0o777,
        # Grant full access to system-wide temporary directory (if for some reason it not set)
        tempfile.gettempdir(): 0o777,
    }

    if sql_alchemy_conn.lower().startswith("sqlite"):
        sqlite_file = Path(sql_alchemy_conn.replace("sqlite:///", ""))
        # Grant r/w access to sqlite file.
        permissions[sqlite_file] = 0o766
        # Grant r/w access to directory where sqlite file placed
        # otherwise we can't create temporary files during write access.
        permissions[sqlite_file.parent] = 0o777

    monkeypatch.setenv("AIRFLOW_HOME", str(airflow_home))

    with set_permissions(permissions):
        subprocess.check_call(["sudo", "chown", f"{username}:root", str(airflow_home), "-R"], close_fds=True)
        yield airflow_home


class BaseImpersonationTest:
    dagbag: DagBag

    @pytest.fixture(autouse=True)
    def setup_impersonation_tests(self, create_airflow_home):
        """Setup test cases for all impersonation tests."""
        db.clear_db_runs()
        db.clear_db_jobs()
        add_default_pool_if_not_exists()
        yield
        db.clear_db_runs()
        db.clear_db_jobs()

    @staticmethod
    def get_dagbag(dag_folder):
        """Get DagBag and print statistic into the log."""
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        logger.info("Loaded DAGs:")
        logger.info(dagbag.dagbag_report())
        return dagbag

    def run_backfill(self, dag_id, task_id):
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        job = Job()
        job_runner = BackfillJobRunner(job=job, dag=dag, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        run_job(job=job, execute_callable=job_runner._execute)
        run_id = DagRun.generate_run_id(DagRunType.BACKFILL_JOB, execution_date=DEFAULT_DATE)
        ti = TaskInstance(task=dag.get_task(task_id), run_id=run_id)
        ti.refresh_from_db()

        assert ti.state == State.SUCCESS


class TestImpersonation(BaseImpersonationTest):
    @classmethod
    def setup_class(cls):
        cls.dagbag = cls.get_dagbag(TEST_DAG_FOLDER)

    def test_impersonation(self):
        """
        Tests that impersonating a unix user works
        """
        self.run_backfill("test_impersonation", "test_impersonated_user")

    def test_no_impersonation(self):
        """
        If default_impersonation=None, tests that the job is run
        as the current user (which will be a sudoer)
        """
        self.run_backfill(
            "test_no_impersonation",
            "test_superuser",
        )

    def test_default_impersonation(self, monkeypatch):
        """
        If default_impersonation=TEST_USER, tests that the job defaults
        to running as TEST_USER for a test without 'run_as_user' set.
        """
        monkeypatch.setenv("AIRFLOW__CORE__DEFAULT_IMPERSONATION", TEST_USER)
        self.run_backfill("test_default_impersonation", "test_deelevated_user")

    @pytest.mark.execution_timeout(150)
    def test_impersonation_subdag(self):
        """Tests that impersonation using a subdag correctly passes the right configuration."""
        self.run_backfill("impersonation_subdag", "test_subdag_operation")


class TestImpersonationWithCustomPythonPath(BaseImpersonationTest):
    @pytest.fixture(autouse=True)
    def setup_dagbag(self, monkeypatch):
        # Adds a path to sys.path to simulate running the current script with `PYTHONPATH` env variable set.
        monkeypatch.syspath_prepend(TEST_UTILS_FOLDER)
        self.dagbag = self.get_dagbag(TEST_DAG_CORRUPTED_FOLDER)
        monkeypatch.undo()
        yield

    def test_impersonation_custom(self, monkeypatch):
        """
        Tests that impersonation using a unix user works with custom packages in PYTHONPATH.
        """
        monkeypatch.setenv("PYTHONPATH", TEST_UTILS_FOLDER)
        assert TEST_UTILS_FOLDER not in sys.path
        self.run_backfill("impersonation_with_custom_pkg", "exec_python_fn")
