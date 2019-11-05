# -*- coding: utf-8 -*-
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

import errno
import functools
import logging
import os
import subprocess
import sys
import unittest
from tests.compat import mock
from copy import deepcopy

from airflow import jobs, models
from airflow.utils.db import add_default_pool_if_not_exists
from airflow.utils.state import State
from airflow.utils.timezone import datetime

DEV_NULL = '/dev/null'
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')
TEST_DAG_CORRUPTED_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags_corrupted')
TEST_UTILS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'test_utils')
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_USER = 'airflow_test_user'


logger = logging.getLogger(__name__)


def mock_custom_module_path(path):
    """
    This decorator adds a path to sys.path to simulate running the current script with the ``PYTHONPATH``
    environment variable set and sets the environment variable ``PYTHONPATH`` to change the
    module load directory for child scripts.
    """
    def wrapper(func):
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            copy_sys_path = deepcopy(sys.path)
            sys.path.append(path)
            try:
                with mock.patch.dict('os.environ', {'PYTHONPATH': path}):
                    return func(*args, **kwargs)
            finally:
                sys.path = copy_sys_path
        return decorator

    return wrapper


def grant_permissions():
    airflow_home = os.environ['AIRFLOW_HOME']
    subprocess.check_call(
        'find "%s" -exec sudo chmod og+w {} +; sudo chmod og+rx /root' % airflow_home, shell=True)


def revoke_permissions():
    airflow_home = os.environ['AIRFLOW_HOME']
    subprocess.check_call(
        'find "%s" -exec sudo chmod og-w {} +; sudo chmod og-rx /root' % airflow_home, shell=True)


def check_original_docker_image():
    if not os.path.isfile('/.dockerenv') or os.environ.get('PYTHON_BASE_IMAGE') is None:
        raise unittest.SkipTest("""Adding/removing a user as part of a test is very bad for host os
(especially if the user already existed to begin with on the OS), therefore we check if we run inside a
the official docker container and only allow to run the test there. This is done by checking /.dockerenv
file (always present inside container) and checking for PYTHON_BASE_IMAGE variable.
""")


def create_user():
    try:
        subprocess.check_output(['sudo', 'useradd', '-m', TEST_USER, '-g',
                                 str(os.getegid())])
    except OSError as e:
        if e.errno == errno.ENOENT:
            raise unittest.SkipTest(
                "The 'useradd' command did not exist so unable to test "
                "impersonation; Skipping Test. These tests can only be run on a "
                "linux host that supports 'useradd'."
            )
        else:
            raise unittest.SkipTest(
                "The 'useradd' command exited non-zero; Skipping tests. Does the "
                "current user have permission to run 'useradd' without a password "
                "prompt (check sudoers file)?"
            )


class TestImpersonation(unittest.TestCase):

    def setUp(self):
        check_original_docker_image()
        grant_permissions()
        add_default_pool_if_not_exists()
        self.dagbag = models.DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        logger.info('Loaded DAGS:')
        logger.info(self.dagbag.dagbag_report())

        create_user()

    def tearDown(self):
        subprocess.check_output(['sudo', 'userdel', '-r', TEST_USER])
        revoke_permissions()

    def run_backfill(self, dag_id, task_id):
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        jobs.BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE).run()

        ti = models.TaskInstance(
            task=dag.get_task(task_id),
            execution_date=DEFAULT_DATE)
        ti.refresh_from_db()

        self.assertEqual(ti.state, State.SUCCESS)

    def test_impersonation(self):
        """
        Tests that impersonating a unix user works
        """
        self.run_backfill(
            'test_impersonation',
            'test_impersonated_user'
        )

    def test_no_impersonation(self):
        """
        If default_impersonation=None, tests that the job is run
        as the current user (which will be a sudoer)
        """
        self.run_backfill(
            'test_no_impersonation',
            'test_superuser',
        )

    def test_default_impersonation(self):
        """
        If default_impersonation=TEST_USER, tests that the job defaults
        to running as TEST_USER for a test without run_as_user set
        """
        os.environ['AIRFLOW__CORE__DEFAULT_IMPERSONATION'] = TEST_USER

        try:
            self.run_backfill(
                'test_default_impersonation',
                'test_deelevated_user'
            )
        finally:
            del os.environ['AIRFLOW__CORE__DEFAULT_IMPERSONATION']

    def test_impersonation_subdag(self):
        """
        Tests that impersonation using a subdag correctly passes the right configuration
        :return:
        """
        self.run_backfill(
            'impersonation_subdag',
            'test_subdag_operation'
        )


class TestImpersonationWithCustomPythonPath(unittest.TestCase):

    @mock_custom_module_path(TEST_UTILS_FOLDER)
    def setUp(self):
        check_original_docker_image()
        grant_permissions()
        add_default_pool_if_not_exists()
        self.dagbag = models.DagBag(
            dag_folder=TEST_DAG_CORRUPTED_FOLDER,
            include_examples=False,
        )
        logger.info('Loaded DAGS:')
        logger.info(self.dagbag.dagbag_report())

        create_user()

    def tearDown(self):
        subprocess.check_output(['sudo', 'userdel', '-r', TEST_USER])
        revoke_permissions()

    def run_backfill(self, dag_id, task_id):
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        jobs.BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE).run()

        ti = models.TaskInstance(
            task=dag.get_task(task_id),
            execution_date=DEFAULT_DATE)
        ti.refresh_from_db()

        self.assertEqual(ti.state, State.SUCCESS)

    @mock_custom_module_path(TEST_UTILS_FOLDER)
    def test_impersonation_custom(self):
        """
        Tests that impersonation using a unix user works with custom packages in
        PYTHONPATH
        """
        # PYTHONPATH is already set in script triggering tests
        assert 'PYTHONPATH' in os.environ

        self.run_backfill(
            'impersonation_with_custom_pkg',
            'exec_python_fn'
        )
