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
import os
import subprocess
import unittest
import logging

from airflow import jobs, models
from airflow.utils.state import State
from airflow.utils.timezone import datetime

DEV_NULL = '/dev/null'
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_USER = 'airflow_test_user'

logger = logging.getLogger(__name__)

# TODO(aoen): Adding/remove a user as part of a test is very bad (especially if the user
# already existed to begin with on the OS), this logic should be moved into a test
# that is wrapped in a container like docker so that the user can be safely added/removed.
# When this is done we can also modify the sudoers file to ensure that useradd will work
# without any manual modification of the sudoers file by the agent that is running these
# tests.


class ImpersonationTest(unittest.TestCase):
    def setUp(self):
        self.dagbag = models.DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        logger.info('Loaded DAGS:')
        logger.info(self.dagbag.dagbag_report())

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

    def tearDown(self):
        subprocess.check_output(['sudo', 'userdel', '-r', TEST_USER])

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

    def test_impersonation_subdag(self):
        """
        Tests that impersonation using a subdag correctly passes the right configuration
        :return:
        """
        self.run_backfill(
            'impersonation_subdag',
            'test_subdag_operation'
        )
