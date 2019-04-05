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

import unittest
import sqlalchemy
import airflow
from argparse import Namespace
from tests.compat import mock, patch

patch('airflow.utils.cli.action_logging', lambda x: x).start()
from airflow.bin import cli # noqa
mock_args = Namespace(queues=1, concurrency=1)


class TestWorkerPrecheck(unittest.TestCase):

    def setUp(self):
        airflow.configuration.load_test_config()

    @mock.patch('airflow.settings.validate_session')
    def test_error(self, mock_validate_session):
        """
        Test to verify the exit mechanism of airflow-worker cli
        by mocking validate_session method
        """
        mock_validate_session.return_value = False
        with self.assertRaises(SystemExit) as cm:
            # airflow.bin.cli.worker(mock_args)
            cli.worker(mock_args)
        self.assertEqual(cm.exception.code, 1)

    @mock.patch('airflow.configuration.conf.getboolean')
    def test_worker_precheck_exception(self, mock_getboolean):
        """
        Test to check the behaviour of validate_session method
        when worker_precheck is absent in airflow configuration
        """
        mock_getboolean.return_value = False

        self.assertTrue(airflow.settings.validate_session())
        mock_getboolean.assert_called_once_with('core', 'worker_precheck', fallback=False)

    @mock.patch('sqlalchemy.orm.session.Session.execute')
    @mock.patch('airflow.configuration.conf.getboolean')
    def test_validate_session_dbapi_exception(self, mock_getboolean, mock_session):
        """
        Test to validate connection failure scenario on SELECT 1 query
        """
        mock_getboolean.return_value = True
        mock_session.side_effect = sqlalchemy.exc.OperationalError("m1", "m2", "m3", "m4")
        self.assertEqual(airflow.settings.validate_session(), False)
