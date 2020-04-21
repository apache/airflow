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
#
import unittest
from unittest import mock

from airflow import DAG, models
from airflow.bin import cli
from airflow.cli.commands import sync_perm_command
from airflow.settings import Session


class TestCliSyncPerm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def setUp(self):
        from airflow.www import app as application
        self.app, self.appbuilder = application.create_app(session=Session, testing=True)

    @mock.patch("airflow.cli.commands.sync_perm_command.DagBag")
    def test_cli_sync_perm(self, dagbag_mock):
        self.expect_dagbag_contains([
            DAG('has_access_control',
                access_control={
                    'Public': {'can_dag_read'}
                }),
            DAG('no_access_control')
        ], dagbag_mock)
        self.appbuilder.sm = mock.Mock()

        args = self.parser.parse_args([
            'sync_perm'
        ])
        sync_perm_command.sync_perm(args)

        assert self.appbuilder.sm.sync_roles.call_count == 1

        self.assertEqual(2,
                         len(self.appbuilder.sm.sync_perm_for_dag.mock_calls))
        self.appbuilder.sm.sync_perm_for_dag.assert_any_call(
            'has_access_control',
            {'Public': {'can_dag_read'}}
        )
        self.appbuilder.sm.sync_perm_for_dag.assert_any_call(
            'no_access_control',
            None,
        )

    def expect_dagbag_contains(self, dags, dagbag_mock):
        dagbag = mock.Mock()
        dagbag.dags = {dag.dag_id: dag for dag in dags}
        dagbag_mock.return_value = dagbag
