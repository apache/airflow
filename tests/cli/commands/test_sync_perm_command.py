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

from airflow.cli import cli_parser
from airflow.cli.commands import sync_perm_command
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.security import permissions


class TestCliSyncPerm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.sync_perm_command.cached_app")
    @mock.patch("airflow.cli.commands.sync_perm_command.DagBag")
    def test_cli_sync_perm(self, dagbag_mock, mock_cached_app):
        dags = [
            DAG('has_access_control', access_control={'Public': {permissions.ACTION_CAN_READ}}),
            DAG('no_access_control'),
        ]

        collect_dags_from_db_mock = mock.Mock()
        dagbag = mock.Mock()

        dagbag.dags = {dag.dag_id: dag for dag in dags}
        dagbag.collect_dags_from_db = collect_dags_from_db_mock
        dagbag_mock.return_value = dagbag

        appbuilder = mock_cached_app.return_value.appbuilder
        appbuilder.sm = mock.Mock()

        args = self.parser.parse_args(['sync-perm'])
        sync_perm_command.sync_perm(args)

        assert appbuilder.sm.sync_roles.call_count == 1

        dagbag_mock.assert_called_once_with(read_dags_from_db=True)
        collect_dags_from_db_mock.assert_called_once_with()
        assert 2 == len(appbuilder.sm.sync_perm_for_dag.mock_calls)
        appbuilder.sm.sync_perm_for_dag.assert_any_call(
            'has_access_control', {'Public': {permissions.ACTION_CAN_READ}}
        )
        appbuilder.sm.sync_perm_for_dag.assert_any_call(
            'no_access_control',
            None,
        )
        appbuilder.add_permissions.assert_called_once_with(update_perms=True)
