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

from unittest import mock

from airflow.cli import cli_parser
from airflow.cli.commands import sync_perm_command


class TestCliSyncPerm:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.sync_perm_command.cached_app")
    def test_cli_sync_perm(self, mock_cached_app):
        appbuilder = mock_cached_app.return_value.appbuilder
        appbuilder.sm = mock.Mock()

        args = self.parser.parse_args(['sync-perm'])
        sync_perm_command.sync_perm(args)

        appbuilder.add_permissions.assert_called_once_with(update_perms=True)
        appbuilder.sm.sync_roles.assert_called_once_with()
        appbuilder.sm.create_dag_specific_permissions.assert_not_called()

    @mock.patch("airflow.cli.commands.sync_perm_command.cached_app")
    def test_cli_sync_perm_include_dags(self, mock_cached_app):
        appbuilder = mock_cached_app.return_value.appbuilder
        appbuilder.sm = mock.Mock()

        args = self.parser.parse_args(['sync-perm', '--include-dags'])
        sync_perm_command.sync_perm(args)

        appbuilder.add_permissions.assert_called_once_with(update_perms=True)
        appbuilder.sm.sync_roles.assert_called_once_with()
        appbuilder.sm.create_dag_specific_permissions.assert_called_once_with()
