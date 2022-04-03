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
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow.cli import cli_parser
from airflow.cli.__main__ import airflow_cmd
from airflow.cli.commands import sync_perm_command


@pytest.fixture
def setup_test(request):
    if request.param == "click":
        module = "airflow.cli.commands.sync_perm.cached_app"
    else:
        module = "airflow.cli.commands.sync_perm_command.cached_app"

    with mock.patch(module) as mocked_cached_app:
        appbuilder = mocked_cached_app.return_value.appbuilder
        appbuilder.sm = mock.Mock()
        yield request.param, appbuilder


class TestCliSyncPerm:
    @pytest.mark.parametrize("setup_test", ["click", "argparse"], indirect=True)
    def test_cli_sync_perm_1(self, setup_test):
        parser, appbuilder = setup_test
        cli_args = ['sync-perm']

        if parser == "click":
            runner = CliRunner()
            runner.invoke(airflow_cmd, cli_args)
        else:
            parser = cli_parser.get_parser()
            args = parser.parse_args(cli_args)
            sync_perm_command.sync_perm(args)

        appbuilder.add_permissions.assert_called_once_with(update_perms=True)
        appbuilder.sm.sync_roles.assert_called_once_with()
        appbuilder.sm.create_dag_specific_permissions.assert_not_called()

    @pytest.mark.parametrize("setup_test", ["click", "argparse"], indirect=True)
    def test_cli_sync_perm_include_dags(self, setup_test):
        parser, appbuilder = setup_test
        cli_args = ['sync-perm', '--include-dags']

        if parser == "click":
            runner = CliRunner()
            runner.invoke(airflow_cmd, cli_args)
        else:
            parser = cli_parser.get_parser()
            args = parser.parse_args(cli_args)
            sync_perm_command.sync_perm(args)

        appbuilder.add_permissions.assert_called_once_with(update_perms=True)
        appbuilder.sm.sync_roles.assert_called_once_with()
        appbuilder.sm.create_dag_specific_permissions.assert_called_once_with()
