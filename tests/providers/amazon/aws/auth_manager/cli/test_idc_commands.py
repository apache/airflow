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

import importlib
from unittest.mock import Mock, patch

import pytest

from airflow.cli import cli_parser
from airflow.providers.amazon.aws.auth_manager.cli.idc_commands import init_idc
from tests.test_utils.compat import AIRFLOW_V_2_8_PLUS
from tests.test_utils.config import conf_vars

mock_boto3 = Mock()

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_8_PLUS, reason="Test requires Airflow 2.8+"),
    pytest.mark.skip_if_database_isolation_mode,
]


@pytest.mark.db_test
class TestIdcCommands:
    def setup_method(self):
        mock_boto3.reset_mock()

    @classmethod
    def setup_class(cls):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager"
            }
        ):
            importlib.reload(cli_parser)
            cls.arg_parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        "dry_run, verbose",
        [
            (False, False),
            (True, True),
        ],
    )
    @patch("airflow.providers.amazon.aws.auth_manager.cli.idc_commands._get_client")
    def test_init_idc_with_no_existing_resources(self, mock_get_client, dry_run, verbose):
        mock_get_client.return_value = mock_boto3

        instance_name = "test-instance"
        instance_arn = "test-instance-arn"
        application_name = "test-application"
        application_arn = "test-application-arn"

        paginator = Mock()
        paginator.paginate.return_value = []

        mock_boto3.list_instances.return_value = {"Instances": []}
        mock_boto3.create_instance.return_value = {"InstanceArn": instance_arn}
        mock_boto3.get_paginator.return_value = paginator
        mock_boto3.create_application.return_value = {"ApplicationArn": application_arn}

        with conf_vars({("database", "check_migrations"): "False"}):
            params = [
                "aws-auth-manager",
                "init-identity-center",
                "--instance-name",
                instance_name,
                "--application-name",
                application_name,
            ]
            if dry_run:
                params.append("--dry-run")
            if verbose:
                params.append("--verbose")
            init_idc(self.arg_parser.parse_args(params))

        mock_boto3.list_instances.assert_called_once_with()
        if not dry_run:
            mock_boto3.create_instance.assert_called_once_with(Name=instance_name)
            mock_boto3.create_application.assert_called_once()

    @pytest.mark.parametrize(
        "dry_run, verbose",
        [
            (False, False),
            (True, True),
        ],
    )
    @patch("airflow.providers.amazon.aws.auth_manager.cli.idc_commands._get_client")
    def test_init_idc_with_existing_resources(self, mock_get_client, dry_run, verbose):
        mock_get_client.return_value = mock_boto3

        instance_name = "test-instance"
        instance_arn = "test-instance-arn"
        application_name = "test-application"
        application_arn = "test-application-arn"

        paginator = Mock()
        paginator.paginate.return_value = [
            {"Applications": [{"Name": application_name, "ApplicationArn": application_arn}]}
        ]

        mock_boto3.list_instances.return_value = {"Instances": [{"InstanceArn": instance_arn}]}
        mock_boto3.get_paginator.return_value = paginator

        with conf_vars({("database", "check_migrations"): "False"}):
            params = [
                "aws-auth-manager",
                "init-identity-center",
                "--instance-name",
                instance_name,
                "--application-name",
                application_name,
            ]
            if dry_run:
                params.append("--dry-run")
            if verbose:
                params.append("--verbose")
            init_idc(self.arg_parser.parse_args(params))

        mock_boto3.list_instances.assert_called_once_with()
        mock_boto3.create_instance.assert_not_called()
        mock_boto3.create_application.assert_not_called()
