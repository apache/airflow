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
from unittest.mock import ANY, Mock, patch

import pytest

from airflow.cli import cli_parser
from airflow.providers.amazon.aws.auth_manager.cli.avp_commands import init_avp, update_schema

from tests_common.test_utils.compat import AIRFLOW_V_2_8_PLUS
from tests_common.test_utils.config import conf_vars

mock_boto3 = Mock()

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_8_PLUS, reason="Test requires Airflow 2.8+"),
    pytest.mark.skip_if_database_isolation_mode,
]


@pytest.mark.db_test
class TestAvpCommands:
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
    @patch("airflow.providers.amazon.aws.auth_manager.cli.avp_commands._get_client")
    def test_init_avp_with_no_existing_resources(self, mock_get_client, dry_run, verbose):
        mock_get_client.return_value = mock_boto3

        policy_store_description = "test-policy-store"
        policy_store_id = "test-policy-store-id"

        paginator = Mock()
        paginator.paginate.return_value = []

        mock_boto3.get_paginator.return_value = paginator
        mock_boto3.create_policy_store.return_value = {"policyStoreId": policy_store_id}

        with conf_vars({("database", "check_migrations"): "False"}):
            params = [
                "aws-auth-manager",
                "init-avp",
                "--policy-store-description",
                policy_store_description,
            ]
            if dry_run:
                params.append("--dry-run")
            if verbose:
                params.append("--verbose")
            init_avp(self.arg_parser.parse_args(params))

        if dry_run:
            mock_boto3.create_policy_store.assert_not_called()
            mock_boto3.put_schema.assert_not_called()
        else:
            mock_boto3.create_policy_store.assert_called_once_with(
                validationSettings={
                    "mode": "STRICT",
                },
                description=policy_store_description,
            )
            mock_boto3.put_schema.assert_called_once_with(
                policyStoreId=policy_store_id,
                definition={
                    "cedarJson": ANY,
                },
            )

    @pytest.mark.parametrize(
        "dry_run, verbose",
        [
            (False, False),
            (True, True),
        ],
    )
    @patch("airflow.providers.amazon.aws.auth_manager.cli.avp_commands._get_client")
    def test_init_avp_with_existing_resources(self, mock_get_client, dry_run, verbose):
        mock_get_client.return_value = mock_boto3

        policy_store_description = "test-policy-store"
        policy_store_id = "test-policy-store-id"

        paginator = Mock()
        paginator.paginate.return_value = [
            {"policyStores": [{"description": policy_store_description, "policyStoreId": policy_store_id}]}
        ]

        mock_boto3.get_paginator.return_value = paginator

        with conf_vars({("database", "check_migrations"): "False"}):
            params = [
                "aws-auth-manager",
                "init-avp",
                "--policy-store-description",
                policy_store_description,
            ]
            if dry_run:
                params.append("--dry-run")
            if verbose:
                params.append("--verbose")
            init_avp(self.arg_parser.parse_args(params))

        mock_boto3.create_policy_store.assert_not_called()
        mock_boto3.update_policy_store.assert_not_called()
        mock_boto3.put_schema.assert_not_called()

    @pytest.mark.parametrize(
        "dry_run, verbose",
        [
            (False, False),
            (True, True),
        ],
    )
    @patch("airflow.providers.amazon.aws.auth_manager.cli.avp_commands._get_client")
    def test_update_schema(self, mock_get_client, dry_run, verbose):
        mock_get_client.return_value = mock_boto3

        policy_store_id = "test-policy-store-id"

        with conf_vars({("database", "check_migrations"): "False"}):
            params = [
                "aws-auth-manager",
                "update-avp-schema",
                "--policy-store-id",
                policy_store_id,
            ]
            if dry_run:
                params.append("--dry-run")
            if verbose:
                params.append("--verbose")
            update_schema(self.arg_parser.parse_args(params))

        if dry_run:
            mock_boto3.put_schema.assert_not_called()
        else:
            mock_boto3.put_schema.assert_called_once_with(
                policyStoreId=policy_store_id,
                definition={
                    "cedarJson": ANY,
                },
            )
