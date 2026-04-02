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
from io import StringIO
from unittest import mock

import pytest
import time_machine

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


class TestGetEksToken:
    @time_machine.travel("1995-02-14", tick=False)
    @pytest.mark.parametrize(
        ("args", "expected_region_name"),
        [
            [
                [
                    "airflow.providers.amazon.aws.utils.eks_get_token",
                    "--region-name",
                    "test-region",
                    "--cluster-name",
                    "test-cluster",
                    "--sts-url",
                    "https://sts.test-region.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                ],
                "test-region",
            ],
            [
                [
                    "airflow.providers.amazon.src.airflow.providers.amazon.aws.utils.eks_get_token"
                    if AIRFLOW_V_3_0_PLUS
                    else "airflow.providers.amazon.aws.utils.eks_get_token",
                    "--region-name",
                    "test-region",
                    "--cluster-name",
                    "test-cluster",
                    "--sts-url",
                    "https://sts.test-region.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                ],
                "test-region",
            ],
            [
                [
                    "airflow.providers.amazon.aws.utils.eks_get_token",
                    "--cluster-name",
                    "test-cluster",
                    "--sts-url",
                    "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                ],
                None,
            ],
        ],
    )
    def test_run(self, args, expected_region_name):
        # Instead of trying to mock deep into the CLI execution context, mock the main function itself
        with mock.patch(
            "airflow.providers.amazon.aws.utils.eks_get_token.fetch_access_token_for_cluster"
        ) as mock_fetch_token:
            mock_fetch_token.return_value = "k8s-aws-v1.aHR0cDovL2V4YW1wbGUuY29t"

            with mock.patch("sys.argv", args), contextlib.redirect_stdout(StringIO()) as temp_stdout:
                # Import and directly call the main function rather than using runpy
                from airflow.providers.amazon.aws.utils.eks_get_token import main

                main()

                output = temp_stdout.getvalue()
                token = "token: k8s-aws-v1.aHR0cDovL2V4YW1wbGUuY29t"
                expected_token = output.split(",")[1].strip()
                expected_expiration_timestamp = output.split(",")[0].split(":")[1].strip()
                assert expected_token == token
                assert expected_expiration_timestamp.startswith("1995-02-")

                # Extract the sts-url from args
                sts_url = None
                if "--sts-url" in args:
                    sts_url_idx = args.index("--sts-url") + 1
                    if sts_url_idx < len(args):
                        sts_url = args[sts_url_idx]

                # Verify fetch_access_token_for_cluster was called with correct parameters
                mock_fetch_token.assert_called_once_with(
                    "test-cluster", sts_url, region_name=expected_region_name
                )

    @mock.patch("airflow.providers.amazon.aws.utils.eks_get_token.RequestSigner")
    @mock.patch("boto3.Session")
    def test_fetch_access_token_for_cluster(self, mock_session, mock_signer):
        """Test the standalone fetch_access_token_for_cluster function."""
        from airflow.providers.amazon.aws.utils.eks_get_token import fetch_access_token_for_cluster

        # Mock the session and client
        mock_session_instance = mock_session.return_value
        mock_eks_client = mock_session_instance.client.return_value
        mock_session_instance.region_name = "us-east-1"

        # Mock the RequestSigner
        mock_signer_instance = mock_signer.return_value
        mock_signer_instance.generate_presigned_url.return_value = "http://example.com"

        result = fetch_access_token_for_cluster(
            eks_cluster_name="test-cluster",
            sts_url="https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
            region_name="us-east-1",
        )

        # Verify session creation
        mock_session.assert_called_once_with(region_name="us-east-1")
        mock_session_instance.client.assert_called_once_with("eks")

        # Verify RequestSigner was called correctly
        mock_signer.assert_called_once_with(
            service_id=mock_eks_client.meta.service_model.service_id,
            region_name="us-east-1",
            signing_name="sts",
            signature_version="v4",
            credentials=mock_session_instance.get_credentials.return_value,
            event_emitter=mock_session_instance.events,
        )

        # Verify presigned URL generation
        mock_signer_instance.generate_presigned_url.assert_called_once_with(
            request_dict={
                "method": "GET",
                "url": "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                "body": {},
                "headers": {"x-k8s-aws-id": "test-cluster"},
                "context": {},
            },
            region_name="us-east-1",
            expires_in=60,
            operation_name="",
        )

        # Verify the token format
        assert result == "k8s-aws-v1.aHR0cDovL2V4YW1wbGUuY29t"
