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

from copy import deepcopy
from typing import Any
from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud import alloydb_v1

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.alloy_db import AlloyDbHook
from airflow.providers.google.common.consts import CLIENT_INFO

TEST_GCP_PROJECT = "test-project"
TEST_GCP_REGION = "global"
TEST_GCP_CONN_ID = "test_conn_id"
TEST_IMPERSONATION_CHAIN = "test_impersonation_chain"

TEST_CLUSTER_ID = "test_cluster_id"
TEST_CLUSTER: dict[str, Any] = {}
TEST_CLUSTER_NAME = f"projects/{TEST_GCP_PROJECT}/locations/{TEST_GCP_REGION}/clusters/{TEST_CLUSTER_ID}"
TEST_UPDATE_MASK = None
TEST_ALLOW_MISSING = False
TEST_ETAG = "test-etag"
TEST_FORCE = False
TEST_REQUEST_ID = "test_request_id"
TEST_VALIDATE_ONLY = False

TEST_RETRY = DEFAULT
TEST_TIMEOUT = None
TEST_METADATA = ()

HOOK_PATH = "airflow.providers.google.cloud.hooks.alloy_db.{}"


class TestAlloyDbHook:
    def setup_method(self):
        with mock.patch("airflow.hooks.base.BaseHook.get_connection"):
            self.hook = AlloyDbHook(
                gcp_conn_id=TEST_GCP_CONN_ID,
            )

    @mock.patch(HOOK_PATH.format("AlloyDbHook.get_credentials"))
    @mock.patch(HOOK_PATH.format("alloydb_v1.AlloyDBAdminClient"))
    def test_gget_alloy_db_admin_client(self, mock_client, mock_get_credentials):
        mock_credentials = mock_get_credentials.return_value
        expected_client = mock_client.return_value

        client = self.hook.get_alloy_db_admin_client()

        assert client == expected_client
        mock_get_credentials.assert_called_once()
        mock_client.assert_called_once_with(
            credentials=mock_credentials,
            client_info=CLIENT_INFO,
        )

    @pytest.mark.parametrize(
        "given_timeout, expected_timeout",
        [
            (None, None),
            (0.0, None),
            (10.0, 10),
            (10.9, 10),
        ],
    )
    @mock.patch(HOOK_PATH.format("AlloyDbHook.log"))
    def test_wait_for_operation(self, mock_log, given_timeout, expected_timeout):
        mock_operation = mock.MagicMock()
        expected_operation_result = mock_operation.result.return_value

        result = self.hook.wait_for_operation(timeout=given_timeout, operation=mock_operation)

        assert result == expected_operation_result
        mock_log.info.assert_called_once_with("Waiting for operation to complete...")
        mock_operation.result.assert_called_once_with(timeout=expected_timeout)

    @pytest.mark.parametrize(
        "given_timeout, expected_timeout",
        [
            (None, None),
            (0.0, None),
            (10.0, 10),
            (10.9, 10),
        ],
    )
    @mock.patch(HOOK_PATH.format("AlloyDbHook.log"))
    def test_wait_for_operation_exception(self, mock_log, given_timeout, expected_timeout):
        mock_operation = mock.MagicMock()
        mock_operation.result.side_effect = Exception

        with pytest.raises(AirflowException):
            self.hook.wait_for_operation(timeout=given_timeout, operation=mock_operation)

        mock_log.info.assert_called_once_with("Waiting for operation to complete...")
        mock_operation.result.assert_called_once_with(timeout=expected_timeout)
        mock_operation.exception.assert_called_once_with(timeout=expected_timeout)

    @mock.patch(HOOK_PATH.format("AlloyDbHook.get_alloy_db_admin_client"))
    def test_create_cluster(self, mock_client):
        mock_create_cluster = mock_client.return_value.create_cluster
        expected_result = mock_create_cluster.return_value
        expected_parent = f"projects/{TEST_GCP_PROJECT}/locations/{TEST_GCP_REGION}"
        mock_common_location_path = mock_client.return_value.common_location_path
        mock_common_location_path.return_value = expected_parent
        expected_request = {
            "parent": expected_parent,
            "cluster_id": TEST_CLUSTER_ID,
            "cluster": TEST_CLUSTER,
            "request_id": TEST_REQUEST_ID,
            "validate_only": TEST_VALIDATE_ONLY,
        }

        result = self.hook.create_cluster(
            cluster_id=TEST_CLUSTER_ID,
            cluster=TEST_CLUSTER,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

        assert result == expected_result
        mock_client.assert_called_once()
        mock_common_location_path.assert_called_once_with(TEST_GCP_PROJECT, TEST_GCP_REGION)
        mock_create_cluster.assert_called_once_with(
            request=expected_request,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(HOOK_PATH.format("AlloyDbHook.get_alloy_db_admin_client"))
    def test_create_secondary_cluster(self, mock_client):
        mock_create_secondary_cluster = mock_client.return_value.create_secondary_cluster
        expected_result = mock_create_secondary_cluster.return_value
        expected_parent = f"projects/{TEST_GCP_PROJECT}/locations/{TEST_GCP_REGION}"
        mock_common_location_path = mock_client.return_value.common_location_path
        mock_common_location_path.return_value = expected_parent
        expected_request = {
            "parent": expected_parent,
            "cluster_id": TEST_CLUSTER_ID,
            "cluster": TEST_CLUSTER,
            "request_id": TEST_REQUEST_ID,
            "validate_only": TEST_VALIDATE_ONLY,
        }

        result = self.hook.create_secondary_cluster(
            cluster_id=TEST_CLUSTER_ID,
            cluster=TEST_CLUSTER,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

        assert result == expected_result
        mock_client.assert_called_once()
        mock_common_location_path.assert_called_once_with(TEST_GCP_PROJECT, TEST_GCP_REGION)
        mock_create_secondary_cluster.assert_called_once_with(
            request=expected_request,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(HOOK_PATH.format("AlloyDbHook.get_alloy_db_admin_client"))
    def test_get_cluster(self, mock_client):
        mock_get_cluster = mock_client.return_value.get_cluster
        mock_cluster_path = mock_client.return_value.cluster_path
        mock_cluster_path.return_value = TEST_CLUSTER_NAME
        expected_result = mock_get_cluster.return_value

        result = self.hook.get_cluster(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

        assert result == expected_result
        mock_client.assert_called_once()
        mock_cluster_path.assert_called_once_with(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_CLUSTER_ID)
        mock_get_cluster.assert_called_once_with(
            request={"name": TEST_CLUSTER_NAME},
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.parametrize(
        "given_cluster, expected_cluster",
        [
            (TEST_CLUSTER, {**deepcopy(TEST_CLUSTER), **{"name": TEST_CLUSTER_NAME}}),
            (alloydb_v1.Cluster(), {"name": TEST_CLUSTER_NAME}),
            ({}, {"name": TEST_CLUSTER_NAME}),
        ],
    )
    @mock.patch(HOOK_PATH.format("deepcopy"))
    @mock.patch(HOOK_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(HOOK_PATH.format("AlloyDbHook.get_alloy_db_admin_client"))
    def test_update_cluster(self, mock_client, mock_to_dict, mock_deepcopy, given_cluster, expected_cluster):
        mock_update_cluster = mock_client.return_value.update_cluster
        expected_result = mock_update_cluster.return_value
        mock_deepcopy.return_value = expected_cluster
        mock_to_dict.return_value = expected_cluster
        mock_cluster_path = mock_client.return_value.cluster_path
        mock_cluster_path.return_value = expected_cluster

        expected_request = {
            "update_mask": TEST_UPDATE_MASK,
            "cluster": expected_cluster,
            "request_id": TEST_REQUEST_ID,
            "validate_only": TEST_VALIDATE_ONLY,
            "allow_missing": TEST_ALLOW_MISSING,
        }

        result = self.hook.update_cluster(
            cluster_id=TEST_CLUSTER_ID,
            cluster=given_cluster,
            location=TEST_GCP_REGION,
            update_mask=TEST_UPDATE_MASK,
            project_id=TEST_GCP_PROJECT,
            allow_missing=TEST_ALLOW_MISSING,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

        assert result == expected_result
        mock_cluster_path.assert_called_once_with(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_CLUSTER_ID)
        if isinstance(given_cluster, dict):
            mock_deepcopy.assert_called_once_with(given_cluster)
            assert not mock_to_dict.called
        else:
            assert not mock_deepcopy.called
            mock_to_dict.assert_called_once_with(given_cluster)
        mock_client.assert_called_once()
        mock_update_cluster.assert_called_once_with(
            request=expected_request,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(HOOK_PATH.format("AlloyDbHook.get_alloy_db_admin_client"))
    def test_delete_cluster(self, mock_client):
        mock_delete_cluster = mock_client.return_value.delete_cluster
        expected_result = mock_delete_cluster.return_value
        mock_cluster_path = mock_client.return_value.cluster_path
        mock_cluster_path.return_value = TEST_CLUSTER_NAME
        expected_request = {
            "name": TEST_CLUSTER_NAME,
            "request_id": TEST_REQUEST_ID,
            "etag": TEST_ETAG,
            "validate_only": TEST_VALIDATE_ONLY,
            "force": TEST_FORCE,
        }

        result = self.hook.delete_cluster(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            etag=TEST_ETAG,
            validate_only=TEST_VALIDATE_ONLY,
            force=TEST_FORCE,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

        assert result == expected_result
        mock_client.assert_called_once()
        mock_cluster_path.assert_called_once_with(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_CLUSTER_ID)
        mock_delete_cluster.assert_called_once_with(
            request=expected_request,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
