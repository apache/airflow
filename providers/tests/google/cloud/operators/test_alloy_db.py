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

from typing import Any
from unittest import mock
from unittest.mock import call

import pytest
from google.api_core.exceptions import AlreadyExists, InvalidArgument
from google.api_core.gapic_v1.method import DEFAULT

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.alloy_db import (
    AlloyDBBaseOperator,
    AlloyDBCreateClusterOperator,
    AlloyDBDeleteClusterOperator,
    AlloyDBUpdateClusterOperator,
    AlloyDBWriteBaseOperator,
)

TEST_TASK_ID = "test-task-id"
TEST_GCP_PROJECT = "test-project"
TEST_GCP_REGION = "global"
TEST_GCP_CONN_ID = "test_conn_id"
TEST_IMPERSONATION_CHAIN = "test_impersonation_chain"
TEST_RETRY = DEFAULT
TEST_TIMEOUT = None
TEST_METADATA = ()

TEST_REQUEST_ID = "test_request_id"
TEST_VALIDATE_ONLY = False

TEST_CLUSTER_ID = "test_cluster_id"
TEST_CLUSTER_NAME = f"projects/{TEST_GCP_PROJECT}/locations/{TEST_GCP_REGION}/clusters/{TEST_CLUSTER_ID}"
TEST_CLUSTER: dict[str, Any] = {}
TEST_IS_SECONDARY = False
TEST_UPDATE_MASK = None
TEST_ALLOW_MISSING = False
TEST_ETAG = "test-etag"
TEST_FORCE = False

OPERATOR_MODULE_PATH = "airflow.providers.google.cloud.operators.alloy_db.{}"


class TestAlloyDBBaseOperator:
    def setup_method(self):
        self.operator = AlloyDBBaseOperator(
            task_id=TEST_TASK_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            gcp_conn_id=TEST_GCP_CONN_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_init(self):
        assert self.operator.project_id == TEST_GCP_PROJECT
        assert self.operator.location == TEST_GCP_REGION
        assert self.operator.gcp_conn_id == TEST_GCP_CONN_ID
        assert self.operator.impersonation_chain == TEST_IMPERSONATION_CHAIN
        assert self.operator.retry == TEST_RETRY
        assert self.operator.timeout == TEST_TIMEOUT
        assert self.operator.metadata == TEST_METADATA

    def test_template_fields(self):
        expected_template_fields = {"project_id", "location", "gcp_conn_id"}
        assert set(AlloyDBBaseOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"))
    def test_hook(self, mock_hook):
        expected_hook = mock_hook.return_value

        hook_1 = self.operator.hook
        hook_2 = self.operator.hook

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID, impersonation_chain=TEST_IMPERSONATION_CHAIN
        )
        assert hook_1 == expected_hook
        assert hook_2 == expected_hook


class TestAlloyDBWriteBaseOperator:
    def setup_method(self):
        self.operator = AlloyDBWriteBaseOperator(
            task_id=TEST_TASK_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            gcp_conn_id=TEST_GCP_CONN_ID,
            request_id=TEST_REQUEST_ID,
            validate_request=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_init(self):
        assert self.operator.request_id == TEST_REQUEST_ID
        assert self.operator.validate_request == TEST_VALIDATE_ONLY

    def test_template_fields(self):
        expected_template_fields = {"request_id", "validate_request"} | set(
            AlloyDBBaseOperator.template_fields
        )
        assert set(AlloyDBWriteBaseOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBWriteBaseOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"))
    def test_get_operation_result(self, mock_hook, mock_log):
        mock_operation = mock.MagicMock()
        mock_wait_for_operation = mock_hook.return_value.wait_for_operation
        expected_result = mock_wait_for_operation.return_value

        result = self.operator.get_operation_result(mock_operation)

        assert result == expected_result
        assert not mock_log.called
        mock_wait_for_operation.assert_called_once_with(timeout=TEST_TIMEOUT, operation=mock_operation)

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBWriteBaseOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"))
    def test_get_operation_result_validate_result(self, mock_hook, mock_log):
        mock_operation = mock.MagicMock()
        mock_wait_for_operation = mock_hook.return_value.wait_for_operation
        self.operator.validate_request = True

        result = self.operator.get_operation_result(mock_operation)

        assert result is None
        mock_log.info.assert_called_once_with("The request validation has been passed successfully!")
        assert not mock_wait_for_operation.called


class TestAlloyDBCreateClusterOperator:
    def setup_method(self):
        self.operator = AlloyDBCreateClusterOperator(
            task_id=TEST_TASK_ID,
            cluster_id=TEST_CLUSTER_ID,
            cluster_configuration=TEST_CLUSTER,
            is_secondary=TEST_IS_SECONDARY,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            gcp_conn_id=TEST_GCP_CONN_ID,
            request_id=TEST_REQUEST_ID,
            validate_request=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_init(self):
        assert self.operator.cluster_id == TEST_CLUSTER_ID
        assert self.operator.cluster_configuration == TEST_CLUSTER
        assert self.operator.is_secondary == TEST_IS_SECONDARY

    def test_template_fields(self):
        expected_template_fields = {"cluster_id", "is_secondary"} | set(
            AlloyDBWriteBaseOperator.template_fields
        )
        assert set(AlloyDBCreateClusterOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_cluster.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_message = "Creating an AlloyDB cluster."
        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with(expected_message)
        mock_create_cluster.assert_called_once_with(
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
        assert not mock_create_secondary_cluster.called
        mock_to_dict.assert_called_once_with(mock_operation_result)
        mock_get_operation_result.assert_called_once_with(mock_operation)
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_is_secondary(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_secondary_cluster.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_message = "Creating an AlloyDB cluster."
        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with(expected_message)
        assert not mock_create_cluster.called
        mock_create_secondary_cluster.assert_called_once_with(
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
        mock_to_dict.assert_called_once_with(mock_operation_result)
        mock_get_operation_result.assert_called_once_with(mock_operation)
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_validate_request(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_cluster.return_value
        mock_get_operation_result.return_value = None

        expected_message = "Validating a Create AlloyDB cluster request."
        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with(expected_message)
        mock_create_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            cluster=TEST_CLUSTER,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_create_secondary_cluster.called
        assert not mock_to_dict.called
        assert not mock_link.persist.called
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_validate_request_is_secondary(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_secondary_cluster.return_value
        mock_get_operation_result.return_value = None

        expected_message = "Validating a Create AlloyDB cluster request."
        mock_context = mock.MagicMock()
        self.operator.validate_request = True
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with(expected_message)
        mock_create_secondary_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            cluster=TEST_CLUSTER,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_create_cluster.called
        assert not mock_to_dict.called
        assert not mock_link.persist.called
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_already_exists(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_cluster.side_effect = AlreadyExists("test-message")

        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_get_cluster_result = mock_get_cluster.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_has_calls(
            [
                call("Creating an AlloyDB cluster."),
                call("AlloyDB cluster %s already exists.", TEST_CLUSTER_ID),
            ]
        )
        mock_create_cluster.assert_called_once_with(
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
        assert not mock_create_secondary_cluster.called
        mock_get_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_to_dict.assert_called_once_with(mock_get_cluster_result)
        assert not mock_get_operation_result.called
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_invalid_argument(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_create_cluster = mock_hook.return_value.create_cluster
        expected_error_message = "cannot create more than one secondary cluster per primary cluster"
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_create_secondary_cluster.side_effect = InvalidArgument(message=expected_error_message)

        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_get_cluster_result = mock_get_cluster.return_value

        expected_result = mock_to_dict.return_value
        expected_result.get.return_value = TEST_CLUSTER_NAME
        mock_context = mock.MagicMock()
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_has_calls(
            [
                call("Creating an AlloyDB cluster."),
                call("AlloyDB cluster %s already exists.", TEST_CLUSTER_ID),
            ]
        )
        mock_create_secondary_cluster.assert_called_once_with(
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
        assert not mock_create_cluster.called
        mock_get_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_to_dict.assert_called_once_with(mock_get_cluster_result)
        assert not mock_get_operation_result.called
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_invalid_argument_exception(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_create_secondary_cluster.side_effect = InvalidArgument(message="Test error")
        mock_get_cluster = mock_hook.return_value.get_cluster
        expected_result = mock_to_dict.return_value
        expected_result.get.return_value = TEST_CLUSTER_NAME
        mock_context = mock.MagicMock()
        self.operator.is_secondary = True

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with("Creating an AlloyDB cluster.")
        mock_create_secondary_cluster.assert_called_once_with(
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
        assert not mock_create_cluster.called
        assert not mock_get_cluster.called
        assert not mock_to_dict.called
        assert not mock_get_operation_result.called
        assert not mock_link.persist.called

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_exception(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_create_cluster.side_effect = Exception()
        mock_get_cluster = mock_hook.return_value.get_cluster
        expected_result = mock_to_dict.return_value
        expected_result.get.return_value = TEST_CLUSTER_NAME
        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with("Creating an AlloyDB cluster.")
        mock_create_cluster.assert_called_once_with(
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
        assert not mock_create_secondary_cluster.called
        assert not mock_get_cluster.called
        assert not mock_to_dict.called
        assert not mock_get_operation_result.called
        assert not mock_link.persist.called


class TestAlloyDBUpdateClusterOperator:
    def setup_method(self):
        self.operator = AlloyDBUpdateClusterOperator(
            task_id=TEST_TASK_ID,
            cluster_id=TEST_CLUSTER_ID,
            cluster_configuration=TEST_CLUSTER,
            update_mask=TEST_UPDATE_MASK,
            allow_missing=TEST_ALLOW_MISSING,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            gcp_conn_id=TEST_GCP_CONN_ID,
            request_id=TEST_REQUEST_ID,
            validate_request=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_init(self):
        assert self.operator.cluster_id == TEST_CLUSTER_ID
        assert self.operator.cluster_configuration == TEST_CLUSTER
        assert self.operator.update_mask == TEST_UPDATE_MASK
        assert self.operator.allow_missing == TEST_ALLOW_MISSING

    def test_template_fields(self):
        expected_template_fields = {"cluster_id", "allow_missing"} | set(
            AlloyDBWriteBaseOperator.template_fields
        )
        assert set(AlloyDBUpdateClusterOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_update_cluster = mock_hook.return_value.update_cluster
        mock_operation = mock_update_cluster.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_update_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            cluster=TEST_CLUSTER,
            update_mask=TEST_UPDATE_MASK,
            allow_missing=TEST_ALLOW_MISSING,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_operation_result.assert_called_once_with(mock_operation)
        mock_to_dict.assert_called_once_with(mock_operation_result)
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        assert result == expected_result
        mock_log.info.assert_has_calls(
            [
                call("Updating an AlloyDB cluster."),
                call("AlloyDB cluster %s was successfully updated.", TEST_CLUSTER_ID),
            ]
        )

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_validate_request(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_update_cluster = mock_hook.return_value.update_cluster
        mock_operation = mock_update_cluster.return_value
        mock_get_operation_result.return_value = None

        expected_message = "Validating an Update AlloyDB cluster request."
        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with(expected_message)
        mock_update_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            cluster=TEST_CLUSTER,
            update_mask=TEST_UPDATE_MASK,
            allow_missing=TEST_ALLOW_MISSING,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert not mock_to_dict.called
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_exception(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_update_cluster = mock_hook.return_value.update_cluster
        mock_update_cluster.side_effect = Exception

        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_update_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            cluster=TEST_CLUSTER,
            update_mask=TEST_UPDATE_MASK,
            allow_missing=TEST_ALLOW_MISSING,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_get_operation_result.called
        assert not mock_to_dict.called
        assert not mock_link.persist.called
        mock_log.info.assert_called_once_with("Updating an AlloyDB cluster.")


class TestAlloyDBDeleteClusterOperator:
    def setup_method(self):
        self.operator = AlloyDBDeleteClusterOperator(
            task_id=TEST_TASK_ID,
            cluster_id=TEST_CLUSTER_ID,
            etag=TEST_ETAG,
            force=TEST_FORCE,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            gcp_conn_id=TEST_GCP_CONN_ID,
            request_id=TEST_REQUEST_ID,
            validate_request=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_init(self):
        assert self.operator.cluster_id == TEST_CLUSTER_ID
        assert self.operator.etag == TEST_ETAG
        assert self.operator.force == TEST_FORCE

    def test_template_fields(self):
        expected_template_fields = {"cluster_id", "etag", "force"} | set(
            AlloyDBWriteBaseOperator.template_fields
        )
        assert set(AlloyDBDeleteClusterOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute(self, mock_hook, mock_log, mock_get_operation_result):
        mock_delete_cluster = mock_hook.return_value.delete_cluster
        mock_operation = mock_delete_cluster.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_delete_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            etag=TEST_ETAG,
            force=TEST_FORCE,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None
        mock_log.info.assert_has_calls(
            [
                call("Deleting an AlloyDB cluster."),
                call("AlloyDB cluster %s was successfully removed.", TEST_CLUSTER_ID),
            ]
        )

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_validate_request(self, mock_hook, mock_log, mock_get_operation_result):
        mock_delete_cluster = mock_hook.return_value.delete_cluster
        mock_operation = mock_delete_cluster.return_value
        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_delete_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            etag=TEST_ETAG,
            force=TEST_FORCE,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None
        mock_log.info.assert_called_once_with("Validating a Delete AlloyDB cluster request.")

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.log"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDbHook"), new_callable=mock.PropertyMock)
    def test_execute_exception(self, mock_hook, mock_log, mock_get_operation_result):
        mock_delete_cluster = mock_hook.return_value.delete_cluster
        mock_delete_cluster.side_effect = Exception
        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            _ = self.operator.execute(context=mock_context)

        mock_delete_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            etag=TEST_ETAG,
            force=TEST_FORCE,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_get_operation_result.called
        mock_log.info.assert_called_once_with("Deleting an AlloyDB cluster.")
