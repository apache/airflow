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
from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.alloy_db import (
    AlloyDBBaseOperator,
    AlloyDBCreateClusterOperator,
    AlloyDBCreateInstanceOperator,
    AlloyDBDeleteClusterOperator,
    AlloyDBDeleteInstanceOperator,
    AlloyDBUpdateClusterOperator,
    AlloyDBUpdateInstanceOperator,
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

TEST_INSTANCE_ID = "test_instance_id"
TEST_INSTANCE: dict[str, Any] = {}

OPERATOR_MODULE_PATH = "airflow.providers.google.cloud.operators.alloy_db.{}"
ALLOY_DB_HOOK_PATH = OPERATOR_MODULE_PATH.format("AlloyDbHook")
BASE_WRITE_CLUSTER_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBWriteBaseOperator.{}")
CREATE_CLUSTER_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBCreateClusterOperator.{}")
UPDATE_CLUSTER_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.{}")
DELETE_CLUSTER_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBDeleteClusterOperator.{}")

CREATE_INSTANCE_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBCreateInstanceOperator.{}")
UPDATE_INSTANCE_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBUpdateInstanceOperator.{}")
DELETE_INSTANCE_OPERATOR_PATH = OPERATOR_MODULE_PATH.format("AlloyDBDeleteInstanceOperator.{}")


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

    @mock.patch(ALLOY_DB_HOOK_PATH)
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

    @mock.patch(BASE_WRITE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH)
    def test_get_operation_result(self, mock_hook, mock_log):
        mock_operation = mock.MagicMock()
        mock_wait_for_operation = mock_hook.return_value.wait_for_operation
        expected_result = mock_wait_for_operation.return_value

        result = self.operator.get_operation_result(mock_operation)

        assert result == expected_result
        assert not mock_log.called
        mock_wait_for_operation.assert_called_once_with(timeout=TEST_TIMEOUT, operation=mock_operation)

    @mock.patch(BASE_WRITE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH)
    def test_get_operation_result_validate_result(self, mock_hook, mock_log):
        mock_operation = mock.MagicMock()
        mock_wait_for_operation = mock_hook.return_value.wait_for_operation
        self.operator.validate_request = True

        result = self.operator.get_operation_result(mock_operation)

        assert result is None
        assert not mock_log.info.called
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
        expected_template_fields = {"cluster_id", "is_secondary", "cluster_configuration"} | set(
            AlloyDBWriteBaseOperator.template_fields
        )
        assert set(AlloyDBCreateClusterOperator.template_fields) == expected_template_fields

    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_get_cluster_not_found(self, mock_hook, mock_log):
        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_get_cluster.side_effect = NotFound("Not found")

        result = self.operator._get_cluster()

        mock_get_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_log.info.assert_has_calls(
            [
                call("Checking if the cluster %s exists already...", TEST_CLUSTER_ID),
                call("The cluster %s does not exist yet.", TEST_CLUSTER_ID),
            ]
        )
        assert result is None

    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_get_cluster_exception(self, mock_hook, mock_log):
        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_get_cluster.side_effect = Exception()

        with pytest.raises(AirflowException):
            self.operator._get_cluster()

        mock_get_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_log.info.assert_called_once_with("Checking if the cluster %s exists already...", TEST_CLUSTER_ID)

    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_get_cluster(self, mock_hook, mock_log, mock_to_dict):
        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_cluster = mock_get_cluster.return_value
        expected_result = mock_to_dict.return_value

        result = self.operator._get_cluster()

        mock_get_cluster.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_log.info.assert_has_calls(
            [
                call("Checking if the cluster %s exists already...", TEST_CLUSTER_ID),
                call("AlloyDB cluster %s already exists.", TEST_CLUSTER_ID),
            ]
        )
        mock_to_dict.assert_called_once_with(mock_cluster)
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("_get_cluster"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_cluster, mock_to_dict, mock_link
    ):
        mock_get_cluster.return_value = None
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_cluster.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Creating an AlloyDB cluster.")
        mock_get_cluster.assert_called_once()
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

        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("_get_cluster"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_is_secondary(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_cluster, mock_to_dict, mock_link
    ):
        mock_get_cluster.return_value = None
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_secondary_cluster.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Creating an AlloyDB cluster.")
        mock_get_cluster.assert_called_once()
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

        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("_get_cluster"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_validate_request(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_cluster, mock_to_dict, mock_link
    ):
        mock_get_cluster.return_value = None
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_cluster.return_value
        mock_get_operation_result.return_value = None

        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Validating a Create AlloyDB cluster request.")
        mock_get_cluster.assert_called_once()
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
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("_get_cluster"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_validate_request_is_secondary(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_cluster, mock_to_dict, mock_link
    ):
        mock_get_cluster.return_value = None
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_operation = mock_create_secondary_cluster.return_value
        mock_get_operation_result.return_value = None

        mock_context = mock.MagicMock()
        self.operator.validate_request = True
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Validating a Create AlloyDB cluster request.")
        mock_get_cluster.assert_called_once()
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
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("_get_cluster"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_already_exists(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_cluster, mock_link
    ):
        expected_result = mock_get_cluster.return_value
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster

        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        assert not mock_log.info.called
        mock_get_cluster.assert_called_once()
        assert not mock_create_cluster.called
        assert not mock_create_secondary_cluster.called
        assert not mock_get_operation_result.called
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("_get_cluster"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_CLUSTER_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_exception(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_cluster, mock_to_dict, mock_link
    ):
        mock_get_cluster.return_value = None
        mock_create_cluster = mock_hook.return_value.create_cluster
        mock_create_secondary_cluster = mock_hook.return_value.create_secondary_cluster
        mock_create_cluster.side_effect = Exception()
        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Creating an AlloyDB cluster.")
        mock_get_cluster.assert_called_once()
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
        assert not mock_to_dict.called
        assert not mock_get_operation_result.called


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
        expected_template_fields = {"cluster_id", "cluster_configuration", "allow_missing"} | set(
            AlloyDBWriteBaseOperator.template_fields
        )
        assert set(AlloyDBUpdateClusterOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.get_operation_result"))
    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBUpdateClusterOperator.log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_update_cluster = mock_hook.return_value.update_cluster
        mock_operation = mock_update_cluster.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
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
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
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
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_exception(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_update_cluster = mock_hook.return_value.update_cluster
        mock_update_cluster.side_effect = Exception

        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
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
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
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
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
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
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
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


class TestAlloyDBCreateInstanceOperator:
    def setup_method(self):
        self.operator = AlloyDBCreateInstanceOperator(
            task_id=TEST_TASK_ID,
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance_configuration=TEST_INSTANCE,
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
        assert self.operator.instance_id == TEST_INSTANCE_ID
        assert self.operator.cluster_id == TEST_CLUSTER_ID
        assert self.operator.instance_configuration == TEST_INSTANCE
        assert self.operator.is_secondary == TEST_IS_SECONDARY

    def test_template_fields(self):
        expected_template_fields = {
            "cluster_id",
            "instance_id",
            "is_secondary",
            "instance_configuration",
        } | set(AlloyDBWriteBaseOperator.template_fields)
        assert set(AlloyDBCreateInstanceOperator.template_fields) == expected_template_fields

    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_get_instance_not_found(self, mock_hook, mock_log):
        mock_get_instance = mock_hook.return_value.get_instance
        mock_get_instance.side_effect = NotFound("Not found")

        result = self.operator._get_instance()

        mock_get_instance.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            instance_id=TEST_INSTANCE_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_log.info.assert_has_calls(
            [
                call("Checking if the instance %s exists already...", TEST_INSTANCE_ID),
                call("The instance %s does not exist yet.", TEST_INSTANCE_ID),
            ]
        )
        assert result is None

    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_get_instance_exception(self, mock_hook, mock_log):
        mock_get_instance = mock_hook.return_value.get_instance
        mock_get_instance.side_effect = Exception("Test exception")

        with pytest.raises(AirflowException):
            self.operator._get_instance()

        mock_get_instance.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            instance_id=TEST_INSTANCE_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_log.info.assert_called_once_with(
            "Checking if the instance %s exists already...", TEST_INSTANCE_ID
        )

    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_get_instance(self, mock_hook, mock_log, mock_to_dict):
        mock_get_instance = mock_hook.return_value.get_instance
        mock_instance = mock_get_instance.return_value
        expected_result = mock_to_dict.return_value

        result = self.operator._get_instance()

        mock_get_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
        )
        mock_log.info.assert_has_calls(
            [
                call("Checking if the instance %s exists already...", TEST_INSTANCE_ID),
                call(
                    "AlloyDB instance %s already exists in the cluster %s.", TEST_CLUSTER_ID, TEST_INSTANCE_ID
                ),
            ]
        )
        mock_to_dict.assert_called_once_with(mock_instance)
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("_get_instance"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_instance, mock_to_dict, mock_link
    ):
        mock_get_instance.return_value = None
        mock_create_instance = mock_hook.return_value.create_instance
        mock_create_secondary_instance = mock_hook.return_value.create_secondary_instance
        mock_operation = mock_create_instance.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Creating an AlloyDB instance.")
        mock_get_instance.assert_called_once()
        mock_create_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance=TEST_INSTANCE,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_create_secondary_instance.called
        mock_to_dict.assert_called_once_with(mock_operation_result)
        mock_get_operation_result.assert_called_once_with(mock_operation)

        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("_get_instance"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_is_secondary(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_instance, mock_to_dict, mock_link
    ):
        mock_get_instance.return_value = None
        mock_create_instance = mock_hook.return_value.create_instance
        mock_create_secondary_instance = mock_hook.return_value.create_secondary_instance
        mock_operation = mock_create_secondary_instance.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Creating an AlloyDB instance.")
        mock_get_instance.assert_called_once()
        assert not mock_create_instance.called
        mock_create_secondary_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance=TEST_INSTANCE,
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

        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("_get_instance"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_validate_request(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_instance, mock_to_dict, mock_link
    ):
        mock_get_instance.return_value = None
        mock_create_instance = mock_hook.return_value.create_instance
        mock_create_secondary_instance = mock_hook.return_value.create_secondary_instance
        mock_operation = mock_create_instance.return_value
        mock_get_operation_result.return_value = None

        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Validating a Create AlloyDB instance request.")
        mock_get_instance.assert_called_once()
        mock_create_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance=TEST_INSTANCE,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_create_secondary_instance.called
        assert not mock_to_dict.called
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("_get_instance"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_validate_request_is_secondary(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_instance, mock_to_dict, mock_link
    ):
        mock_get_instance.return_value = None
        mock_create_instance = mock_hook.return_value.create_instance
        mock_create_secondary_instance = mock_hook.return_value.create_secondary_instance
        mock_operation = mock_create_secondary_instance.return_value
        mock_get_operation_result.return_value = None

        mock_context = mock.MagicMock()
        self.operator.validate_request = True
        self.operator.is_secondary = True

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Validating a Create AlloyDB instance request.")
        mock_get_instance.assert_called_once()
        mock_create_secondary_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance=TEST_INSTANCE,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_create_instance.called
        assert not mock_to_dict.called
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("_get_instance"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_already_exists(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_instance, mock_link
    ):
        expected_result = mock_get_instance.return_value
        mock_create_instance = mock_hook.return_value.create_instance
        mock_create_secondary_instance = mock_hook.return_value.create_secondary_instance

        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        assert not mock_log.info.called
        mock_get_instance.assert_called_once()
        assert not mock_create_instance.called
        assert not mock_create_secondary_instance.called
        assert not mock_get_operation_result.called
        assert result == expected_result

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("_get_instance"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(CREATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_exception(
        self, mock_hook, mock_log, mock_get_operation_result, mock_get_instance, mock_to_dict, mock_link
    ):
        mock_get_instance.return_value = None
        mock_create_instance = mock_hook.return_value.create_instance
        mock_create_secondary_instance = mock_hook.return_value.create_secondary_instance
        mock_create_instance.side_effect = Exception()
        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )

        mock_log.info.assert_called_once_with("Creating an AlloyDB instance.")
        mock_get_instance.assert_called_once()
        mock_create_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance=TEST_INSTANCE,
            location=TEST_GCP_REGION,
            project_id=TEST_GCP_PROJECT,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_create_secondary_instance.called
        assert not mock_to_dict.called
        assert not mock_get_operation_result.called


class TestAlloyDBUpdateInstanceOperator:
    def setup_method(self):
        self.operator = AlloyDBUpdateInstanceOperator(
            task_id=TEST_TASK_ID,
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            instance_configuration=TEST_INSTANCE,
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
        assert self.operator.instance_id == TEST_INSTANCE_ID
        assert self.operator.cluster_id == TEST_CLUSTER_ID
        assert self.operator.instance_configuration == TEST_INSTANCE
        assert self.operator.update_mask == TEST_UPDATE_MASK
        assert self.operator.allow_missing == TEST_ALLOW_MISSING

    def test_template_fields(self):
        expected_template_fields = {
            "cluster_id",
            "instance_id",
            "instance_configuration",
            "update_mask",
            "allow_missing",
        } | set(AlloyDBWriteBaseOperator.template_fields)
        assert set(AlloyDBUpdateInstanceOperator.template_fields) == expected_template_fields

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Instance.to_dict"))
    @mock.patch(UPDATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(UPDATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_update_instance = mock_hook.return_value.update_instance
        mock_operation = mock_update_instance.return_value
        mock_operation_result = mock_get_operation_result.return_value

        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        mock_update_instance.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            instance=TEST_INSTANCE,
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
        assert result == expected_result
        mock_log.info.assert_has_calls(
            [
                call("Updating an AlloyDB instance."),
                call("AlloyDB instance %s was successfully updated.", TEST_CLUSTER_ID),
            ]
        )

    @mock.patch(OPERATOR_MODULE_PATH.format("AlloyDBClusterLink"))
    @mock.patch(OPERATOR_MODULE_PATH.format("alloydb_v1.Cluster.to_dict"))
    @mock.patch(UPDATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(UPDATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_validate_request(
        self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link
    ):
        mock_update_instance = mock_hook.return_value.update_instance
        mock_operation = mock_update_instance.return_value
        mock_get_operation_result.return_value = None

        expected_message = "Validating an Update AlloyDB instance request."
        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_log.info.assert_called_once_with(expected_message)
        mock_update_instance.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            instance=TEST_INSTANCE,
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
    @mock.patch(UPDATE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(UPDATE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_exception(self, mock_hook, mock_log, mock_get_operation_result, mock_to_dict, mock_link):
        mock_update_instance = mock_hook.return_value.update_instance
        mock_update_instance.side_effect = Exception

        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=self.operator,
            location_id=TEST_GCP_REGION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
        )
        mock_update_instance.assert_called_once_with(
            cluster_id=TEST_CLUSTER_ID,
            instance_id=TEST_INSTANCE_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            instance=TEST_INSTANCE,
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
        mock_log.info.assert_called_once_with("Updating an AlloyDB instance.")


class TestAlloyDBDeleteInstanceOperator:
    def setup_method(self):
        self.operator = AlloyDBDeleteInstanceOperator(
            task_id=TEST_TASK_ID,
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            etag=TEST_ETAG,
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
        assert self.operator.instance_id == TEST_INSTANCE_ID
        assert self.operator.etag == TEST_ETAG

    def test_template_fields(self):
        expected_template_fields = {"cluster_id", "instance_id", "etag"} | set(
            AlloyDBWriteBaseOperator.template_fields
        )
        assert set(AlloyDBDeleteInstanceOperator.template_fields) == expected_template_fields

    @mock.patch(DELETE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(DELETE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute(self, mock_hook, mock_log, mock_get_operation_result):
        mock_delete_instance = mock_hook.return_value.delete_instance
        mock_operation = mock_delete_instance.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_delete_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            etag=TEST_ETAG,
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
                call("Deleting an AlloyDB instance."),
                call("AlloyDB instance %s was successfully removed.", TEST_INSTANCE_ID),
            ]
        )

    @mock.patch(DELETE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(DELETE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_validate_request(self, mock_hook, mock_log, mock_get_operation_result):
        mock_delete_instance = mock_hook.return_value.delete_instance
        mock_operation = mock_delete_instance.return_value
        mock_context = mock.MagicMock()
        self.operator.validate_request = True

        result = self.operator.execute(context=mock_context)

        mock_delete_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            etag=TEST_ETAG,
            request_id=TEST_REQUEST_ID,
            validate_only=True,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_operation_result.assert_called_once_with(mock_operation)
        assert result is None
        mock_log.info.assert_called_once_with("Validating a Delete AlloyDB instance request.")

    @mock.patch(DELETE_INSTANCE_OPERATOR_PATH.format("get_operation_result"))
    @mock.patch(DELETE_INSTANCE_OPERATOR_PATH.format("log"))
    @mock.patch(ALLOY_DB_HOOK_PATH, new_callable=mock.PropertyMock)
    def test_execute_exception(self, mock_hook, mock_log, mock_get_operation_result):
        mock_delete_instance = mock_hook.return_value.delete_instance
        mock_delete_instance.side_effect = Exception
        mock_context = mock.MagicMock()

        with pytest.raises(AirflowException):
            _ = self.operator.execute(context=mock_context)

        mock_delete_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_GCP_PROJECT,
            location=TEST_GCP_REGION,
            etag=TEST_ETAG,
            request_id=TEST_REQUEST_ID,
            validate_only=TEST_VALIDATE_ONLY,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        assert not mock_get_operation_result.called
        mock_log.info.assert_called_once_with("Deleting an AlloyDB instance.")
