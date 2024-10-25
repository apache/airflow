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

from airflow.providers.google.cloud.operators.financial_services import (
    FinancialServicesCreateInstanceOperator,
    FinancialServicesDeleteInstanceOperator,
)

TEST_KMS_KEY_URI = "projects/test-project/locations/us-central1/keyRings/my-kr/cryptoKeys/my-kms-key"
TEST_LOCATION_RESOURCE_URI = "projects/test-project/locations/us-central1"
TEST_INSTANCE_ID = "test-instance"
TEST_INSTANCE_RESOURCE_URI = f"{TEST_LOCATION_RESOURCE_URI}/instances/{TEST_INSTANCE_ID}"
TEST_OPERATION = {"name": "test-operation", "metadata": {}, "done": False}
TEST_INSTANCE = {
    "name": "test-instance",
    "createTime": "2014-10-02T15:01:23Z",
    "updateTime": "2014-10-02T15:01:23Z",
    "labels": {},
    "state": "ACTIVE",
    "kmsKey": TEST_KMS_KEY_URI,
}


class TestFinancialServicesCreateInstanceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.financial_services.FinancialServicesHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.create_instance.return_value = TEST_OPERATION

        op = FinancialServicesCreateInstanceOperator(
            task_id="test_create_instance_task",
            discovery_doc={},
            instance_id=TEST_INSTANCE_ID,
            kms_key_uri=TEST_KMS_KEY_URI,
            location_resource_uri=TEST_LOCATION_RESOURCE_URI,
        )
        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(discovery_doc={}, gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            instance_id=TEST_INSTANCE_ID,
            kms_key_uri=TEST_KMS_KEY_URI,
            location_resource_uri=TEST_LOCATION_RESOURCE_URI,
        )


class TestFinancialServicesDeleteInstanceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.financial_services.FinancialServicesHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.delete_instance.return_value = TEST_OPERATION

        op = FinancialServicesDeleteInstanceOperator(
            task_id="test_delete_instance_task",
            discovery_doc={},
            instance_resource_uri=TEST_INSTANCE_RESOURCE_URI,
        )
        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(discovery_doc={}, gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            instance_resource_uri=TEST_INSTANCE_RESOURCE_URI
        )


class TestFinancialServicesGetInstanceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.financial_services.FinancialServicesHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = TEST_INSTANCE

        op = FinancialServicesDeleteInstanceOperator(
            task_id="test_get_instance_task",
            discovery_doc={},
            instance_resource_uri=TEST_INSTANCE_RESOURCE_URI,
        )
        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(discovery_doc={}, gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            instance_resource_uri=TEST_INSTANCE_RESOURCE_URI
        )
