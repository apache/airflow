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

PROJECT_ID = "test-project"
REGION = "us-central1"
KMS_KEY_RING = "test-key-ring"
KMS_KEY = "test-key"
INSTANCE_ID = "test-instance"


class TestFinancialServicesCreateInstanceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.financial_services.FinancialServicesHook")
    def test_execute(self, mock_hook):
        op = FinancialServicesCreateInstanceOperator(
            task_id="test_create_instance_task",
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
            kms_key_ring_id=KMS_KEY_RING,
            kms_key_id=KMS_KEY,
        )
        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
            kms_key_ring_id=KMS_KEY_RING,
            kms_key_id=KMS_KEY,
        )


class TestFinancialServicesDeleteInstanceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.financial_services.FinancialServicesHook")
    def test_execute(self, mock_hook):
        op = FinancialServicesDeleteInstanceOperator(
            task_id="test_delete_instance_task",
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
        )
        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
        )


class TestFinancialServicesGetInstanceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.financial_services.FinancialServicesHook")
    def test_execute(self, mock_hook):
        op = FinancialServicesDeleteInstanceOperator(
            task_id="test_get_instance_task",
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
        )
        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
        )
