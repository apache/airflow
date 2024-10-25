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

import pytest

from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.sensors.financial_services import FinancialServicesOperationSensor

TEST_LOCATION_RESOURCE_URI = "projects/test-project/locations/us-central1"
TEST_OPERATION_RESOURCE_URI = f"{TEST_LOCATION_RESOURCE_URI}/operations/test-operation"
TEST_OPERATION = {"name": "test-operation", "metadata": {}, "done": False}
TEST_OPERATION_DONE = {"name": "test-operation", "metadata": {}, "done": True, "response": {}}
TEST_OPERATION_ERROR = {"name": "test-operation", "metadata": {}, "done": True, "error": {}}


class TestFinancialServicesOperationSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.financial_services.FinancialServicesHook")
    def test_poke(self, mock_hook):
        mock_hook.return_value.get_operation.return_value = TEST_OPERATION

        op = FinancialServicesOperationSensor(
            task_id="test_operation_sensor_task",
            discovery_doc={},
            operation_resource_uri=TEST_OPERATION_RESOURCE_URI,
        )
        response = op.poke(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(discovery_doc={}, gcp_conn_id="google_cloud_default")
        mock_hook.return_value.get_operation.assert_called_once_with(
            operation_resource_uri=TEST_OPERATION_RESOURCE_URI,
        )
        assert response.is_done == TEST_OPERATION["done"]

    @mock.patch("airflow.providers.google.cloud.sensors.financial_services.FinancialServicesHook")
    def test_poke_done(self, mock_hook):
        mock_hook.return_value.get_operation.return_value = TEST_OPERATION_DONE

        op = FinancialServicesOperationSensor(
            task_id="test_operation_sensor_task",
            discovery_doc={},
            operation_resource_uri=TEST_OPERATION_RESOURCE_URI,
        )
        response = op.poke(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(discovery_doc={}, gcp_conn_id="google_cloud_default")
        mock_hook.return_value.get_operation.assert_called_once_with(
            operation_resource_uri=TEST_OPERATION_RESOURCE_URI,
        )
        assert response.is_done == TEST_OPERATION_DONE["done"]

    @mock.patch("airflow.providers.google.cloud.sensors.financial_services.FinancialServicesHook")
    def test_poke_error(self, mock_hook):
        mock_hook.return_value.get_operation.return_value = TEST_OPERATION_ERROR

        op = FinancialServicesOperationSensor(
            task_id="test_operation_sensor_task",
            discovery_doc={},
            operation_resource_uri=TEST_OPERATION_RESOURCE_URI,
        )
        with pytest.raises(AirflowFailException):
            op.poke(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(discovery_doc={}, gcp_conn_id="google_cloud_default")
        mock_hook.return_value.get_operation.assert_called_once_with(
            operation_resource_uri=TEST_OPERATION_RESOURCE_URI,
        )
