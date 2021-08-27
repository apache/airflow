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

import json
import unittest
# from azure.mgmt.datafactory import DataFactoryManagementClient
from datetime import datetime
from parameterized import parameterized
from pytest import fixture
from unittest import mock

from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.hooks.azure_data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.sensors.azure_data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.utils.session import create_session

@fixture
def data_factory_client():
    client = AzureDataFactoryHook(conn_id="azure_data_factory_test")
    client._conn = mock.MagicMock(
        spec=[
            "factories",
            "linked_services",
            "datasets",
            "pipelines",
            "pipeline_runs",
            "triggers",
            "trigger_runs",
        ]
    )

    return client

class TestPipelineRunStatusSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG("test", start_date=datetime(2021, 8, 16), schedule_interval=None, catchup=False)
        self.config = {
            "conn_id": "azure_data_factory_test",
            "run_id": "run_id",
            "timeout": 100,
            "poke_interval": 15,
        }
        self.conn = Connection(
            conn_id="azure_data_factory_test",
            conn_type="azure_data_factory",
            login="client_id",
            password="client_secret",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": "default-resource-group-name",
                    "extra__azure_data_factory__factory_name": "default-factory-name",
                },
            )
        )

        with create_session() as session:
            session.query(Connection).filter(Connection.conn_id == self.conn.conn_id).delete()
            session.add(self.conn)
            session.commit()

    @fixture(autouse=True)
    def hook(self, data_factory_client):
        self.hook = data_factory_client

    def test_init(self):
        sensor = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor", dag=self.dag, **self.config
        )
        assert sensor.conn_id == self.config["conn_id"]
        assert sensor.run_id == self.config["run_id"]
        assert sensor.resource_group_name == None
        assert sensor.factory_name == None
        assert sensor.expected_statuses == {AzureDataFactoryPipelineRunStatus.SUCCEEDED}
        assert sensor.timeout == self.config["timeout"]
        assert sensor.poke_interval == self.config["poke_interval"]


    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.azure_data_factory.AzureDataFactoryHook",
        autospec=True,
    )
    @mock.patch.object(AzureDataFactoryHook, 'get_pipeline_run')
    def test_poke(self, mock_hook, mock_get_pipeline_run):
        mock_run = mock_get_pipeline_run.return_value

        sensor = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor_poke", dag=self.dag, **self.config
        )
        result = sensor.poke({})

        mock_run.get_pipeline_run.assert_called_once_with(
            run_id="run_id",
            factory_name="default_factory_name",
            resource_group_name="default_resource_group_name",
        )

        assert result == False

    # @mock.patch(
    #     "airflow.providers.microsoft.azure.hooks.azure_data_factory.AzureDataFactoryHook",
    #     autospec=True,
    # )
    # def test_poke_(self, mock_hook):
    #     mock_run = mock_hook.get_pipeline_run.return_value
    #     mock_run.return_value = DataFactoryManagementClient
    #
    #     sensor = AzureDataFactoryPipelineRunStatusSensor(
    #         task_id="pipeline_run_sensor_poke", dag=self.dag, **self.config
    #     )
    #     result = sensor.poke({})
    #
    #     mock_run.get_pipeline_run.assert_called_once_with(
    #         run_id="run_id",
    #         factory_name="default_factory_name",
    #         resource_group_name="default_resource_group_name",
    #     )
    #
    #     assert result == False
