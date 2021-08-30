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

from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.sensors.base import BaseSensorOperator


class AzureDataFactoryPipelineRunStatusSensor(BaseSensorOperator):
    """
    Checks the status of a pipeline run.

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :type azure_data_factory_conn_id: str
    :param run_id: The pipeline run identifier.
    :type run_id: str
    :param resource_group_name: The resource group name.
    :type resource_group_name: str
    :param factory_name: The data factory name.
    :type factory_name: str
    """

    template_fields = ("azure_data_factory_conn_id", "resource_group_name", "factory_name", "run_id")

    def __init__(
        self,
        *,
        azure_data_factory_conn_id: str,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.run_id = run_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name

    def poke(self, context: Dict) -> bool:
        self.log.info(f"Checking the status for pipeline run {self.run_id}.")
        self.hook = AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        pipeline_run = self.hook.get_pipeline_run(
            run_id=self.run_id,
            factory_name=self.factory_name,
            resource_group_name=self.resource_group_name,
        )
        pipeline_run_status = pipeline_run.status
        self.log.info(f"Current status for pipeline run {self.run_id}: {pipeline_run_status}.")

        if pipeline_run_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
            self.log.info(f"The pipeline run {self.run_id} has succeeded.")
            return True
        elif pipeline_run_status in {
            AzureDataFactoryPipelineRunStatus.FAILED,
            AzureDataFactoryPipelineRunStatus.CANCELLED,
        }:
            raise AirflowException(
                f"Pipeline run {self.run_id} is in a negative terminal status: {pipeline_run_status}"
            )

        return False
