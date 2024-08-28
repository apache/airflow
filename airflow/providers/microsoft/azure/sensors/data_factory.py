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

from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.triggers.data_factory import ADFPipelineRunStatusSensorTrigger
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureDataFactoryPipelineRunStatusSensor(BaseSensorOperator):
    """
    Checks the status of a pipeline run.

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param run_id: The pipeline run identifier.
    :param resource_group_name: The resource group name.
    :param factory_name: The data factory name.
    :param deferrable: Run sensor in the deferrable mode.
    """

    template_fields: Sequence[str] = (
        "azure_data_factory_conn_id",
        "resource_group_name",
        "factory_name",
        "run_id",
    )

    ui_color = "#50e6ff"

    def __init__(
        self,
        *,
        run_id: str,
        azure_data_factory_conn_id: str = AzureDataFactoryHook.default_conn_name,
        resource_group_name: str,
        factory_name: str,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.run_id = run_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name

        self.deferrable = deferrable

    @cached_property
    def hook(self):
        """Create and return an AzureDataFactoryHook (cached)."""
        return AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)

    def poke(self, context: Context) -> bool:
        pipeline_run_status = self.hook.get_pipeline_run_status(
            run_id=self.run_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )

        if pipeline_run_status == AzureDataFactoryPipelineRunStatus.FAILED:
            message = f"Pipeline run {self.run_id} has failed."
            raise AzureDataFactoryPipelineRunException(message)

        if pipeline_run_status == AzureDataFactoryPipelineRunStatus.CANCELLED:
            message = f"Pipeline run {self.run_id} has been cancelled."
            raise AzureDataFactoryPipelineRunException(message)

        return pipeline_run_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED

    def execute(self, context: Context) -> None:
        """
        Poll for state of the job run.

        In deferrable mode, the polling is deferred to the triggerer. Otherwise
        the sensor waits synchronously.
        """
        if not self.deferrable:
            super().execute(context=context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=ADFPipelineRunStatusSensorTrigger(
                        run_id=self.run_id,
                        azure_data_factory_conn_id=self.azure_data_factory_conn_id,
                        resource_group_name=self.resource_group_name,
                        factory_name=self.factory_name,
                        poke_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        return None
