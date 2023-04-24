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

import asyncio
import time
from typing import Any, AsyncIterator

from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryAsyncHook,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ADFPipelineRunStatusSensorTrigger(BaseTrigger):
    """
    ADFPipelineRunStatusSensorTrigger is fired as deferred class with params to run the
    task in trigger worker, when ADF Pipeline is running

    :param run_id: The pipeline run identifier.
    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param poke_interval:  polling period in seconds to check for the status
    :param resource_group_name: The resource group name.
    :param factory_name: The data factory name.
    """

    def __init__(
        self,
        run_id: str,
        azure_data_factory_conn_id: str,
        poke_interval: float,
        resource_group_name: str | None = None,
        factory_name: str | None = None,
    ):
        super().__init__()
        self.run_id = run_id
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes ADFPipelineRunStatusSensorTrigger arguments and classpath."""
        return (
            "airflow.providers.microsoft.azure.triggers.data_factory.ADFPipelineRunStatusSensorTrigger",
            {
                "run_id": self.run_id,
                "azure_data_factory_conn_id": self.azure_data_factory_conn_id,
                "resource_group_name": self.resource_group_name,
                "factory_name": self.factory_name,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Azure Data Factory, polls for the pipeline run status"""
        hook = AzureDataFactoryAsyncHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        try:
            while True:
                pipeline_status = await hook.get_adf_pipeline_run_status(
                    run_id=self.run_id,
                    resource_group_name=self.resource_group_name,
                    factory_name=self.factory_name,
                )
                if pipeline_status == AzureDataFactoryPipelineRunStatus.FAILED:
                    yield TriggerEvent(
                        {"status": "error", "message": f"Pipeline run {self.run_id} has Failed."}
                    )
                elif pipeline_status == AzureDataFactoryPipelineRunStatus.CANCELLED:
                    msg = f"Pipeline run {self.run_id} has been Cancelled."
                    yield TriggerEvent({"status": "error", "message": msg})
                elif pipeline_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                    msg = f"Pipeline run {self.run_id} has been Succeeded."
                    yield TriggerEvent({"status": "success", "message": msg})
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class AzureDataFactoryTrigger(BaseTrigger):
    """
    AzureDataFactoryTrigger is triggered when Azure data factory pipeline job succeeded or failed.
    When wait_for_termination is set to False it triggered immediately with success status

    :param run_id: Run id of a Azure data pipeline run job.
    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param end_time: Time in seconds when triggers will timeout.
    :param resource_group_name: The resource group name.
    :param factory_name: The data factory name.
    :param wait_for_termination: Flag to wait on a pipeline run's termination.
    :param check_interval: Time in seconds to check on a pipeline run's status.
    """

    def __init__(
        self,
        run_id: str,
        azure_data_factory_conn_id: str,
        end_time: float,
        resource_group_name: str | None = None,
        factory_name: str | None = None,
        wait_for_termination: bool = True,
        check_interval: int = 60,
    ):
        super().__init__()
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.check_interval = check_interval
        self.run_id = run_id
        self.wait_for_termination = wait_for_termination
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.end_time = end_time

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes AzureDataFactoryTrigger arguments and classpath."""
        return (
            "airflow.providers.microsoft.azure.triggers.data_factory.AzureDataFactoryTrigger",
            {
                "azure_data_factory_conn_id": self.azure_data_factory_conn_id,
                "check_interval": self.check_interval,
                "run_id": self.run_id,
                "wait_for_termination": self.wait_for_termination,
                "resource_group_name": self.resource_group_name,
                "factory_name": self.factory_name,
                "end_time": self.end_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Azure Data Factory, polls for the pipeline run status"""
        hook = AzureDataFactoryAsyncHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        try:
            pipeline_status = await hook.get_adf_pipeline_run_status(
                run_id=self.run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )
            if self.wait_for_termination:
                while self.end_time > time.time():
                    pipeline_status = await hook.get_adf_pipeline_run_status(
                        run_id=self.run_id,
                        resource_group_name=self.resource_group_name,
                        factory_name=self.factory_name,
                    )
                    if pipeline_status in AzureDataFactoryPipelineRunStatus.FAILURE_STATES:
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "message": f"The pipeline run {self.run_id} has {pipeline_status}.",
                                "run_id": self.run_id,
                            }
                        )
                    elif pipeline_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message": f"The pipeline run {self.run_id} has {pipeline_status}.",
                                "run_id": self.run_id,
                            }
                        )
                    self.log.info(
                        "Sleeping for %s. The pipeline state is %s.", self.check_interval, pipeline_status
                    )
                    await asyncio.sleep(self.check_interval)

                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Timeout: The pipeline run {self.run_id} has {pipeline_status}.",
                        "run_id": self.run_id,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"The pipeline run {self.run_id} has {pipeline_status} status.",
                        "run_id": self.run_id,
                    }
                )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})
