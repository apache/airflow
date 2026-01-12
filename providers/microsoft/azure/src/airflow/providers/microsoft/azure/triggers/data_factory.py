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
from collections.abc import AsyncIterator
from typing import Any

from azure.core.exceptions import ServiceRequestError

from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryAsyncHook,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ADFPipelineRunStatusSensorTrigger(BaseTrigger):
    """
    Trigger with params to run the task when the ADF Pipeline is running.

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
        resource_group_name: str,
        factory_name: str,
    ):
        super().__init__()
        self.run_id = run_id
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize ADFPipelineRunStatusSensorTrigger arguments and classpath."""
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
        """Make async connection to Azure Data Factory, polls for the pipeline run status."""
        hook = AzureDataFactoryAsyncHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        executed_after_token_refresh = False
        try:
            while True:
                try:
                    pipeline_status = await hook.get_adf_pipeline_run_status(
                        run_id=self.run_id,
                        resource_group_name=self.resource_group_name,
                        factory_name=self.factory_name,
                    )
                    executed_after_token_refresh = False
                    if pipeline_status == AzureDataFactoryPipelineRunStatus.FAILED:
                        yield TriggerEvent(
                            {"status": "error", "message": f"Pipeline run {self.run_id} has Failed."}
                        )
                        return
                    elif pipeline_status == AzureDataFactoryPipelineRunStatus.CANCELLED:
                        msg = f"Pipeline run {self.run_id} has been Cancelled."
                        yield TriggerEvent({"status": "error", "message": msg})
                        return
                    elif pipeline_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                        msg = f"Pipeline run {self.run_id} has been Succeeded."
                        yield TriggerEvent({"status": "success", "message": msg})
                        return
                    await asyncio.sleep(self.poke_interval)
                except ServiceRequestError:
                    # conn might expire during long running pipeline.
                    # If exception is caught, it tries to refresh connection once.
                    # If it still doesn't fix the issue,
                    # than the execute_after_token_refresh would still be False
                    # and an exception will be raised
                    if executed_after_token_refresh:
                        await hook.refresh_conn()
                        executed_after_token_refresh = False
                    else:
                        raise
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class AzureDataFactoryTrigger(BaseTrigger):
    """
    Trigger when the Azure data factory pipeline job finishes.

    When wait_for_termination is set to False, it triggers immediately with success status.

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
        resource_group_name: str,
        factory_name: str,
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
        """Serialize AzureDataFactoryTrigger arguments and classpath."""
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
        """Make async connection to Azure Data Factory, polls for the pipeline run status."""
        hook = AzureDataFactoryAsyncHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        try:
            pipeline_status = await hook.get_adf_pipeline_run_status(
                run_id=self.run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )
            executed_after_token_refresh = True
            if self.wait_for_termination:
                while self.end_time > time.time():
                    try:
                        pipeline_status = await hook.get_adf_pipeline_run_status(
                            run_id=self.run_id,
                            resource_group_name=self.resource_group_name,
                            factory_name=self.factory_name,
                        )
                        executed_after_token_refresh = True
                        if pipeline_status in AzureDataFactoryPipelineRunStatus.FAILURE_STATES:
                            yield TriggerEvent(
                                {
                                    "status": "error",
                                    "message": f"The pipeline run {self.run_id} has {pipeline_status}.",
                                    "run_id": self.run_id,
                                }
                            )
                            return
                        elif pipeline_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                            yield TriggerEvent(
                                {
                                    "status": "success",
                                    "message": f"The pipeline run {self.run_id} has {pipeline_status}.",
                                    "run_id": self.run_id,
                                }
                            )
                            return
                        self.log.info(
                            "Sleeping for %s. The pipeline state is %s.", self.check_interval, pipeline_status
                        )
                        await asyncio.sleep(self.check_interval)
                    except ServiceRequestError:
                        # conn might expire during long running pipeline.
                        # If exception is caught, it tries to refresh connection once.
                        # If it still doesn't fix the issue,
                        # than the execute_after_token_refresh would still be False
                        # and an exception will be raised
                        if executed_after_token_refresh:
                            await hook.refresh_conn()
                            executed_after_token_refresh = False
                        else:
                            raise

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
            self.log.exception(e)
            if self.run_id:
                try:
                    self.log.info("Cancelling pipeline run %s", self.run_id)
                    await hook.cancel_pipeline_run(
                        run_id=self.run_id,
                        resource_group_name=self.resource_group_name,
                        factory_name=self.factory_name,
                    )
                except Exception:
                    self.log.exception("Failed to cancel pipeline run %s", self.run_id)
            yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})
