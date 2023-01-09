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

import json
from time import sleep
from typing import TYPE_CHECKING

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from mypy_boto3_appflow.client import AppflowClient
    from mypy_boto3_appflow.type_defs import TaskTypeDef


class AppflowHook(AwsBaseHook):
    """
    Interact with Amazon Appflow, using the boto3 library.
    Hook attribute ``conn`` has all methods that listed in documentation.

    .. seealso::
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/appflow.html
        - https://docs.aws.amazon.com/appflow/1.0/APIReference/Welcome.html

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
        are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "appflow"
        super().__init__(*args, **kwargs)

    @cached_property
    def conn(self) -> AppflowClient:
        """Get the underlying boto3 Appflow client (cached)"""
        return super().conn

    def run_flow(self, flow_name: str, poll_interval: int = 20, wait_for_completion: bool = True) -> str:
        """
        Execute an AppFlow run.

        :param flow_name: The flow name
        :param poll_interval: Time (seconds) to wait between two consecutive calls to check the run status
        :param wait_for_completion: whether to wait for the run to end to return
        :return: The run execution ID
        """
        response_start = self.conn.start_flow(flowName=flow_name)
        execution_id = response_start["executionId"]
        self.log.info("executionId: %s", execution_id)

        if wait_for_completion:
            last_execs: dict = {}
            self.log.info("Waiting for flow run to complete...")
            while (
                execution_id not in last_execs or last_execs[execution_id]["executionStatus"] == "InProgress"
            ):
                sleep(poll_interval)
                # queries the last 20 runs, which should contain ours.
                response_desc = self.conn.describe_flow_execution_records(flowName=flow_name)
                last_execs = {fe["executionId"]: fe for fe in response_desc["flowExecutions"]}

            exec_details = last_execs[execution_id]
            self.log.info("Run complete, execution details: %s", exec_details)

            if exec_details["executionStatus"] == "Error":
                raise Exception(f"Flow error:\n{json.dumps(exec_details, default=str)}")

        return execution_id

    def update_flow_filter(
        self, flow_name: str, filter_tasks: list[TaskTypeDef], set_trigger_ondemand: bool = False
    ) -> None:
        """
        Update the flow task filter.
        All filters will be removed if an empty array is passed to filter_tasks.

        :param flow_name: The flow name
        :param filter_tasks: List flow tasks to be added
        :param set_trigger_ondemand: If True, set the trigger to on-demand; otherwise, keep the trigger as is
        :return: None
        """
        response = self.conn.describe_flow(flowName=flow_name)
        connector_type = response["sourceFlowConfig"]["connectorType"]
        tasks: list[TaskTypeDef] = []

        # cleanup old filter tasks
        for task in response["tasks"]:
            if (
                task["taskType"] == "Filter"
                and task.get("connectorOperator", {}).get(connector_type) != "PROJECTION"
            ):
                self.log.info("Removing task: %s", task)
            else:
                tasks.append(task)  # List of non-filter tasks

        tasks += filter_tasks  # Add the new filter tasks

        if set_trigger_ondemand:
            # Clean up attribute to force on-demand trigger
            del response["triggerConfig"]["triggerProperties"]

        self.conn.update_flow(
            flowName=response["flowName"],
            destinationFlowConfigList=response["destinationFlowConfigList"],
            sourceFlowConfig=response["sourceFlowConfig"],
            triggerConfig=response["triggerConfig"],
            description=response.get("description", "Flow description."),
            tasks=tasks,
        )
