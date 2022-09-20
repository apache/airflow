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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.appflow import AppflowHook
from airflow.providers.amazon.aws.utils import datetime_to_epoch_ms, get_airflow_version

if TYPE_CHECKING:
    from mypy_boto3_appflow.type_defs import (
        DescribeFlowExecutionRecordsResponseTypeDef,
        ExecutionRecordTypeDef,
        TaskTypeDef,
    )

    from airflow.utils.context import Context


SUPPORTED_SOURCES = {"salesforce", "zendesk"}
MANDATORY_FILTER_DATE_MSG = "The filter_date argument is mandatory for {entity}!"
NOT_SUPPORTED_SOURCE_MSG = "Source {source} is not supported for {entity}!"


class AppflowBaseOperator(BaseOperator):
    """
    Amazon Appflow Base Operator class (not supposed to be used directly in DAGs).

    :param source: The source name (Supported: salesforce, zendesk)
    :param flow_name: The flow name
    :param flow_update: A boolean to enable/disable a flow update before the run
    :param source_field: The field name to apply filters
    :param filter_date: The date value (or template) to be used in filters.
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    ui_color = "#2bccbd"

    def __init__(
        self,
        source: str,
        flow_name: str,
        flow_update: bool,
        source_field: str | None = None,
        filter_date: str | None = None,
        poll_interval: int = 20,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if source not in SUPPORTED_SOURCES:
            raise ValueError(f"{source} is not a supported source (options: {SUPPORTED_SOURCES})!")
        self.filter_date = filter_date
        self.flow_name = flow_name
        self.source = source
        self.source_field = source_field
        self.poll_interval = poll_interval
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.flow_update = flow_update

    @cached_property
    def hook(self) -> AppflowHook:
        """Create and return an AppflowHook."""
        return AppflowHook(aws_conn_id=self.aws_conn_id, region_name=self.region)

    def execute(self, context: Context) -> None:
        self.filter_date_parsed: datetime | None = (
            datetime.fromisoformat(self.filter_date) if self.filter_date else None
        )
        self.connector_type = self._get_connector_type()
        if self.flow_update:
            self._update_flow()
        self._run_flow(context)

    def _get_connector_type(self) -> str:
        response = self.hook.conn.describe_flow(flowName=self.flow_name)
        connector_type = response["sourceFlowConfig"]["connectorType"]
        if self.source != connector_type.lower():
            raise ValueError(f"Incompatible source ({self.source} and connector type ({connector_type})!")
        return connector_type

    def _update_flow(self) -> None:
        self.hook.update_flow_filter(flow_name=self.flow_name, filter_tasks=[], set_trigger_ondemand=True)

    def _run_flow(self, context) -> str:
        execution_id = self.hook.run_flow(flow_name=self.flow_name, poll_interval=self.poll_interval)
        task_instance = context["task_instance"]
        task_instance.xcom_push("execution_id", execution_id)
        return execution_id


class AppflowRunOperator(AppflowBaseOperator):
    """
    Execute a Appflow run with filters as is.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AppflowRunOperator`

    :param source: The source name (Supported: salesforce, zendesk)
    :param flow_name: The flow name
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    def __init__(
        self,
        source: str,
        flow_name: str,
        poll_interval: int = 20,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        if source not in {"salesforce", "zendesk"}:
            raise ValueError(NOT_SUPPORTED_SOURCE_MSG.format(source=source, entity="AppflowRunOperator"))
        super().__init__(
            source=source,
            flow_name=flow_name,
            flow_update=False,
            source_field=None,
            filter_date=None,
            poll_interval=poll_interval,
            aws_conn_id=aws_conn_id,
            region=region,
            **kwargs,
        )


class AppflowRunFullOperator(AppflowBaseOperator):
    """
    Execute a Appflow full run removing any filter.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AppflowRunFullOperator`

    :param source: The source name (Supported: salesforce, zendesk)
    :param flow_name: The flow name
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    def __init__(
        self,
        source: str,
        flow_name: str,
        poll_interval: int = 20,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        if source not in {"salesforce", "zendesk"}:
            raise ValueError(NOT_SUPPORTED_SOURCE_MSG.format(source=source, entity="AppflowRunFullOperator"))
        super().__init__(
            source=source,
            flow_name=flow_name,
            flow_update=True,
            source_field=None,
            filter_date=None,
            poll_interval=poll_interval,
            aws_conn_id=aws_conn_id,
            region=region,
            **kwargs,
        )


class AppflowRunBeforeOperator(AppflowBaseOperator):
    """
    Execute a Appflow run after updating the filters to select only previous data.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AppflowRunBeforeOperator`

    :param source: The source name (Supported: salesforce)
    :param flow_name: The flow name
    :param source_field: The field name to apply filters
    :param filter_date: The date value (or template) to be used in filters.
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    template_fields = ("filter_date",)

    def __init__(
        self,
        source: str,
        flow_name: str,
        source_field: str,
        filter_date: str,
        poll_interval: int = 20,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        if not filter_date:
            raise ValueError(MANDATORY_FILTER_DATE_MSG.format(entity="AppflowRunBeforeOperator"))
        if source != "salesforce":
            raise ValueError(
                NOT_SUPPORTED_SOURCE_MSG.format(source=source, entity="AppflowRunBeforeOperator")
            )
        super().__init__(
            source=source,
            flow_name=flow_name,
            flow_update=True,
            source_field=source_field,
            filter_date=filter_date,
            poll_interval=poll_interval,
            aws_conn_id=aws_conn_id,
            region=region,
            **kwargs,
        )

    def _update_flow(self) -> None:
        if not self.filter_date_parsed:
            raise ValueError(f"Invalid filter_date argument parser value: {self.filter_date_parsed}")
        if not self.source_field:
            raise ValueError(f"Invalid source_field argument value: {self.source_field}")
        filter_task: TaskTypeDef = {
            "taskType": "Filter",
            "connectorOperator": {self.connector_type: "LESS_THAN"},  # type: ignore
            "sourceFields": [self.source_field],
            "taskProperties": {
                "DATA_TYPE": "datetime",
                "VALUE": str(datetime_to_epoch_ms(self.filter_date_parsed)),
            },  # NOT inclusive
        }
        self.hook.update_flow_filter(
            flow_name=self.flow_name, filter_tasks=[filter_task], set_trigger_ondemand=True
        )


class AppflowRunAfterOperator(AppflowBaseOperator):
    """
    Execute a Appflow run after updating the filters to select only future data.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AppflowRunAfterOperator`

    :param source: The source name (Supported: salesforce, zendesk)
    :param flow_name: The flow name
    :param source_field: The field name to apply filters
    :param filter_date: The date value (or template) to be used in filters.
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    template_fields = ("filter_date",)

    def __init__(
        self,
        source: str,
        flow_name: str,
        source_field: str,
        filter_date: str,
        poll_interval: int = 20,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        if not filter_date:
            raise ValueError(MANDATORY_FILTER_DATE_MSG.format(entity="AppflowRunAfterOperator"))
        if source not in {"salesforce", "zendesk"}:
            raise ValueError(NOT_SUPPORTED_SOURCE_MSG.format(source=source, entity="AppflowRunAfterOperator"))
        super().__init__(
            source=source,
            flow_name=flow_name,
            flow_update=True,
            source_field=source_field,
            filter_date=filter_date,
            poll_interval=poll_interval,
            aws_conn_id=aws_conn_id,
            region=region,
            **kwargs,
        )

    def _update_flow(self) -> None:
        if not self.filter_date_parsed:
            raise ValueError(f"Invalid filter_date argument parser value: {self.filter_date_parsed}")
        if not self.source_field:
            raise ValueError(f"Invalid source_field argument value: {self.source_field}")
        filter_task: TaskTypeDef = {
            "taskType": "Filter",
            "connectorOperator": {self.connector_type: "GREATER_THAN"},  # type: ignore
            "sourceFields": [self.source_field],
            "taskProperties": {
                "DATA_TYPE": "datetime",
                "VALUE": str(datetime_to_epoch_ms(self.filter_date_parsed)),
            },  # NOT inclusive
        }
        self.hook.update_flow_filter(
            flow_name=self.flow_name, filter_tasks=[filter_task], set_trigger_ondemand=True
        )


class AppflowRunDailyOperator(AppflowBaseOperator):
    """
    Execute a Appflow run after updating the filters to select only a single day.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AppflowRunDailyOperator`

    :param source: The source name (Supported: salesforce)
    :param flow_name: The flow name
    :param source_field: The field name to apply filters
    :param filter_date: The date value (or template) to be used in filters.
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    template_fields = ("filter_date",)

    def __init__(
        self,
        source: str,
        flow_name: str,
        source_field: str,
        filter_date: str,
        poll_interval: int = 20,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        if not filter_date:
            raise ValueError(MANDATORY_FILTER_DATE_MSG.format(entity="AppflowRunDailyOperator"))
        if source != "salesforce":
            raise ValueError(NOT_SUPPORTED_SOURCE_MSG.format(source=source, entity="AppflowRunDailyOperator"))
        super().__init__(
            source=source,
            flow_name=flow_name,
            flow_update=True,
            source_field=source_field,
            filter_date=filter_date,
            poll_interval=poll_interval,
            aws_conn_id=aws_conn_id,
            region=region,
            **kwargs,
        )

    def _update_flow(self) -> None:
        if not self.filter_date_parsed:
            raise ValueError(f"Invalid filter_date argument parser value: {self.filter_date_parsed}")
        if not self.source_field:
            raise ValueError(f"Invalid source_field argument value: {self.source_field}")
        start_filter_date = self.filter_date_parsed - timedelta(milliseconds=1)
        end_filter_date = self.filter_date_parsed + timedelta(days=1)
        filter_task: TaskTypeDef = {
            "taskType": "Filter",
            "connectorOperator": {self.connector_type: "BETWEEN"},  # type: ignore
            "sourceFields": [self.source_field],
            "taskProperties": {
                "DATA_TYPE": "datetime",
                "LOWER_BOUND": str(datetime_to_epoch_ms(start_filter_date)),  # NOT inclusive
                "UPPER_BOUND": str(datetime_to_epoch_ms(end_filter_date)),  # NOT inclusive
            },
        }
        self.hook.update_flow_filter(
            flow_name=self.flow_name, filter_tasks=[filter_task], set_trigger_ondemand=True
        )


class AppflowRecordsShortCircuitOperator(ShortCircuitOperator):
    """
    Short-circuit in case of a empty Appflow's run.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AppflowRecordsShortCircuitOperator`

    :param flow_name: The flow name
    :param appflow_run_task_id: Run task ID from where this operator should extract the execution ID
    :param ignore_downstream_trigger_rules: Ignore downstream trigger rules (Ignored for Airflow < 2.3)
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    ui_color = "#33ffec"  # Light blue

    def __init__(
        self,
        *,
        flow_name: str,
        appflow_run_task_id: str,
        ignore_downstream_trigger_rules: bool = True,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        **kwargs,
    ) -> None:
        if get_airflow_version() >= (2, 3):
            kwargs["ignore_downstream_trigger_rules"] = ignore_downstream_trigger_rules
        else:
            self.log.warning(
                "Ignoring argument ignore_downstream_trigger_rules (%s) - Only supported for Airflow >= 2.3",
                ignore_downstream_trigger_rules,
            )
        super().__init__(
            python_callable=self._has_new_records_func,
            op_kwargs={
                "flow_name": flow_name,
                "appflow_run_task_id": appflow_run_task_id,
            },
            **kwargs,
        )
        self.aws_conn_id = aws_conn_id
        self.region = region

    @staticmethod
    def _get_target_execution_id(
        records: list[ExecutionRecordTypeDef], execution_id: str
    ) -> ExecutionRecordTypeDef | None:
        for record in records:
            if record.get("executionId") == execution_id:
                return record
        return None

    @cached_property
    def hook(self) -> AppflowHook:
        """Create and return an AppflowHook."""
        return AppflowHook(aws_conn_id=self.aws_conn_id, region_name=self.region)

    def _has_new_records_func(self, **kwargs) -> bool:
        appflow_task_id = kwargs["appflow_run_task_id"]
        self.log.info("appflow_task_id: %s", appflow_task_id)
        flow_name = kwargs["flow_name"]
        self.log.info("flow_name: %s", flow_name)
        af_client = self.hook.conn
        task_instance = kwargs["task_instance"]
        execution_id = task_instance.xcom_pull(task_ids=appflow_task_id, key="execution_id")  # type: ignore
        if not execution_id:
            raise AirflowException(f"No execution_id found from task_id {appflow_task_id}!")
        self.log.info("execution_id: %s", execution_id)
        args = {"flowName": flow_name, "maxResults": 100}
        response: DescribeFlowExecutionRecordsResponseTypeDef = cast(
            "DescribeFlowExecutionRecordsResponseTypeDef", {}
        )
        record = None

        while not record:
            if "nextToken" in response:
                response = af_client.describe_flow_execution_records(nextToken=response["nextToken"], **args)
            else:
                response = af_client.describe_flow_execution_records(**args)
            record = AppflowRecordsShortCircuitOperator._get_target_execution_id(
                response["flowExecutions"], execution_id
            )
            if not record and "nextToken" not in response:
                raise AirflowException(f"Flow ({execution_id}) without recordsProcessed info.")

        execution = record.get("executionResult", {})
        if "recordsProcessed" not in execution:
            raise AirflowException(f"Flow ({execution_id}) without recordsProcessed info!")
        records_processed = execution["recordsProcessed"]
        self.log.info("records_processed: %d", records_processed)
        task_instance.xcom_push("records_processed", records_processed)  # type: ignore
        return records_processed > 0
