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
from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncIterator

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GlueJobCompleteTrigger(BaseTrigger):
    """
    Watches for a glue job, triggers when it finishes.

    :param job_name: glue job name
    :param run_id: the ID of the specific run to watch for that job
    :param verbose: whether to print the job's logs in airflow logs or not
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_name: str,
        run_id: str,
        verbose: bool,
        aws_conn_id: str | None,
        job_poll_interval: int | float,
    ):
        super().__init__()
        self.job_name = job_name
        self.run_id = run_id
        self.verbose = verbose
        self.aws_conn_id = aws_conn_id
        self.job_poll_interval = job_poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            # dynamically generate the fully qualified name of the class
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "job_name": self.job_name,
                "run_id": self.run_id,
                "verbose": str(self.verbose),
                "aws_conn_id": self.aws_conn_id,
                "job_poll_interval": self.job_poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = GlueJobHook(
            aws_conn_id=self.aws_conn_id, job_poll_interval=self.job_poll_interval
        )
        await hook.async_job_completion(self.job_name, self.run_id, self.verbose)
        yield TriggerEvent(
            {"status": "success", "message": "Job done", "value": self.run_id}
        )


class GlueCatalogPartitionTrigger(BaseTrigger):
    """
    Asynchronously waits for a partition to show up in AWS Glue Catalog.

    :param database_name: The name of the catalog database where the partitions reside.
    :param table_name: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :param expression: The partition clause to wait for. This is passed as
        is to the AWS Glue Catalog API's get_partitions function,
        and supports SQL like notation as in ``ds='2015-01-01'
        AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
        #aws-glue-api-catalog-partitions-GetPartitions
    :param aws_conn_id: ID of the Airflow connection where
        credentials and extra configuration are stored
    :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
        if not specified.
    :param waiter_delay: Number of seconds to wait between two checks. Default is 60 seconds.
    """

    def __init__(
        self,
        database_name: str,
        table_name: str,
        expression: str = "",
        waiter_delay: int = 60,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ):
        self.database_name = database_name
        self.table_name = table_name
        self.expression = expression
        self.waiter_delay = waiter_delay

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            # dynamically generate the fully qualified name of the class
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "database_name": self.database_name,
                "table_name": self.table_name,
                "expression": self.expression,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "waiter_delay": self.waiter_delay,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    @cached_property
    def hook(self) -> GlueCatalogHook:
        return GlueCatalogHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def poke(self, client: Any) -> bool:
        if "." in self.table_name:
            self.database_name, self.table_name = self.table_name.split(".")
        self.log.info(
            "Poking for table %s. %s, expression %s",
            self.database_name,
            self.table_name,
            self.expression,
        )
        partitions = await self.hook.async_get_partitions(
            client=client,
            database_name=self.database_name,
            table_name=self.table_name,
            expression=self.expression,
        )

        return bool(partitions)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with self.hook.async_conn as client:
            while True:
                result = await self.poke(client=client)
                if result:
                    yield TriggerEvent({"status": "success"})
                    break
                else:
                    await asyncio.sleep(self.waiter_delay)


class GlueDataQualityRuleSetEvaluationRunCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a AWS Glue data quality evaluation run complete.

    :param evaluation_run_id: The AWS Glue data quality ruleset evaluation run identifier.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        evaluation_run_id: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
    ):
        super().__init__(
            serialized_fields={"evaluation_run_id": evaluation_run_id},
            waiter_name="data_quality_ruleset_evaluation_run_complete",
            waiter_args={"RunId": evaluation_run_id},
            failure_message="AWS Glue data quality ruleset evaluation run failed.",
            status_message="Status of AWS Glue data quality ruleset evaluation run is",
            status_queries=["Status"],
            return_key="evaluation_run_id",
            return_value=evaluation_run_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return GlueDataQualityHook(aws_conn_id=self.aws_conn_id)


class GlueDataQualityRuleRecommendationRunCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a AWS Glue data quality recommendation run complete.

    :param recommendation_run_id: The AWS Glue data quality rule recommendation run identifier.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        recommendation_run_id: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
    ):
        super().__init__(
            serialized_fields={"recommendation_run_id": recommendation_run_id},
            waiter_name="data_quality_rule_recommendation_run_complete",
            waiter_args={"RunId": recommendation_run_id},
            failure_message="AWS Glue data quality recommendation run failed.",
            status_message="Status of AWS Glue data quality recommendation run is",
            status_queries=["Status"],
            return_key="recommendation_run_id",
            return_value=recommendation_run_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return GlueDataQualityHook(aws_conn_id=self.aws_conn_id)
