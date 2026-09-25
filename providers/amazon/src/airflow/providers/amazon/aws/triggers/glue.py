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
from collections.abc import AsyncIterator
from functools import cached_property
from typing import TYPE_CHECKING, Any

from asgiref.sync import sync_to_async
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.glue import (
    GlueDataQualityHook,
    GlueJobHook,
    format_glue_logs,
    get_glue_log_group_names,
)
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

if not AIRFLOW_V_3_0_PLUS:
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import provide_session


class GlueJobCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Watches for a glue job, triggers when it finishes.

    :param job_name: glue job name
    :param run_id: the ID of the specific run to watch for that job
    :param verbose: whether to print the job's logs in airflow logs or not
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials
    :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
        if not specified.
    :param verify: Whether or not to verify SSL certificates.
    :param botocore_config: Configuration dictionary (key-values) for botocore client.
    :param stop_job_run_on_kill: If True, stop the Glue job run when the deferred task is killed
        (for example, cleared while running). Defaults to False.
    """

    aws_hook_class = GlueJobHook

    def __init__(
        self,
        job_name: str,
        run_id: str,
        verbose: bool = False,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        stop_job_run_on_kill: bool = False,
    ):
        super().__init__(
            serialized_fields={
                "job_name": job_name,
                "run_id": run_id,
                "verbose": verbose,
                "stop_job_run_on_kill": stop_job_run_on_kill,
            },
            waiter_name="job_complete",
            waiter_args={"JobName": job_name, "RunId": run_id},
            failure_message="AWS Glue job failed.",
            status_message="Status of AWS Glue job is",
            status_queries=["JobRun.JobRunState", "JobRun.ErrorMessage"],
            return_key="run_id",
            return_value=run_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )
        self.job_name = job_name
        self.run_id = run_id
        self.verbose = verbose
        self.stop_job_run_on_kill = stop_job_run_on_kill

    if not AIRFLOW_V_3_0_PLUS:

        @provide_session
        def get_task_instance(self, *, session: Session) -> TaskInstance:
            """Get the task instance for the current trigger (Airflow 2.x compatibility)."""
            from sqlalchemy import select

            ti = self.task_instance
            if ti is None:
                raise RuntimeError("task_instance is not set on the trigger")
            query = select(TaskInstance).where(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.map_index == ti.map_index,
            )
            task_instance = session.scalars(query).one_or_none()
            if task_instance is None:
                raise ValueError(
                    f"TaskInstance with dag_id: {ti.dag_id}, "
                    f"task_id: {ti.task_id}, "
                    f"run_id: {ti.run_id} and "
                    f"map_index: {ti.map_index} is not found"
                )
            return task_instance

    async def get_task_state(self):
        """Get the current state of the task instance (Airflow 3.x)."""
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        task_states_response = await sync_to_async(RuntimeTaskInstance.get_task_states)(
            dag_id=self.task_instance.dag_id,
            task_ids=[self.task_instance.task_id],
            run_ids=[self.task_instance.run_id],
            map_index=self.task_instance.map_index,
        )
        try:
            task_state = task_states_response[self.task_instance.run_id][self.task_instance.task_id]
        except Exception:
            raise ValueError(
                f"TaskInstance with dag_id: {self.task_instance.dag_id}, "
                f"task_id: {self.task_instance.task_id}, "
                f"run_id: {self.task_instance.run_id} and "
                f"map_index: {self.task_instance.map_index} is not found"
            )
        return task_state

    async def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to stop the Glue job run.

        Returns True if the task is NOT DEFERRED (a user-initiated clear/kill). Returns False if the
        task is still DEFERRED, which means the triggerer is merely restarting and the job must keep
        running.
        """
        if AIRFLOW_V_3_0_PLUS:
            task_state = await self.get_task_state()
        else:
            task_instance = self.get_task_instance()  # type: ignore[call-arg]
            task_state = task_instance.state
        return task_state != TaskInstanceState.DEFERRED

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Watch the Glue job run to completion.

        If the task is killed while waiting, stop the Glue job run when ``stop_job_run_on_kill`` is
        enabled and it is safe to do so.
        """
        try:
            async for event in self._watch():
                yield event
        except asyncio.CancelledError as e:
            # TODO: Remove this handler once the minimum supported Airflow version is 3.3+.
            # On Airflow 3.3+ the triggerer passes a sentinel via task.cancel(msg) for
            # user-initiated kills and calls on_kill() separately -- skip here to avoid stopping
            # the job twice. On older Airflow there is no sentinel, so we handle it here.
            if not (e.args and e.args[0] == "__airflow_user_action__"):
                if self.run_id and self.stop_job_run_on_kill and await self.safe_to_cancel():
                    self.log.info(
                        "Task was cancelled. Stopping AWS Glue job %s run %s.", self.job_name, self.run_id
                    )
                    self.hook().conn.batch_stop_job_run(JobName=self.job_name, JobRunIds=[self.run_id])
                else:
                    self.log.info(
                        "Trigger may have shut down or stop_job_run_on_kill is disabled. "
                        "Skipping stop of AWS Glue job %s run %s.",
                        self.job_name,
                        self.run_id,
                    )
            raise

    async def on_kill(self) -> None:
        """
        Stop the Glue job run when the trigger is cancelled by a user action.

        Available on Airflow 3.3+ via ``BaseTrigger.on_kill()``. On older Airflow the
        ``CancelledError`` handler in ``run()`` provides the same behaviour.
        """
        if self.run_id and self.stop_job_run_on_kill:
            self.log.info("Stopping AWS Glue job %s run %s.", self.job_name, self.run_id)
            await sync_to_async(self.hook().conn.batch_stop_job_run)(
                JobName=self.job_name, JobRunIds=[self.run_id]
            )

    async def _watch(self) -> AsyncIterator[TriggerEvent]:
        if not self.verbose:
            async for event in super().run():
                yield event
            return

        hook = self.hook()
        async with (
            await hook.get_async_conn() as glue_client,
            await AwsLogsHook(
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                verify=self.verify,
                config=self.botocore_config,
            ).get_async_conn() as logs_client,
        ):
            # Get log group names from job run metadata
            job_run_resp = await glue_client.get_job_run(JobName=self.job_name, RunId=self.run_id)
            log_group_output, log_group_error = get_glue_log_group_names(job_run_resp["JobRun"])

            output_token: str | None = None
            error_token: str | None = None

            for _attempt in range(self.attempts):
                # Fetch current job state
                resp = await glue_client.get_job_run(JobName=self.job_name, RunId=self.run_id)
                job_run_state = resp["JobRun"]["JobRunState"]

                # Fetch and print logs from both output and error streams
                try:
                    output_token = await self._forward_logs(
                        logs_client, log_group_output, self.run_id, output_token
                    )
                    error_token = await self._forward_logs(
                        logs_client, log_group_error, self.run_id, error_token
                    )
                except ClientError as e:
                    self.log.error(
                        "Failed to fetch logs for Glue Job %s Run %s: %s",
                        self.job_name,
                        self.run_id,
                        e,
                    )
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Failed to fetch logs for Glue Job {self.job_name} Run {self.run_id}: {e}",
                            self.return_key: self.return_value,
                        }
                    )
                    return

                # STOPPED means the run was cancelled before it produced its output, not that it succeeded.
                if job_run_state in ("FAILED", "TIMEOUT", "STOPPED", "ERROR"):
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Glue Job {self.job_name} Run {self.run_id}"
                            f" exited with state: {job_run_state}",
                            self.return_key: self.return_value,
                        }
                    )
                    return
                if job_run_state == "SUCCEEDED":
                    self.log.info(
                        "Exiting Job %s Run %s State: %s",
                        self.job_name,
                        self.run_id,
                        job_run_state,
                    )
                    yield TriggerEvent({"status": "success", self.return_key: self.return_value})
                    return

                self.log.info(
                    "Polling for AWS Glue Job %s current run state: %s",
                    self.job_name,
                    job_run_state,
                )
                await asyncio.sleep(self.waiter_delay)

            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Glue Job {self.job_name} Run {self.run_id}"
                    f" waiter exceeded max attempts ({self.attempts})",
                    self.return_key: self.return_value,
                }
            )

    async def _forward_logs(
        self,
        logs_client: Any,
        log_group: str,
        log_stream: str,
        next_token: str | None,
    ) -> str | None:
        # Matches the format used by the synchronous GlueJobHook.print_job_logs.
        fetched_logs: list[str] = []
        while True:
            token_arg: dict[str, str] = {"nextToken": next_token} if next_token else {}
            try:
                response = await logs_client.get_log_events(
                    logGroupName=log_group,
                    logStreamName=log_stream,
                    startFromHead=True,
                    **token_arg,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    region = logs_client.meta.region_name
                    self.log.warning(
                        "No new Glue driver logs so far.\n"
                        "If this persists, check the CloudWatch dashboard at: %r.",
                        f"https://{region}.console.aws.amazon.com/cloudwatch/home",
                    )
                    return None
                raise

            events = response["events"]
            fetched_logs.extend(event["message"] for event in events)

            if not events or next_token == response["nextForwardToken"]:
                break
            next_token = response["nextForwardToken"]

        self.log.info(format_glue_logs(fetched_logs, log_group))

        return response.get("nextForwardToken")


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
            "Poking for table %s. %s, expression %s", self.database_name, self.table_name, self.expression
        )
        partitions = await self.hook.async_get_partitions(
            client=client,
            database_name=self.database_name,
            table_name=self.table_name,
            expression=self.expression,
        )

        return bool(partitions)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with await self.hook.get_async_conn() as client:
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
    :param region_name: The AWS region where the resources to watch are.
    :param verify: Whether or not to verify SSL certificates.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = GlueDataQualityHook

    def __init__(
        self,
        evaluation_run_id: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
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
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )


class GlueDataQualityRuleRecommendationRunCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a AWS Glue data quality recommendation run complete.

    :param recommendation_run_id: The AWS Glue data quality rule recommendation run identifier.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: The AWS region where the resources to watch are.
    :param verify: Whether or not to verify SSL certificates.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = GlueDataQualityHook

    def __init__(
        self,
        recommendation_run_id: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
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
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )
