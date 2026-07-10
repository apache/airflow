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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import botocore.exceptions

from airflow.providers.amazon.aws.hooks.redshift_data import FAILURE_STATES, FINISHED_STATE, RedshiftDataHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf

try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Airflow <3.3 stub, task_state_store unavailable, always submits fresh."""

        external_id_key: str = "redshift_statement_id"

        def __init__(self, *, durable: bool = True, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = durable

        def execute_resumable(self, context):
            external_id = self.submit_job(context)
            self.poll_until_complete(external_id, context)
            return self.get_job_result(external_id, context)


if TYPE_CHECKING:
    from mypy_boto3_redshift_data.type_defs import (
        DescribeStatementResponseTypeDef,
        GetStatementResultResponseTypeDef,
    )
    from pydantic import JsonValue

    from airflow.sdk import Context


class RedshiftDataOperator(ResumableJobMixin, AwsBaseOperator[RedshiftDataHook]):
    """
    Executes SQL Statements against an Amazon Redshift cluster using Redshift Data.

    ... see also::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftDataOperator`

    :param database: the name of the database
    :param sql: the SQL statement or list of  SQL statement to run
    :param cluster_identifier: unique identifier of a cluster
    :param db_user: the database username
    :param parameters: the parameters for the SQL statement
    :param secret_arn: the name or ARN of the secret that enables db access
    :param statement_name: the name of the SQL statement
    :param with_event: indicates whether to send an event to EventBridge
    :param wait_for_completion: indicates whether to wait for a result, if True wait, if False don't wait
    :param poll_interval: how often in seconds to check the query status
    :param return_sql_result: if True will return the result of an SQL statement,
        if False (default) will return statement ID
    :param workgroup_name: name of the Redshift Serverless workgroup. Mutually exclusive with
        `cluster_identifier`. Specify this parameter to query Redshift Serverless. More info
        https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html
    :param session_id: the session identifier of the query
    :param session_keep_alive_seconds: duration in seconds to keep the session alive after the query
        finishes. The maximum time a session can keep alive is 24 hours
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :param durable: When ``True`` (the default), the Redshift statement id is persisted to task
        state before polling begins. A worker crash on retry reconnects to the existing statement
        instead of resubmitting the SQL. Set to ``False`` to always submit fresh SQL on retry.
        Requires Airflow 3.3+; ignored silently on earlier versions.
    """

    aws_hook_class = RedshiftDataHook
    external_id_key = "redshift_statement_id"
    template_fields = aws_template_fields(
        "cluster_identifier",
        "database",
        "sql",
        "db_user",
        "parameters",
        "statement_name",
        "workgroup_name",
        "session_id",
    )
    template_ext = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        sql: str | list,
        database: str | None = None,
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        parameters: list | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        wait_for_completion: bool = True,
        poll_interval: int = 10,
        return_sql_result: bool = False,
        workgroup_name: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        session_id: str | None = None,
        session_keep_alive_seconds: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database = database
        self.sql = sql
        self.cluster_identifier = cluster_identifier
        self.workgroup_name = workgroup_name
        self.db_user = db_user
        self.parameters = parameters
        self.secret_arn = secret_arn
        self.statement_name = statement_name
        self.with_event = with_event
        self.wait_for_completion = wait_for_completion
        if poll_interval > 0:
            self.poll_interval = poll_interval
        else:
            self.log.warning("Invalid poll_interval: %s", poll_interval)
        self.return_sql_result = return_sql_result
        self.deferrable = deferrable
        self.session_id = session_id
        self.session_keep_alive_seconds = session_keep_alive_seconds
        if self.deferrable and not self.wait_for_completion:
            self.log.warning(
                "deferrable=True and wait_for_completion=False are set; deferrable will be "
                "ignored and this task will run non-deferrable."
            )

    def execute(self, context: Context) -> list[GetStatementResultResponseTypeDef] | list[str]:
        """Execute a statement against Amazon Redshift."""
        if not self.deferrable:
            self.execute_resumable(context)
            return self._sql_results

        self.log.info("Executing statement: %s", self.sql)

        # wait_for_completion=False so that it waits for the status in the deferred task instead.
        query_execution_output = self.hook.execute_query(
            database=self.database,
            sql=self.sql,
            cluster_identifier=self.cluster_identifier,
            workgroup_name=self.workgroup_name,
            db_user=self.db_user,
            parameters=self.parameters,
            secret_arn=self.secret_arn,
            statement_name=self.statement_name,
            with_event=self.with_event,
            wait_for_completion=False,
            poll_interval=self.poll_interval,
            session_id=self.session_id,
            session_keep_alive_seconds=self.session_keep_alive_seconds,
        )

        # Pull the statement ID, session ID
        self.statement_id: str = query_execution_output.statement_id

        if query_execution_output.session_id:
            context["ti"].xcom_push(key="session_id", value=query_execution_output.session_id)

        if self.wait_for_completion:
            is_finished: bool = self.hook.check_query_is_finished(self.statement_id)
            if not is_finished:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=RedshiftDataTrigger(
                        statement_id=self.statement_id,
                        task_id=self.task_id,
                        poll_interval=self.poll_interval,
                        aws_conn_id=self.aws_conn_id,
                        region_name=self.region_name,
                        verify=self.verify,
                        botocore_config=self.botocore_config,
                    ),
                    method_name="execute_complete",
                )

        # Use the get_sql_results method to return the results of the SQL query, or the statement_ids,
        # depending on the value of self.return_sql_result
        return self.get_sql_results(statement_id=self.statement_id, return_sql_result=self.return_sql_result)

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> list[GetStatementResultResponseTypeDef] | list[str]:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] == "error":
            msg = f"context: {context}, error message: {validated_event['message']}"
            raise AirflowException(msg)

        statement_id = validated_event["statement_id"]
        if not statement_id:
            raise AirflowException("statement_id should not be empty.")

        self.log.info("%s completed successfully.", self.task_id)

        # Use the get_sql_results method to return the results of the SQL query, or the statement_ids,
        # depending on the value of self.return_sql_result
        return self.get_sql_results(statement_id=statement_id, return_sql_result=self.return_sql_result)

    def get_sql_results(
        self, statement_id: str, return_sql_result: bool
    ) -> list[GetStatementResultResponseTypeDef] | list[str]:
        """
        Retrieve either the result of the SQL query, or the statement ID(s).

        :param statement_id: Statement ID of the running queries
        :param return_sql_result: Boolean, true if results should be returned
        """
        # ISSUE-40427: Pull the statement, and check to see if there are sub-statements. If that is the
        # case, pull each of the sub-statement ID's, and grab the results. Otherwise, just use statement_id
        statement: DescribeStatementResponseTypeDef = self.hook.conn.describe_statement(Id=statement_id)
        statement_ids: list[str] = (
            [sub_statement["Id"] for sub_statement in statement["SubStatements"]]
            if len(statement.get("SubStatements", [])) > 0
            else [statement_id]
        )

        # If returning the SQL result, use get_statement_result to return the records for each query
        if return_sql_result:
            results: list = [self.hook.conn.get_statement_result(Id=sid) for sid in statement_ids]
            self.log.debug("Statement result(s): %s", results)
            return results
        return statement_ids

    def on_kill(self) -> None:
        """Cancel the submitted redshift query."""
        if hasattr(self, "statement_id"):
            self.log.info("Received a kill signal.")
            self.log.info("Stopping Query with statementId - %s", self.statement_id)

            try:
                self.hook.conn.cancel_statement(Id=self.statement_id)
            except Exception as ex:
                self.log.error("Unable to cancel query. Exiting. %s", ex)

    def submit_job(self, context: Context) -> str:
        """Submit the statement for execution and return its statement id."""
        output = self.hook.execute_query(
            database=self.database,
            sql=self.sql,
            cluster_identifier=self.cluster_identifier,
            workgroup_name=self.workgroup_name,
            db_user=self.db_user,
            parameters=self.parameters,
            secret_arn=self.secret_arn,
            statement_name=self.statement_name,
            with_event=self.with_event,
            wait_for_completion=False,
            poll_interval=self.poll_interval,
            session_id=self.session_id,
            session_keep_alive_seconds=self.session_keep_alive_seconds,
        )
        # Set immediately (before any polling) so on_kill can cancel even if the worker
        # dies before poll_until_complete runs.
        self.statement_id = output.statement_id
        if output.session_id:
            context["ti"].xcom_push(key="session_id", value=output.session_id)
        return self.statement_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        """Query the raw statement status; a missing/expired statement degrades to NOT_FOUND."""
        statement_id = cast("str", external_id)
        try:
            resp = self.hook.conn.describe_statement(Id=statement_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceNotFoundException", "ValidationException"):
                return "NOT_FOUND"
            raise
        return resp["Status"]

    def is_job_active(self, status: str) -> bool:
        return status not in (FINISHED_STATE, *FAILURE_STATES, "NOT_FOUND")

    def is_job_succeeded(self, status: str) -> bool:
        return status == FINISHED_STATE

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        statement_id = cast("str", external_id)
        self.statement_id = statement_id
        if self.wait_for_completion:
            self.hook.wait_for_results(statement_id, poll_interval=self.poll_interval)
        # Fetch here (not just in get_job_result): on reconnect the mixin calls only
        # poll_until_complete, never get_job_result, so execute() reads self._sql_results
        # afterward rather than relying on either method's return value.
        self._sql_results = self.get_sql_results(
            statement_id=statement_id, return_sql_result=self.return_sql_result
        )
        self._result_fetched = True

    def get_job_result(
        self, external_id: JsonValue, context: Context
    ) -> list[GetStatementResultResponseTypeDef] | list[str]:
        statement_id = cast("str", external_id)
        self.statement_id = statement_id
        if not getattr(self, "_result_fetched", False):
            self._sql_results = self.get_sql_results(
                statement_id=statement_id, return_sql_result=self.return_sql_result
            )
            self._result_fetched = True
        return self._sql_results
